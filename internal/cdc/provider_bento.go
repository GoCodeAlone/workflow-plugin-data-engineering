package cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/warpstreamlabs/bento/v4/public/service"
)

// BentoProvider implements CDCProvider using the warpstreamlabs/bento/v4 stream library.
// It generates Bento YAML configs for each database type and manages real Bento streams.
type BentoProvider struct {
	mu      sync.RWMutex
	streams map[string]*bentoStream
}

// bentoStream holds the runtime state of a running Bento CDC stream.
type bentoStream struct {
	config    SourceConfig
	stream    *service.Stream
	cancel    context.CancelFunc
	done      chan struct{}
	state     string // "starting", "running", "stopped", "error"
	lastEvent string
	lastErr   string
	handler   EventHandler
	mu        sync.RWMutex
}

// newBentoProvider creates a new BentoProvider.
func newBentoProvider() *BentoProvider {
	return &BentoProvider{
		streams: make(map[string]*bentoStream),
	}
}

// Connect starts a Bento CDC stream for the given source configuration.
func (p *BentoProvider) Connect(ctx context.Context, config SourceConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.streams[config.SourceID]; exists {
		return fmt.Errorf("bento CDC provider: stream %q already exists", config.SourceID)
	}

	inputYAML, err := buildBentoInputYAML(config)
	if err != nil {
		return fmt.Errorf("bento CDC provider %q: build input config: %w", config.SourceID, err)
	}

	bs := &bentoStream{
		config: config,
		done:   make(chan struct{}),
		state:  "starting",
	}
	p.streams[config.SourceID] = bs

	if err := bs.start(ctx, inputYAML); err != nil {
		delete(p.streams, config.SourceID)
		return fmt.Errorf("bento CDC provider %q: start stream: %w", config.SourceID, err)
	}

	return nil
}

// Disconnect stops the Bento CDC stream and removes it from the registry.
func (p *BentoProvider) Disconnect(ctx context.Context, sourceID string) error {
	p.mu.Lock()
	bs, exists := p.streams[sourceID]
	if !exists {
		p.mu.Unlock()
		return fmt.Errorf("bento CDC provider: stream %q not found", sourceID)
	}
	delete(p.streams, sourceID)
	p.mu.Unlock()

	return bs.stop(ctx)
}

// Status returns the current status of a Bento CDC stream.
func (p *BentoProvider) Status(ctx context.Context, sourceID string) (*CDCStatus, error) {
	p.mu.RLock()
	bs, exists := p.streams[sourceID]
	p.mu.RUnlock()

	if !exists {
		return &CDCStatus{SourceID: sourceID, State: "not_found", Provider: "bento"}, nil
	}

	bs.mu.RLock()
	defer bs.mu.RUnlock()
	return &CDCStatus{
		SourceID:  sourceID,
		State:     bs.state,
		Provider:  "bento",
		LastEvent: bs.lastEvent,
		Error:     bs.lastErr,
	}, nil
}

// Snapshot restarts the Bento stream in snapshot mode (re-reads all data).
func (p *BentoProvider) Snapshot(ctx context.Context, sourceID string, tables []string) error {
	p.mu.Lock()
	bs, exists := p.streams[sourceID]
	if !exists {
		p.mu.Unlock()
		return fmt.Errorf("bento CDC provider: stream %q not found", sourceID)
	}
	p.mu.Unlock()

	// Stop the current stream.
	if err := bs.stop(ctx); err != nil {
		return fmt.Errorf("bento CDC provider %q: snapshot stop: %w", sourceID, err)
	}

	// Rebuild config with snapshot tables override.
	snapCfg := bs.config
	if len(tables) > 0 {
		snapCfg.Tables = tables
	}

	inputYAML, err := buildBentoInputYAML(snapCfg)
	if err != nil {
		return fmt.Errorf("bento CDC provider %q: snapshot build config: %w", sourceID, err)
	}

	bs.done = make(chan struct{})
	bs.state = "starting"

	if err := bs.start(ctx, inputYAML); err != nil {
		return fmt.Errorf("bento CDC provider %q: snapshot restart: %w", sourceID, err)
	}

	return nil
}

// SchemaHistory returns schema change history.
// Bento streams do not track DDL history natively; this always returns empty.
func (p *BentoProvider) SchemaHistory(_ context.Context, sourceID string, _ string) ([]SchemaVersion, error) {
	p.mu.RLock()
	_, exists := p.streams[sourceID]
	p.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("bento CDC provider: stream %q not found", sourceID)
	}
	return []SchemaVersion{}, nil
}

// RegisterEventHandler registers a callback for CDC events from a source stream.
// The handler is called from the Bento stream goroutine; it must be goroutine-safe.
func (p *BentoProvider) RegisterEventHandler(sourceID string, h EventHandler) error {
	p.mu.RLock()
	bs, exists := p.streams[sourceID]
	p.mu.RUnlock()
	if !exists {
		return fmt.Errorf("bento CDC provider: stream %q not found", sourceID)
	}
	bs.mu.Lock()
	bs.handler = h
	bs.mu.Unlock()
	return nil
}

// start builds and runs the Bento stream in a goroutine.
// inputYAML is a Bento input config fragment (e.g., "sql_raw:\n  driver: postgres\n  ...").
func (bs *bentoStream) start(ctx context.Context, inputYAML string) error {
	builder := service.NewStreamBuilder()
	builder.DisableLinting()
	if err := builder.AddInputYAML(inputYAML); err != nil {
		return fmt.Errorf("add input yaml: %w", err)
	}
	return bs.buildAndRun(ctx, builder)
}

// startWithYAML starts the stream using a complete Bento config YAML string.
// This form is used by tests that need to provide a fully-specified config
// (e.g. with the "generate" batch input) rather than just an input fragment.
func (bs *bentoStream) startWithYAML(ctx context.Context, fullYAML string) error {
	builder := service.NewStreamBuilder()
	builder.DisableLinting()
	if err := builder.SetYAML(fullYAML); err != nil {
		return fmt.Errorf("set yaml: %w", err)
	}
	return bs.buildAndRun(ctx, builder)
}

// buildAndRun attaches the consumer function, builds, and runs the stream.
func (bs *bentoStream) buildAndRun(ctx context.Context, builder *service.StreamBuilder) error {
	if err := builder.AddConsumerFunc(bs.handleMessage); err != nil {
		return fmt.Errorf("add consumer func: %w", err)
	}

	stream, err := builder.Build()
	if err != nil {
		return fmt.Errorf("build stream: %w", err)
	}
	bs.stream = stream

	runCtx, cancel := context.WithCancel(ctx)
	bs.cancel = cancel

	go func() {
		defer close(bs.done)
		bs.mu.Lock()
		bs.state = "running"
		bs.mu.Unlock()

		if runErr := stream.Run(runCtx); runCtx.Err() == nil && runErr != nil {
			bs.mu.Lock()
			bs.state = "error"
			bs.lastErr = runErr.Error()
			bs.mu.Unlock()
		} else {
			bs.mu.Lock()
			bs.state = "stopped"
			bs.mu.Unlock()
		}
	}()

	return nil
}

// stop halts the stream and waits for the goroutine to exit.
func (bs *bentoStream) stop(ctx context.Context) error {
	if bs.stream != nil {
		if err := bs.stream.Stop(ctx); err != nil {
			return fmt.Errorf("stop stream: %w", err)
		}
	}
	if bs.cancel != nil {
		bs.cancel()
	}
	select {
	case <-bs.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// handleMessage is the Bento consumer function. It decodes each CDC message
// into a map and dispatches to the registered event handler.
func (bs *bentoStream) handleMessage(ctx context.Context, msg *service.Message) error {
	raw, err := msg.AsBytes()
	if err != nil {
		return fmt.Errorf("read message bytes: %w", err)
	}

	event := make(map[string]any)
	if jsonErr := json.Unmarshal(raw, &event); jsonErr != nil {
		// Non-JSON: store as raw string.
		event["raw"] = string(raw)
	}

	// Attach metadata.
	meta := make(map[string]any)
	_ = msg.MetaWalkMut(func(key string, value any) error {
		meta[key] = value
		return nil
	})
	if len(meta) > 0 {
		event["_meta"] = meta
	}

	// Update last event timestamp.
	bs.mu.Lock()
	bs.lastEvent = time.Now().UTC().Format(time.RFC3339)
	handler := bs.handler
	bs.mu.Unlock()

	if handler != nil {
		return handler(bs.config.SourceID, event)
	}
	return nil
}
