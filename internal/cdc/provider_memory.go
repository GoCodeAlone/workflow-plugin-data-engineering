package cdc

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryProvider is an in-memory CDCProvider implementation intended for testing.
// It allows injecting synthetic CDC events via InjectEvent and verifying provider behavior
// without real database connections or external services.
type MemoryProvider struct {
	mu      sync.RWMutex
	sources map[string]*memorySource
}

// memorySource holds the in-memory state of a CDC source.
type memorySource struct {
	mu            sync.RWMutex
	config        SourceConfig
	state         string
	lastEvent     string
	schemaHistory []SchemaVersion
	handler       EventHandler
	events        chan map[string]any
	cancel        context.CancelFunc
	done          chan struct{}
	lagBytes      int64
	lagSeconds    int64
}

// NewMemoryProvider creates a new MemoryProvider.
func NewMemoryProvider() *MemoryProvider {
	return &MemoryProvider{
		sources: make(map[string]*memorySource),
	}
}

// Connect registers an in-memory CDC source and starts its event dispatch goroutine.
func (p *MemoryProvider) Connect(ctx context.Context, config SourceConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.sources[config.SourceID]; exists {
		return fmt.Errorf("memory CDC provider: source %q already exists", config.SourceID)
	}

	src := &memorySource{
		config: config,
		state:  "running",
		events: make(chan map[string]any, 256),
		done:   make(chan struct{}),
	}
	runCtx, cancel := context.WithCancel(ctx)
	src.cancel = cancel
	p.sources[config.SourceID] = src

	go src.dispatchLoop(runCtx)

	return nil
}

// Disconnect stops the event dispatch goroutine and removes the source.
func (p *MemoryProvider) Disconnect(ctx context.Context, sourceID string) error {
	p.mu.Lock()
	src, exists := p.sources[sourceID]
	if !exists {
		p.mu.Unlock()
		return fmt.Errorf("memory CDC provider: source %q not found", sourceID)
	}
	delete(p.sources, sourceID)
	p.mu.Unlock()

	src.cancel()
	select {
	case <-src.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	src.mu.Lock()
	src.state = "stopped"
	src.mu.Unlock()
	return nil
}

// Status returns the current status of a memory CDC source.
func (p *MemoryProvider) Status(_ context.Context, sourceID string) (*CDCStatus, error) {
	p.mu.RLock()
	src, exists := p.sources[sourceID]
	p.mu.RUnlock()

	if !exists {
		return &CDCStatus{SourceID: sourceID, State: "not_found", Provider: "memory"}, nil
	}

	src.mu.RLock()
	defer src.mu.RUnlock()
	return &CDCStatus{
		SourceID:   sourceID,
		State:      src.state,
		Provider:   "memory",
		LastEvent:  src.lastEvent,
		LagBytes:   src.lagBytes,
		LagSeconds: src.lagSeconds,
	}, nil
}

// SetLag configures simulated CDC lag on a running memory source (for backpressure tests).
func (p *MemoryProvider) SetLag(sourceID string, lagBytes, lagSeconds int64) error {
	p.mu.RLock()
	src, exists := p.sources[sourceID]
	p.mu.RUnlock()
	if !exists {
		return fmt.Errorf("memory CDC provider: source %q not found", sourceID)
	}
	src.mu.Lock()
	src.lagBytes = lagBytes
	src.lagSeconds = lagSeconds
	src.mu.Unlock()
	return nil
}

// Snapshot triggers a synthetic snapshot by emitting a snapshot_started event.
func (p *MemoryProvider) Snapshot(_ context.Context, sourceID string, tables []string) error {
	p.mu.RLock()
	src, exists := p.sources[sourceID]
	p.mu.RUnlock()

	if !exists {
		return fmt.Errorf("memory CDC provider: source %q not found", sourceID)
	}

	event := map[string]any{
		"type":   "snapshot_started",
		"tables": tables,
		"ts":     time.Now().UTC().Format(time.RFC3339),
	}
	select {
	case src.events <- event:
		return nil
	default:
		return fmt.Errorf("memory CDC provider: event buffer full for source %q", sourceID)
	}
}

// SchemaHistory returns the recorded DDL change history for a table.
func (p *MemoryProvider) SchemaHistory(_ context.Context, sourceID string, table string) ([]SchemaVersion, error) {
	p.mu.RLock()
	src, exists := p.sources[sourceID]
	p.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("memory CDC provider: source %q not found", sourceID)
	}

	src.mu.RLock()
	defer src.mu.RUnlock()

	var result []SchemaVersion
	for _, sv := range src.schemaHistory {
		if sv.Table == table {
			result = append(result, sv)
		}
	}
	return result, nil
}

// RegisterEventHandler registers a callback for events from a named source.
func (p *MemoryProvider) RegisterEventHandler(sourceID string, h EventHandler) error {
	p.mu.RLock()
	src, exists := p.sources[sourceID]
	p.mu.RUnlock()

	if !exists {
		return fmt.Errorf("memory CDC provider: source %q not found", sourceID)
	}

	src.mu.Lock()
	src.handler = h
	src.mu.Unlock()
	return nil
}

// InjectEvent injects a synthetic CDC event into the named source's event stream.
// This is the primary API used by tests to simulate database change events.
func (p *MemoryProvider) InjectEvent(sourceID string, event map[string]any) error {
	p.mu.RLock()
	src, exists := p.sources[sourceID]
	p.mu.RUnlock()

	if !exists {
		return fmt.Errorf("memory CDC provider: source %q not found", sourceID)
	}

	select {
	case src.events <- event:
		return nil
	default:
		return fmt.Errorf("memory CDC provider: event buffer full for source %q", sourceID)
	}
}

// AddSchemaVersion records a synthetic DDL change event for testing schema history.
func (p *MemoryProvider) AddSchemaVersion(sourceID string, sv SchemaVersion) error {
	p.mu.RLock()
	src, exists := p.sources[sourceID]
	p.mu.RUnlock()

	if !exists {
		return fmt.Errorf("memory CDC provider: source %q not found", sourceID)
	}

	src.mu.Lock()
	src.schemaHistory = append(src.schemaHistory, sv)
	src.mu.Unlock()
	return nil
}

// dispatchLoop reads events from the source channel and calls the registered handler.
func (src *memorySource) dispatchLoop(ctx context.Context) {
	defer close(src.done)
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-src.events:
			if !ok {
				return
			}
			src.mu.Lock()
			src.lastEvent = time.Now().UTC().Format(time.RFC3339)
			h := src.handler
			src.mu.Unlock()

			if h != nil {
				_ = h(src.config.SourceID, event)
			}
		}
	}
}
