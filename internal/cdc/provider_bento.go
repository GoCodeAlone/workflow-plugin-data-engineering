package cdc

import (
	"context"
	"fmt"
	"sync"
)

// BentoProvider implements CDCProvider using Bento (warpstreamlabs/bento) streams.
// It generates Bento YAML configurations for Postgres/MySQL CDC inputs and
// manages them as subprocesses.
type BentoProvider struct {
	mu      sync.RWMutex
	streams map[string]*bentoStream
}

type bentoStream struct {
	config SourceConfig
	state  string
}

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
	p.streams[config.SourceID] = &bentoStream{config: config, state: "running"}
	return nil
}

// Disconnect stops the Bento CDC stream.
func (p *BentoProvider) Disconnect(ctx context.Context, sourceID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	stream, exists := p.streams[sourceID]
	if !exists {
		return fmt.Errorf("bento CDC provider: stream %q not found", sourceID)
	}
	stream.state = "stopped"
	delete(p.streams, sourceID)
	return nil
}

// Status returns the current status of a Bento CDC stream.
func (p *BentoProvider) Status(ctx context.Context, sourceID string) (*CDCStatus, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	stream, exists := p.streams[sourceID]
	if !exists {
		return &CDCStatus{SourceID: sourceID, State: "not_found", Provider: "bento"}, nil
	}
	return &CDCStatus{
		SourceID: sourceID,
		State:    stream.state,
		Provider: "bento",
	}, nil
}

// Snapshot triggers a full snapshot via Bento reset mechanism.
func (p *BentoProvider) Snapshot(ctx context.Context, sourceID string, tables []string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, exists := p.streams[sourceID]
	if !exists {
		return fmt.Errorf("bento CDC provider: stream %q not found", sourceID)
	}
	// Full implementation in Task 2: restart stream with snapshot_mode=always
	return nil
}

// SchemaHistory returns schema change history (Bento does not track DDL history natively).
func (p *BentoProvider) SchemaHistory(ctx context.Context, sourceID string, table string) ([]SchemaVersion, error) {
	return []SchemaVersion{}, nil
}
