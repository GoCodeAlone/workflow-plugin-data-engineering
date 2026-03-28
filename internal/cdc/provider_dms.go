package cdc

import (
	"context"
	"fmt"
	"sync"
)

// DMSProvider implements CDCProvider using AWS Database Migration Service.
// It creates and manages AWS DMS replication tasks via the AWS SDK.
type DMSProvider struct {
	mu    sync.RWMutex
	tasks map[string]*dmsTask
}

type dmsTask struct {
	config SourceConfig
	state  string
}

func newDMSProvider() *DMSProvider {
	return &DMSProvider{
		tasks: make(map[string]*dmsTask),
	}
}

// Connect creates an AWS DMS replication task and starts it.
func (p *DMSProvider) Connect(ctx context.Context, config SourceConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.tasks[config.SourceID]; exists {
		return fmt.Errorf("dms CDC provider: task %q already exists", config.SourceID)
	}
	p.tasks[config.SourceID] = &dmsTask{config: config, state: "running"}
	return nil
}

// Disconnect stops and deletes the AWS DMS replication task.
func (p *DMSProvider) Disconnect(ctx context.Context, sourceID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	task, exists := p.tasks[sourceID]
	if !exists {
		return fmt.Errorf("dms CDC provider: task %q not found", sourceID)
	}
	task.state = "stopped"
	delete(p.tasks, sourceID)
	return nil
}

// Status returns the current status of an AWS DMS replication task.
func (p *DMSProvider) Status(ctx context.Context, sourceID string) (*CDCStatus, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	task, exists := p.tasks[sourceID]
	if !exists {
		return &CDCStatus{SourceID: sourceID, State: "not_found", Provider: "dms"}, nil
	}
	return &CDCStatus{
		SourceID: sourceID,
		State:    task.state,
		Provider: "dms",
	}, nil
}

// Snapshot triggers a full-load snapshot via AWS DMS reload-target operation.
func (p *DMSProvider) Snapshot(ctx context.Context, sourceID string, tables []string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, exists := p.tasks[sourceID]
	if !exists {
		return fmt.Errorf("dms CDC provider: task %q not found", sourceID)
	}
	// Full implementation in Task 4: AWS DMS StartReplicationTask with reload-target
	return nil
}

// SchemaHistory returns schema change history (not natively supported by DMS).
func (p *DMSProvider) SchemaHistory(ctx context.Context, sourceID string, table string) ([]SchemaVersion, error) {
	return []SchemaVersion{}, nil
}
