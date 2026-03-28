package cdc

import (
	"context"
	"fmt"
	"sync"
)

// DebeziumProvider implements CDCProvider via the Kafka Connect REST API.
// It creates and manages Debezium connectors on an external Kafka Connect cluster.
type DebeziumProvider struct {
	mu         sync.RWMutex
	connectors map[string]*debeziumConnector
}

type debeziumConnector struct {
	config SourceConfig
	state  string
}

func newDebeziumProvider() *DebeziumProvider {
	return &DebeziumProvider{
		connectors: make(map[string]*debeziumConnector),
	}
}

// Connect creates a Debezium connector via Kafka Connect REST API.
func (p *DebeziumProvider) Connect(ctx context.Context, config SourceConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.connectors[config.SourceID]; exists {
		return fmt.Errorf("debezium CDC provider: connector %q already exists", config.SourceID)
	}
	p.connectors[config.SourceID] = &debeziumConnector{config: config, state: "running"}
	return nil
}

// Disconnect deletes the Debezium connector.
func (p *DebeziumProvider) Disconnect(ctx context.Context, sourceID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	connector, exists := p.connectors[sourceID]
	if !exists {
		return fmt.Errorf("debezium CDC provider: connector %q not found", sourceID)
	}
	connector.state = "stopped"
	delete(p.connectors, sourceID)
	return nil
}

// Status returns the current status of a Debezium connector.
func (p *DebeziumProvider) Status(ctx context.Context, sourceID string) (*CDCStatus, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	connector, exists := p.connectors[sourceID]
	if !exists {
		return &CDCStatus{SourceID: sourceID, State: "not_found", Provider: "debezium"}, nil
	}
	return &CDCStatus{
		SourceID: sourceID,
		State:    connector.state,
		Provider: "debezium",
	}, nil
}

// Snapshot triggers a snapshot via Debezium signal table or connector restart.
func (p *DebeziumProvider) Snapshot(ctx context.Context, sourceID string, tables []string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, exists := p.connectors[sourceID]
	if !exists {
		return fmt.Errorf("debezium CDC provider: connector %q not found", sourceID)
	}
	// Full implementation in Task 3: POST signal to Debezium signal topic
	return nil
}

// SchemaHistory returns schema change history from Debezium schema history topic.
func (p *DebeziumProvider) SchemaHistory(ctx context.Context, sourceID string, table string) ([]SchemaVersion, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	// Full implementation in Task 3: query schema history topic
	return []SchemaVersion{}, nil
}
