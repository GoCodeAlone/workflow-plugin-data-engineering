package cdc

import (
	"context"
	"fmt"
)

// CDCProvider defines the interface for Change Data Capture providers.
// Implementations: BentoProvider, DebeziumProvider, DMSProvider.
type CDCProvider interface {
	// Connect establishes a connection and starts the CDC stream.
	Connect(ctx context.Context, config SourceConfig) error
	// Disconnect stops the CDC stream and releases resources.
	Disconnect(ctx context.Context, sourceID string) error
	// Status returns the current status of a CDC stream.
	Status(ctx context.Context, sourceID string) (*CDCStatus, error)
	// Snapshot triggers a full table snapshot for the given tables.
	Snapshot(ctx context.Context, sourceID string, tables []string) error
	// SchemaHistory returns the schema change history for a table.
	SchemaHistory(ctx context.Context, sourceID string, table string) ([]SchemaVersion, error)
}

// CDCStatus describes the current state of a CDC stream.
type CDCStatus struct {
	SourceID  string `json:"source_id"  yaml:"source_id"`
	State     string `json:"state"      yaml:"state"`
	Provider  string `json:"provider"   yaml:"provider"`
	LastEvent string `json:"last_event" yaml:"last_event"`
	Error     string `json:"error,omitempty" yaml:"error,omitempty"`
}

// SchemaVersion describes a schema change event for a table.
type SchemaVersion struct {
	Table     string `json:"table"      yaml:"table"`
	Version   int64  `json:"version"    yaml:"version"`
	DDL       string `json:"ddl"        yaml:"ddl"`
	AppliedAt string `json:"applied_at" yaml:"applied_at"`
}

// newProvider constructs a CDCProvider by name.
// Actual implementations are wired in Tasks 2-4.
func newProvider(name string) (CDCProvider, error) {
	switch name {
	case "bento":
		return newBentoProvider(), nil
	case "debezium":
		return newDebeziumProvider(), nil
	case "dms":
		return newDMSProvider(), nil
	default:
		return nil, fmt.Errorf("unknown CDC provider %q (valid: bento, debezium, dms)", name)
	}
}
