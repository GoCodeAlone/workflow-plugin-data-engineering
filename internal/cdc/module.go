// Package cdc implements Change Data Capture modules, steps, and triggers.
package cdc

import (
	"context"
	"fmt"
	"sync"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// SourceModule is a CDC source module that streams change events from a database.
type SourceModule struct {
	mu       sync.RWMutex
	name     string
	config   SourceConfig
	provider CDCProvider
}

// SourceConfig holds configuration for the cdc.source module.
type SourceConfig struct {
	Provider   string         `json:"provider"    yaml:"provider"`
	SourceID   string         `json:"source_id"   yaml:"source_id"`
	SourceType string         `json:"source_type" yaml:"source_type"`
	Connection string         `json:"connection"  yaml:"connection"`
	Tables     []string       `json:"tables"      yaml:"tables"`
	Options    map[string]any `json:"options"     yaml:"options"`
}

// NewSourceModule creates a new CDC source module.
func NewSourceModule(name string, config map[string]any) (sdk.ModuleInstance, error) {
	cfg, err := parseSourceConfig(config)
	if err != nil {
		return nil, fmt.Errorf("cdc.source %q: %w", name, err)
	}
	provider, err := newProvider(cfg)
	if err != nil {
		return nil, fmt.Errorf("cdc.source %q: %w", name, err)
	}
	return &SourceModule{
		name:     name,
		config:   cfg,
		provider: provider,
	}, nil
}

// Init validates the module configuration.
func (m *SourceModule) Init() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.config.SourceID == "" {
		return fmt.Errorf("cdc.source %q: source_id is required", m.name)
	}
	if m.config.Connection == "" {
		return fmt.Errorf("cdc.source %q: connection is required", m.name)
	}
	return nil
}

// Start initializes the CDC provider connection and registers the module in the global registry.
func (m *SourceModule) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.provider.Connect(ctx, m.config); err != nil {
		return err
	}

	// Register so steps and triggers can look up this source by ID.
	if err := RegisterSource(m.config.SourceID, m.provider); err != nil {
		// Unwind the connection if registration fails.
		_ = m.provider.Disconnect(ctx, m.config.SourceID)
		return fmt.Errorf("cdc.source %q: register: %w", m.name, err)
	}

	return nil
}

// Stop shuts down the CDC provider connection and deregisters from the global registry.
func (m *SourceModule) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	UnregisterSource(m.config.SourceID)
	return m.provider.Disconnect(ctx, m.config.SourceID)
}

// Provider returns the underlying CDC provider (used by steps).
func (m *SourceModule) Provider() CDCProvider {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.provider
}

// Config returns the source configuration.
func (m *SourceModule) Config() SourceConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

func parseSourceConfig(config map[string]any) (SourceConfig, error) {
	var cfg SourceConfig
	if v, ok := config["provider"].(string); ok {
		cfg.Provider = v
	}
	if v, ok := config["source_id"].(string); ok {
		cfg.SourceID = v
	}
	if v, ok := config["source_type"].(string); ok {
		cfg.SourceType = v
	}
	if v, ok := config["connection"].(string); ok {
		cfg.Connection = v
	}
	switch t := config["tables"].(type) {
	case []string:
		cfg.Tables = t
	case []any:
		for _, v := range t {
			if s, ok := v.(string); ok {
				cfg.Tables = append(cfg.Tables, s)
			}
		}
	}
	if v, ok := config["options"].(map[string]any); ok {
		cfg.Options = v
	}
	if cfg.Provider == "" {
		return cfg, fmt.Errorf("provider is required (bento, debezium, or dms)")
	}
	return cfg, nil
}
