// Package tenancy implements multi-tenant data isolation modules and steps.
package tenancy

import (
	"context"
	"fmt"
	"sync"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// TenancyModule is a data.tenancy module that manages tenant provisioning and
// schema isolation strategies.
type TenancyModule struct {
	mu     sync.RWMutex
	name   string
	config TenancyConfig
}

// TenancyConfig holds configuration for the data.tenancy module.
type TenancyConfig struct {
	Strategy     string `json:"strategy"      yaml:"strategy"`
	Connection   string `json:"connection"    yaml:"connection"`
	MigrationDir string `json:"migration_dir" yaml:"migration_dir"`
}

// NewTenancyModule creates a new data.tenancy module instance.
func NewTenancyModule(name string, config map[string]any) (sdk.ModuleInstance, error) {
	cfg, err := parseTenancyConfig(config)
	if err != nil {
		return nil, fmt.Errorf("data.tenancy %q: %w", name, err)
	}
	return &TenancyModule{name: name, config: cfg}, nil
}

// Init validates the tenancy module configuration.
func (m *TenancyModule) Init() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.config.Connection == "" {
		return fmt.Errorf("data.tenancy %q: connection is required", m.name)
	}
	switch m.config.Strategy {
	case "schema", "database":
		// valid
	case "":
		return fmt.Errorf("data.tenancy %q: strategy is required (schema or database)", m.name)
	default:
		return fmt.Errorf("data.tenancy %q: unknown strategy %q (valid: schema, database)", m.name, m.config.Strategy)
	}
	return nil
}

// Start opens the admin database connection.
func (m *TenancyModule) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Full implementation in Task 6: open pgx pool to admin connection
	return nil
}

// Stop closes the admin database connection.
func (m *TenancyModule) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return nil
}

// Config returns the tenancy configuration.
func (m *TenancyModule) Config() TenancyConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

func parseTenancyConfig(config map[string]any) (TenancyConfig, error) {
	var cfg TenancyConfig
	if v, ok := config["strategy"].(string); ok {
		cfg.Strategy = v
	}
	if v, ok := config["connection"].(string); ok {
		cfg.Connection = v
	}
	if v, ok := config["migration_dir"].(string); ok {
		cfg.MigrationDir = v
	}
	return cfg, nil
}
