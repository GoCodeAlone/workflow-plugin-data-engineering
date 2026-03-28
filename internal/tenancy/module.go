package tenancy

import (
	"context"
	"fmt"
	"sync"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// TenancyConfig holds configuration for the data.tenancy module.
type TenancyConfig struct {
	Strategy           string   `json:"strategy"            yaml:"strategy"`
	TenantKeyPath      string   `json:"tenant_key"          yaml:"tenant_key"`
	SchemaPrefix       string   `json:"schema_prefix"       yaml:"schema_prefix"`
	ConnectionTemplate string   `json:"connection_template" yaml:"connection_template"`
	TenantColumn       string   `json:"tenant_column"       yaml:"tenant_column"`
	Tables             []string `json:"tables"              yaml:"tables"`
}

// TenancyModule is a data.tenancy module that manages tenant isolation via a pluggable strategy.
type TenancyModule struct {
	mu       sync.RWMutex
	name     string
	config   TenancyConfig
	strategy TenancyStrategy
}

// NewTenancyModule creates a new data.tenancy module instance.
func NewTenancyModule(name string, config map[string]any) (sdk.ModuleInstance, error) {
	cfg, err := parseTenancyConfig(config)
	if err != nil {
		return nil, fmt.Errorf("data.tenancy %q: %w", name, err)
	}
	strategy, err := buildStrategy(cfg, noopSQLExecutor)
	if err != nil {
		return nil, fmt.Errorf("data.tenancy %q: %w", name, err)
	}
	return &TenancyModule{name: name, config: cfg, strategy: strategy}, nil
}

// Init validates the tenancy module configuration.
func (m *TenancyModule) Init() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return nil
}

// Start activates the module (no-op: strategies use injected executors).
func (m *TenancyModule) Start(_ context.Context) error {
	return nil
}

// Stop deactivates the module.
func (m *TenancyModule) Stop(_ context.Context) error {
	return nil
}

// Strategy returns the active TenancyStrategy.
func (m *TenancyModule) Strategy() TenancyStrategy {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.strategy
}

// TenantKey returns the dot-path used to resolve the tenant ID from pipeline context.
func (m *TenancyModule) TenantKey() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.TenantKeyPath
}

// buildStrategy constructs the TenancyStrategy for the given config and executor.
func buildStrategy(cfg TenancyConfig, exec SQLExecutor) (TenancyStrategy, error) {
	switch cfg.Strategy {
	case "schema_per_tenant":
		return NewSchemaPerTenant(cfg.SchemaPrefix, exec), nil
	case "db_per_tenant":
		return NewDBPerTenant(exec), nil
	case "row_level":
		return NewRowLevel(cfg.TenantColumn, cfg.Tables, exec), nil
	case "":
		return nil, fmt.Errorf("strategy is required (schema_per_tenant, db_per_tenant, row_level)")
	default:
		return nil, fmt.Errorf("unknown strategy %q (valid: schema_per_tenant, db_per_tenant, row_level)", cfg.Strategy)
	}
}

// strategyFromConfig builds a TenancyStrategy directly from a raw config map.
// Used by steps that embed strategy config.
func strategyFromConfig(config map[string]any, exec SQLExecutor) (TenancyStrategy, error) {
	cfg, err := parseTenancyConfig(config)
	if err != nil {
		return nil, err
	}
	return buildStrategy(cfg, exec)
}

func parseTenancyConfig(config map[string]any) (TenancyConfig, error) {
	var cfg TenancyConfig
	if v, ok := config["strategy"].(string); ok {
		cfg.Strategy = v
	}
	if v, ok := config["tenant_key"].(string); ok {
		cfg.TenantKeyPath = v
	}
	if v, ok := config["schema_prefix"].(string); ok {
		cfg.SchemaPrefix = v
	}
	if v, ok := config["connection_template"].(string); ok {
		cfg.ConnectionTemplate = v
	}
	if v, ok := config["tenant_column"].(string); ok {
		cfg.TenantColumn = v
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
	return cfg, nil
}
