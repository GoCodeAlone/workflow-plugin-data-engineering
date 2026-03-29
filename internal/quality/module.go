package quality

import (
	"context"
	"fmt"
	"sync"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// ChecksConfig holds configuration for the quality.checks module.
type ChecksConfig struct {
	Provider     string `json:"provider"     yaml:"provider"`
	ContractsDir string `json:"contractsDir" yaml:"contractsDir"`
	Database     string `json:"database"     yaml:"database"`
}

// ChecksModule implements the quality.checks module.
type ChecksModule struct {
	mu     sync.RWMutex
	name   string
	config ChecksConfig
	exec   DBQuerier
}

// NewChecksModule creates a new quality.checks module from config.
func NewChecksModule(name string, config map[string]any) (sdk.ModuleInstance, error) {
	cfg := parseChecksConfig(config)
	return &ChecksModule{name: name, config: cfg}, nil
}

// NewChecksModuleWithExecutor creates a ChecksModule with an injected DBQuerier (for testing).
func NewChecksModuleWithExecutor(name string, exec DBQuerier) *ChecksModule {
	return &ChecksModule{
		name: name,
		config: ChecksConfig{
			Provider: "builtin",
		},
		exec: exec,
	}
}

// Init validates the module configuration.
func (m *ChecksModule) Init() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.config.Provider == "" {
		return fmt.Errorf("quality.checks %q: provider is required", m.name)
	}
	return nil
}

// Start registers the module in the global registry.
func (m *ChecksModule) Start(_ context.Context) error {
	if err := RegisterChecksModule(m.name, m); err != nil {
		return fmt.Errorf("quality.checks %q: %w", m.name, err)
	}
	return nil
}

// Stop deregisters the module.
func (m *ChecksModule) Stop(_ context.Context) error {
	UnregisterChecksModule(m.name)
	return nil
}

// Executor returns the DBQuerier for this module (may be nil if no DB is configured).
func (m *ChecksModule) Executor() DBQuerier {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.exec
}

// SetExecutor replaces the DBQuerier (used for lazy injection or test overrides).
func (m *ChecksModule) SetExecutor(exec DBQuerier) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.exec = exec
}

// ContractsDir returns the directory used to load data contracts.
func (m *ChecksModule) ContractsDir() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.ContractsDir
}

func parseChecksConfig(config map[string]any) ChecksConfig {
	cfg := ChecksConfig{Provider: "builtin"}
	if v, ok := config["provider"].(string); ok {
		cfg.Provider = v
	}
	if v, ok := config["contractsDir"].(string); ok {
		cfg.ContractsDir = v
	}
	if v, ok := config["database"].(string); ok {
		cfg.Database = v
	}
	return cfg
}
