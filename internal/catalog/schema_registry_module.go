package catalog

import (
	"context"
	"fmt"
	"sync"
	"time"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// SchemaRegistryConfig holds configuration for the catalog.schema_registry module.
type SchemaRegistryConfig struct {
	Provider             string        `json:"provider"             yaml:"provider"`
	Endpoint             string        `json:"endpoint"             yaml:"endpoint"`
	Username             string        `json:"username"             yaml:"username"`
	Password             string        `json:"password"             yaml:"password"`
	DefaultCompatibility string        `json:"defaultCompatibility" yaml:"defaultCompatibility"`
	Timeout              time.Duration `json:"timeout"              yaml:"timeout"`
}

// SchemaRegistryModule implements the catalog.schema_registry module.
type SchemaRegistryModule struct {
	mu         sync.RWMutex
	name       string
	config     SchemaRegistryConfig
	client     SchemaRegistryClient
	newClient  func(cfg SchemaRegistryConfig) SchemaRegistryClient
}

// NewSchemaRegistryModule creates a new catalog.schema_registry module.
func NewSchemaRegistryModule(name string, config map[string]any) (sdk.ModuleInstance, error) {
	cfg, err := parseSRConfig(config)
	if err != nil {
		return nil, fmt.Errorf("catalog.schema_registry %q: %w", name, err)
	}
	return &SchemaRegistryModule{
		name:   name,
		config: cfg,
		newClient: func(c SchemaRegistryConfig) SchemaRegistryClient {
			return NewSchemaRegistryClient(c.Endpoint, c.Username, c.Password, c.Timeout)
		},
	}, nil
}

// Init validates the module configuration.
func (m *SchemaRegistryModule) Init() error {
	if m.config.Endpoint == "" {
		return fmt.Errorf("catalog.schema_registry %q: endpoint is required", m.name)
	}
	return nil
}

// Start verifies connectivity by listing subjects, then registers the module.
func (m *SchemaRegistryModule) Start(ctx context.Context) error {
	client := m.newClient(m.config)

	if _, err := client.ListSubjects(ctx); err != nil {
		return fmt.Errorf("catalog.schema_registry %q: connect: %w", m.name, err)
	}

	// Apply default compatibility level if configured.
	if m.config.DefaultCompatibility != "" {
		if err := client.SetCompatibilityLevel(ctx, "", m.config.DefaultCompatibility); err != nil {
			return fmt.Errorf("catalog.schema_registry %q: set default compatibility: %w", m.name, err)
		}
	}

	m.mu.Lock()
	m.client = client
	m.mu.Unlock()

	if err := RegisterSRModule(m.name, m); err != nil {
		return fmt.Errorf("catalog.schema_registry %q: register: %w", m.name, err)
	}
	return nil
}

// Stop deregisters the module.
func (m *SchemaRegistryModule) Stop(_ context.Context) error {
	UnregisterSRModule(m.name)
	m.mu.Lock()
	m.client = nil
	m.mu.Unlock()
	return nil
}

// Client returns the underlying SchemaRegistryClient.
func (m *SchemaRegistryModule) Client() SchemaRegistryClient {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.client
}

// Config returns the module configuration.
func (m *SchemaRegistryModule) Config() SchemaRegistryConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

func parseSRConfig(config map[string]any) (SchemaRegistryConfig, error) {
	var cfg SchemaRegistryConfig
	cfg.Provider = "confluent" // default

	if v, ok := config["provider"].(string); ok {
		cfg.Provider = v
	}
	if v, ok := config["endpoint"].(string); ok {
		cfg.Endpoint = v
	}
	if auth, ok := config["auth"].(map[string]any); ok {
		if u, ok := auth["username"].(string); ok {
			cfg.Username = u
		}
		if p, ok := auth["password"].(string); ok {
			cfg.Password = p
		}
	}
	if v, ok := config["username"].(string); ok {
		cfg.Username = v
	}
	if v, ok := config["password"].(string); ok {
		cfg.Password = v
	}
	if v, ok := config["defaultCompatibility"].(string); ok {
		cfg.DefaultCompatibility = v
	}
	if v, ok := config["timeout"]; ok {
		switch d := v.(type) {
		case string:
			parsed, err := time.ParseDuration(d)
			if err != nil {
				return cfg, fmt.Errorf("invalid timeout %q: %w", d, err)
			}
			cfg.Timeout = parsed
		case time.Duration:
			cfg.Timeout = d
		}
	}
	if cfg.Endpoint == "" {
		return cfg, fmt.Errorf("endpoint is required")
	}
	return cfg, nil
}
