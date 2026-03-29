package catalog

import (
	"context"
	"fmt"
	"sync"
	"time"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// DataHubConfig holds configuration for the catalog.datahub module.
type DataHubConfig struct {
	Endpoint string        `json:"endpoint" yaml:"endpoint"`
	Token    string        `json:"token"    yaml:"token"`
	Timeout  time.Duration `json:"timeout"  yaml:"timeout"`
}

// DataHubModule implements the catalog.datahub module.
type DataHubModule struct {
	mu        sync.RWMutex
	name      string
	config    DataHubConfig
	client    DataHubClient
	newClient func(cfg DataHubConfig) DataHubClient
}

// NewDataHubModule creates a new catalog.datahub module.
func NewDataHubModule(name string, config map[string]any) (sdk.ModuleInstance, error) {
	cfg, err := parseDHConfig(config)
	if err != nil {
		return nil, fmt.Errorf("catalog.datahub %q: %w", name, err)
	}
	return &DataHubModule{
		name:   name,
		config: cfg,
		newClient: func(c DataHubConfig) DataHubClient {
			return NewDataHubClient(c.Endpoint, c.Token, c.Timeout)
		},
	}, nil
}

// Init validates configuration.
func (m *DataHubModule) Init() error {
	if m.config.Endpoint == "" {
		return fmt.Errorf("catalog.datahub %q: endpoint is required", m.name)
	}
	return nil
}

// Start creates the client and registers the module.
func (m *DataHubModule) Start(_ context.Context) error {
	client := m.newClient(m.config)
	m.mu.Lock()
	m.client = client
	m.mu.Unlock()

	if err := RegisterCatalogModule(m.name, m); err != nil {
		return fmt.Errorf("catalog.datahub %q: register: %w", m.name, err)
	}
	return nil
}

// Stop deregisters the module.
func (m *DataHubModule) Stop(_ context.Context) error {
	UnregisterCatalogModule(m.name)
	m.mu.Lock()
	m.client = nil
	m.mu.Unlock()
	return nil
}

// Client returns the underlying DataHubClient.
func (m *DataHubModule) Client() DataHubClient {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.client
}

// CatalogProvider returns this module as a CatalogProvider.
func (m *DataHubModule) CatalogProvider() CatalogProvider {
	return &datahubCatalogProvider{module: m}
}

func parseDHConfig(config map[string]any) (DataHubConfig, error) {
	var cfg DataHubConfig
	if v, ok := config["endpoint"].(string); ok {
		cfg.Endpoint = v
	}
	if v, ok := config["token"].(string); ok {
		cfg.Token = v
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
