package catalog

import (
	"context"
	"fmt"
	"sync"
	"time"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// OpenMetadataConfig holds configuration for the catalog.openmetadata module.
type OpenMetadataConfig struct {
	Endpoint string        `json:"endpoint" yaml:"endpoint"`
	Token    string        `json:"token"    yaml:"token"`
	Timeout  time.Duration `json:"timeout"  yaml:"timeout"`
}

// OpenMetadataModule implements the catalog.openmetadata module.
type OpenMetadataModule struct {
	mu        sync.RWMutex
	name      string
	config    OpenMetadataConfig
	client    OpenMetadataClient
	newClient func(cfg OpenMetadataConfig) OpenMetadataClient
}

// NewOpenMetadataModule creates a new catalog.openmetadata module.
func NewOpenMetadataModule(name string, config map[string]any) (sdk.ModuleInstance, error) {
	cfg, err := parseOMConfig(config)
	if err != nil {
		return nil, fmt.Errorf("catalog.openmetadata %q: %w", name, err)
	}
	return &OpenMetadataModule{
		name:   name,
		config: cfg,
		newClient: func(c OpenMetadataConfig) OpenMetadataClient {
			return NewOpenMetadataClient(c.Endpoint, c.Token, c.Timeout)
		},
	}, nil
}

// Init validates configuration.
func (m *OpenMetadataModule) Init() error {
	if m.config.Endpoint == "" {
		return fmt.Errorf("catalog.openmetadata %q: endpoint is required", m.name)
	}
	return nil
}

// Start creates the client and registers the module.
func (m *OpenMetadataModule) Start(_ context.Context) error {
	client := m.newClient(m.config)
	m.mu.Lock()
	m.client = client
	m.mu.Unlock()

	if err := RegisterCatalogModule(m.name, m); err != nil {
		return fmt.Errorf("catalog.openmetadata %q: register: %w", m.name, err)
	}
	return nil
}

// Stop deregisters the module.
func (m *OpenMetadataModule) Stop(_ context.Context) error {
	UnregisterCatalogModule(m.name)
	m.mu.Lock()
	m.client = nil
	m.mu.Unlock()
	return nil
}

// Client returns the underlying OpenMetadataClient.
func (m *OpenMetadataModule) Client() OpenMetadataClient {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.client
}

// CatalogProvider returns this module as a CatalogProvider.
func (m *OpenMetadataModule) CatalogProvider() CatalogProvider {
	return &omCatalogProvider{module: m}
}

func parseOMConfig(config map[string]any) (OpenMetadataConfig, error) {
	var cfg OpenMetadataConfig
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
