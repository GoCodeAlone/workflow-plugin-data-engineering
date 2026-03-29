package lakehouse

import (
	"context"
	"fmt"
	"sync"
	"time"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// catalogModuleConfig holds configuration for the catalog.iceberg module.
type catalogModuleConfig struct {
	Endpoint    string        `json:"endpoint"    yaml:"endpoint"`
	Warehouse   string        `json:"warehouse"   yaml:"warehouse"`
	Credential  string        `json:"credential"  yaml:"credential"`
	HTTPTimeout time.Duration `json:"httpTimeout" yaml:"httpTimeout"`
}

// CatalogModule implements sdk.ModuleInstance for the catalog.iceberg module type.
// It wraps an IcebergCatalogClient and registers it in the global catalog registry.
type CatalogModule struct {
	mu     sync.RWMutex
	name   string
	config catalogModuleConfig
	client IcebergCatalogClient
}

// NewCatalogModule creates a new catalog.iceberg module instance.
func NewCatalogModule(name string, config map[string]any) (sdk.ModuleInstance, error) {
	cfg, err := parseCatalogConfig(config)
	if err != nil {
		return nil, fmt.Errorf("catalog.iceberg %q: %w", name, err)
	}
	return &CatalogModule{name: name, config: cfg}, nil
}

// Init validates the catalog configuration.
func (m *CatalogModule) Init() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.config.Endpoint == "" {
		return fmt.Errorf("catalog.iceberg %q: endpoint is required", m.name)
	}
	return nil
}

// Start creates the HTTP client, pings the catalog to verify connectivity,
// and registers the catalog in the global registry.
func (m *CatalogModule) Start(ctx context.Context) error {
	client, err := NewIcebergCatalogClient(IcebergClientConfig{
		Endpoint:    m.config.Endpoint,
		Token:       m.config.Credential,
		HTTPTimeout: m.config.HTTPTimeout,
	})
	if err != nil {
		return fmt.Errorf("catalog.iceberg %q: create client: %w", m.name, err)
	}

	// Ping to verify connectivity.
	if _, err := client.GetConfig(ctx); err != nil {
		return fmt.Errorf("catalog.iceberg %q: ping catalog: %w", m.name, err)
	}

	m.mu.Lock()
	m.client = client
	m.mu.Unlock()

	if err := RegisterCatalog(m.name, client); err != nil {
		return fmt.Errorf("catalog.iceberg %q: register: %w", m.name, err)
	}
	return nil
}

// Stop deregisters the catalog from the global registry.
func (m *CatalogModule) Stop(_ context.Context) error {
	UnregisterCatalog(m.name)
	m.mu.Lock()
	m.client = nil
	m.mu.Unlock()
	return nil
}

// Client returns the underlying IcebergCatalogClient.
func (m *CatalogModule) Client() IcebergCatalogClient {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.client
}

func parseCatalogConfig(config map[string]any) (catalogModuleConfig, error) {
	var cfg catalogModuleConfig
	if v, ok := config["endpoint"].(string); ok {
		cfg.Endpoint = v
	}
	if v, ok := config["warehouse"].(string); ok {
		cfg.Warehouse = v
	}
	if v, ok := config["credential"].(string); ok {
		cfg.Credential = v
	}
	if v, ok := config["httpTimeout"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.HTTPTimeout = d
		}
	}
	return cfg, nil
}
