package lakehouse

import (
	"context"
	"fmt"
	"sync"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// TableConfig holds configuration for the lakehouse.table module.
type TableConfig struct {
	Catalog   string            `json:"catalog"    yaml:"catalog"`
	Namespace Namespace         `json:"namespace"  yaml:"namespace"`
	Table     string            `json:"table"      yaml:"table"`
	Schema    TableSchemaConfig `json:"schema"     yaml:"schema"`
}

// TableSchemaConfig holds the declared schema for a managed table.
type TableSchemaConfig struct {
	Fields []TableFieldConfig `json:"fields" yaml:"fields"`
}

// TableFieldConfig holds a single field declaration.
type TableFieldConfig struct {
	Name     string `json:"name"     yaml:"name"`
	Type     string `json:"type"     yaml:"type"`
	Required bool   `json:"required" yaml:"required"`
	Doc      string `json:"doc"      yaml:"doc"`
}

// TableModule implements sdk.ModuleInstance for the lakehouse.table module type.
// It ensures the managed table exists in the catalog on Start.
type TableModule struct {
	mu     sync.RWMutex
	name   string
	config TableConfig
}

// NewTableModule creates a new lakehouse.table module instance.
func NewTableModule(name string, config map[string]any) (sdk.ModuleInstance, error) {
	cfg, err := parseTableConfig(config)
	if err != nil {
		return nil, fmt.Errorf("lakehouse.table %q: %w", name, err)
	}
	return &TableModule{name: name, config: cfg}, nil
}

// Init validates the table configuration. Catalog lookup is deferred to Start.
func (m *TableModule) Init() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.config.Table == "" {
		return fmt.Errorf("lakehouse.table %q: table name is required", m.name)
	}
	if m.config.Catalog == "" {
		return fmt.Errorf("lakehouse.table %q: catalog reference is required", m.name)
	}
	return nil
}

// Start ensures the managed table exists, creating it if necessary.
func (m *TableModule) Start(ctx context.Context) error {
	m.mu.RLock()
	cfg := m.config
	m.mu.RUnlock()

	client, err := LookupCatalog(cfg.Catalog)
	if err != nil {
		return fmt.Errorf("lakehouse.table %q: %w", m.name, err)
	}

	id := TableIdentifier{Namespace: cfg.Namespace, Name: cfg.Table}
	exists, err := client.TableExists(ctx, id)
	if err != nil {
		return fmt.Errorf("lakehouse.table %q: check existence: %w", m.name, err)
	}

	if !exists {
		if err := m.createTable(ctx, client, cfg); err != nil {
			return fmt.Errorf("lakehouse.table %q: create: %w", m.name, err)
		}
		return nil
	}

	// Table exists — load and verify schema.
	if _, err := client.LoadTable(ctx, id); err != nil {
		return fmt.Errorf("lakehouse.table %q: load: %w", m.name, err)
	}
	return nil
}

// Stop is a no-op; the catalog handles its own lifecycle.
func (m *TableModule) Stop(_ context.Context) error {
	return nil
}

func (m *TableModule) createTable(ctx context.Context, client IcebergCatalogClient, cfg TableConfig) error {
	fields := make([]SchemaField, len(cfg.Schema.Fields))
	for i, f := range cfg.Schema.Fields {
		fields[i] = SchemaField{
			ID:       i + 1,
			Name:     f.Name,
			Type:     f.Type,
			Required: f.Required,
			Doc:      f.Doc,
		}
	}
	req := CreateTableRequest{
		Name: cfg.Table,
		Schema: Schema{
			SchemaID: 0,
			Type:     "struct",
			Fields:   fields,
		},
	}
	_, err := client.CreateTable(ctx, cfg.Namespace, req)
	return err
}

func parseTableConfig(config map[string]any) (TableConfig, error) {
	var cfg TableConfig
	if v, ok := config["catalog"].(string); ok {
		cfg.Catalog = v
	}
	if v, ok := config["table"].(string); ok {
		cfg.Table = v
	}
	cfg.Namespace = parseNamespace(config["namespace"])

	// Parse schema.
	if raw, ok := config["schema"].(map[string]any); ok {
		if fieldsRaw, ok := raw["fields"].([]any); ok {
			for _, fRaw := range fieldsRaw {
				fm, ok := fRaw.(map[string]any)
				if !ok {
					continue
				}
				var f TableFieldConfig
				if v, ok := fm["name"].(string); ok {
					f.Name = v
				}
				if v, ok := fm["type"].(string); ok {
					f.Type = v
				}
				if v, ok := fm["required"].(bool); ok {
					f.Required = v
				}
				if v, ok := fm["doc"].(string); ok {
					f.Doc = v
				}
				cfg.Schema.Fields = append(cfg.Schema.Fields, f)
			}
		}
	}
	return cfg, nil
}

// parseNamespace converts a raw config value to a Namespace.
// Accepts []any (YAML list) or string (single-level namespace).
func parseNamespace(raw any) Namespace {
	switch v := raw.(type) {
	case []string:
		return Namespace(v)
	case []any:
		ns := make(Namespace, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				ns = append(ns, s)
			}
		}
		return ns
	case string:
		if v == "" {
			return nil
		}
		return Namespace{v}
	}
	return nil
}
