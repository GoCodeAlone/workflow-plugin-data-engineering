package migrate

import (
	"context"
	"fmt"
	"sync"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// SchemaModuleConfig holds configuration for the migrate.schema module.
type SchemaModuleConfig struct {
	Strategy        string       `json:"strategy"         yaml:"strategy"`         // declarative, scripted, or both
	Target          string       `json:"target"           yaml:"target"`            // database module reference
	Schemas         []SchemaPath `json:"schemas"          yaml:"schemas"`           // YAML schema file paths
	MigrationsDir   string       `json:"migrationsDir"    yaml:"migrationsDir"`     // directory of NNN_*.sql files
	LockTable       string       `json:"lockTable"        yaml:"lockTable"`         // defaults to schema_migrations
	OnBreakingChange string      `json:"onBreakingChange" yaml:"onBreakingChange"`  // block, warn, blue_green
}

// SchemaPath is a reference to a YAML schema file.
type SchemaPath struct {
	Path string `json:"path" yaml:"path"`
}

// SchemaModule implements sdk.ModuleInstance for the migrate.schema module type.
type SchemaModule struct {
	mu      sync.RWMutex
	name    string
	config  SchemaModuleConfig
	schemas []SchemaDefinition
	scripts []MigrationScript
	runner  *MigrationRunner
}

// NewSchemaModule creates a new migrate.schema module instance.
func NewSchemaModule(name string, config map[string]any) (sdk.ModuleInstance, error) {
	cfg, err := parseSchemaModuleConfig(config)
	if err != nil {
		return nil, fmt.Errorf("migrate.schema %q: %w", name, err)
	}
	return &SchemaModule{name: name, config: cfg}, nil
}

// Init validates the module configuration.
func (m *SchemaModule) Init() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.config.Strategy == "" {
		return fmt.Errorf("migrate.schema %q: strategy is required (declarative, scripted, both)", m.name)
	}
	switch m.config.Strategy {
	case "declarative", "scripted", "both":
	default:
		return fmt.Errorf("migrate.schema %q: unknown strategy %q (declarative, scripted, both)", m.name, m.config.Strategy)
	}
	if m.config.OnBreakingChange != "" {
		switch m.config.OnBreakingChange {
		case "block", "warn", "blue_green":
		default:
			return fmt.Errorf("migrate.schema %q: unknown onBreakingChange %q (block, warn, blue_green)", m.name, m.config.OnBreakingChange)
		}
	}
	return nil
}

// Start parses schema files and/or migration scripts based on the configured strategy,
// then registers this module in the global registry.
func (m *SchemaModule) Start(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	lockTable := m.config.LockTable
	if lockTable == "" {
		lockTable = "schema_migrations"
	}

	switch m.config.Strategy {
	case "declarative", "both":
		schemas := make([]SchemaDefinition, 0, len(m.config.Schemas))
		for _, sp := range m.config.Schemas {
			def, err := ParseSchemaFile(sp.Path)
			if err != nil {
				return fmt.Errorf("migrate.schema %q: parse schema %q: %w", m.name, sp.Path, err)
			}
			schemas = append(schemas, def)
		}
		m.schemas = schemas
		if m.config.Strategy == "declarative" {
			break
		}
		fallthrough
	case "scripted":
		if m.config.MigrationsDir != "" {
			scripts, err := LoadScripts(m.config.MigrationsDir)
			if err != nil {
				return fmt.Errorf("migrate.schema %q: load scripts: %w", m.name, err)
			}
			m.scripts = scripts
		}
	}

	m.runner = NewMigrationRunner(nil, lockTable) // executor injected at step run time

	if err := RegisterModule(m.name, m); err != nil {
		return fmt.Errorf("migrate.schema %q: register: %w", m.name, err)
	}
	return nil
}

// Stop deregisters the module.
func (m *SchemaModule) Stop(_ context.Context) error {
	UnregisterModule(m.name)
	m.mu.Lock()
	m.runner = nil
	m.mu.Unlock()
	return nil
}

// Schemas returns the parsed declarative schema definitions.
func (m *SchemaModule) Schemas() []SchemaDefinition {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.schemas
}

// Scripts returns the loaded migration scripts.
func (m *SchemaModule) Scripts() []MigrationScript {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.scripts
}

// LockTable returns the name of the migration state table.
func (m *SchemaModule) LockTable() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.config.LockTable != "" {
		return m.config.LockTable
	}
	return "schema_migrations"
}

// OnBreakingChange returns the configured breaking-change policy.
func (m *SchemaModule) OnBreakingChange() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.config.OnBreakingChange == "" {
		return "block"
	}
	return m.config.OnBreakingChange
}

// parseSchemaModuleConfig extracts SchemaModuleConfig from a raw config map.
func parseSchemaModuleConfig(config map[string]any) (SchemaModuleConfig, error) {
	var cfg SchemaModuleConfig
	if v, ok := config["strategy"].(string); ok {
		cfg.Strategy = v
	}
	if v, ok := config["target"].(string); ok {
		cfg.Target = v
	}
	if v, ok := config["migrationsDir"].(string); ok {
		cfg.MigrationsDir = v
	}
	if v, ok := config["lockTable"].(string); ok {
		cfg.LockTable = v
	}
	if v, ok := config["onBreakingChange"].(string); ok {
		cfg.OnBreakingChange = v
	}
	// schemas: [{path: ...}, ...]
	switch raw := config["schemas"].(type) {
	case []any:
		for _, item := range raw {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			path, _ := m["path"].(string)
			if path != "" {
				cfg.Schemas = append(cfg.Schemas, SchemaPath{Path: path})
			}
		}
	case []map[string]any:
		for _, m := range raw {
			path, _ := m["path"].(string)
			if path != "" {
				cfg.Schemas = append(cfg.Schemas, SchemaPath{Path: path})
			}
		}
	}
	return cfg, nil
}
