// Package internal implements the workflow-plugin-data-engineering plugin.
package internal

import (
	"fmt"

	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/cdc"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/lakehouse"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/tenancy"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// dataEngineeringPlugin implements sdk.PluginProvider, sdk.ModuleProvider,
// sdk.StepProvider, sdk.TriggerProvider, and sdk.SchemaProvider.
type dataEngineeringPlugin struct {
	version string
}

// NewDataEngineeringPlugin returns a new dataEngineeringPlugin instance.
func NewDataEngineeringPlugin(version string) sdk.PluginProvider {
	return &dataEngineeringPlugin{version: version}
}

// Manifest returns plugin metadata.
func (p *dataEngineeringPlugin) Manifest() sdk.PluginManifest {
	return sdk.PluginManifest{
		Name:        "workflow-plugin-data-engineering",
		Version:     p.version,
		Author:      "GoCodeAlone",
		Description: "Data engineering plugin: CDC, lakehouse, time-series, graph, data quality",
	}
}

// ModuleTypes returns the module type names this plugin provides.
func (p *dataEngineeringPlugin) ModuleTypes() []string {
	return []string{
		"cdc.source",
		"data.tenancy",
		"catalog.iceberg",
		"lakehouse.table",
	}
}

// CreateModule creates a module instance of the given type.
func (p *dataEngineeringPlugin) CreateModule(typeName, name string, config map[string]any) (sdk.ModuleInstance, error) {
	switch typeName {
	case "cdc.source":
		return cdc.NewSourceModule(name, config)
	case "data.tenancy":
		return tenancy.NewTenancyModule(name, config)
	case "catalog.iceberg":
		return lakehouse.NewCatalogModule(name, config)
	case "lakehouse.table":
		return lakehouse.NewTableModule(name, config)
	default:
		return nil, fmt.Errorf("data-engineering plugin: unknown module type %q", typeName)
	}
}

// StepTypes returns the step type names this plugin provides.
func (p *dataEngineeringPlugin) StepTypes() []string {
	return []string{
		// CDC steps
		"step.cdc_start",
		"step.cdc_stop",
		"step.cdc_status",
		"step.cdc_snapshot",
		"step.cdc_schema_history",
		// Tenancy steps
		"step.tenant_provision",
		"step.tenant_deprovision",
		"step.tenant_migrate",
		// Lakehouse steps
		"step.lakehouse_create_table",
		"step.lakehouse_evolve_schema",
		"step.lakehouse_write",
		"step.lakehouse_compact",
		"step.lakehouse_snapshot",
		"step.lakehouse_query",
		"step.lakehouse_expire_snapshots",
	}
}

// CreateStep creates a step instance of the given type.
func (p *dataEngineeringPlugin) CreateStep(typeName, name string, config map[string]any) (sdk.StepInstance, error) {
	switch typeName {
	case "step.cdc_start":
		return cdc.NewStartStep(name, config)
	case "step.cdc_stop":
		return cdc.NewStopStep(name, config)
	case "step.cdc_status":
		return cdc.NewStatusStep(name, config)
	case "step.cdc_snapshot":
		return cdc.NewSnapshotStep(name, config)
	case "step.cdc_schema_history":
		return cdc.NewSchemaHistoryStep(name, config)
	case "step.tenant_provision":
		return tenancy.NewProvisionStep(name, config)
	case "step.tenant_deprovision":
		return tenancy.NewDeprovisionStep(name, config)
	case "step.tenant_migrate":
		return tenancy.NewMigrateStep(name, config)
	case "step.lakehouse_create_table":
		return lakehouse.NewCreateTableStep(name, config)
	case "step.lakehouse_evolve_schema":
		return lakehouse.NewEvolveSchemaStep(name, config)
	case "step.lakehouse_write":
		return lakehouse.NewWriteStep(name, config)
	case "step.lakehouse_compact":
		return lakehouse.NewCompactStep(name, config)
	case "step.lakehouse_snapshot":
		return lakehouse.NewSnapshotStep(name, config)
	case "step.lakehouse_query":
		return lakehouse.NewQueryStep(name, config)
	case "step.lakehouse_expire_snapshots":
		return lakehouse.NewExpireSnapshotsStep(name, config)
	default:
		return nil, fmt.Errorf("data-engineering plugin: unknown step type %q", typeName)
	}
}

// TriggerTypes returns the trigger type names this plugin provides.
func (p *dataEngineeringPlugin) TriggerTypes() []string {
	return []string{
		"trigger.cdc",
	}
}

// CreateTrigger creates a trigger instance of the given type.
func (p *dataEngineeringPlugin) CreateTrigger(typeName string, config map[string]any, cb sdk.TriggerCallback) (sdk.TriggerInstance, error) {
	switch typeName {
	case "trigger.cdc":
		return cdc.NewTrigger(config, cb)
	default:
		return nil, fmt.Errorf("data-engineering plugin: unknown trigger type %q", typeName)
	}
}

// ModuleSchemas returns schema metadata for all module types.
func (p *dataEngineeringPlugin) ModuleSchemas() []sdk.ModuleSchemaData {
	schemas := []sdk.ModuleSchemaData{
		{
			Type:        "cdc.source",
			Label:       "CDC Source",
			Category:    "Data Engineering",
			Description: "Change Data Capture stream from a relational database (Postgres, MySQL)",
			ConfigFields: []sdk.ConfigField{
				{Name: "provider", Type: "string", Description: "CDC provider: bento, debezium, or dms", Required: true, Options: []string{"bento", "debezium", "dms"}},
				{Name: "source_id", Type: "string", Description: "Unique identifier for this CDC source", Required: true},
				{Name: "source_type", Type: "string", Description: "Source database type: postgres or mysql", Required: true, Options: []string{"postgres", "mysql"}},
				{Name: "connection", Type: "string", Description: "Database connection string or DSN", Required: true},
				{Name: "tables", Type: "string", Description: "Comma-separated list of tables to capture", Required: false},
			},
		},
		tenancy.TenancyModuleSchema(),
	}
	schemas = append(schemas, lakehouse.LakehouseModuleSchemas()...)
	return schemas
}
