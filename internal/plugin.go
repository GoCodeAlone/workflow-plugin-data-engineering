// Package internal implements the workflow-plugin-data-engineering plugin.
package internal

import (
	"fmt"

	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/catalog"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/cdc"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/lakehouse"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/tenancy"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/timeseries"
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
		// Phase 1
		"cdc.source",
		"data.tenancy",
		// Lakehouse (Phase 2)
		"catalog.iceberg",
		"lakehouse.table",
		// Time-series (Phase 2)
		"timeseries.influxdb",
		"timeseries.timescaledb",
		"timeseries.clickhouse",
		"timeseries.questdb",
		"timeseries.druid",
		// Catalog (Phase 2)
		"catalog.schema_registry",
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
	case "timeseries.influxdb":
		return timeseries.NewInfluxModule(name, config)
	case "timeseries.timescaledb":
		return timeseries.NewTimescaleModule(name, config)
	case "timeseries.clickhouse":
		return timeseries.NewClickHouseModule(name, config)
	case "timeseries.questdb":
		return timeseries.NewQuestDBModule(name, config)
	case "timeseries.druid":
		return timeseries.NewDruidModule(name, config)
	case "catalog.schema_registry":
		return catalog.NewSchemaRegistryModule(name, config)
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
		// Time-series shared steps
		"step.ts_write",
		"step.ts_write_batch",
		"step.ts_query",
		"step.ts_downsample",
		"step.ts_retention",
		"step.ts_continuous_query",
		// Druid-specific steps
		"step.ts_druid_ingest",
		"step.ts_druid_query",
		"step.ts_druid_datasource",
		"step.ts_druid_compact",
		// Schema Registry steps
		"step.schema_register",
		"step.schema_validate",
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
	case "step.ts_write":
		return timeseries.NewTSWriteStep(name, config)
	case "step.ts_write_batch":
		return timeseries.NewTSWriteBatchStep(name, config)
	case "step.ts_query":
		return timeseries.NewTSQueryStep(name, config)
	case "step.ts_downsample":
		return timeseries.NewTSDownsampleStep(name, config)
	case "step.ts_retention":
		return timeseries.NewTSRetentionStep(name, config)
	case "step.ts_continuous_query":
		return timeseries.NewTSContinuousQueryStep(name, config)
	case "step.ts_druid_ingest":
		return timeseries.NewDruidIngestStep(name, config)
	case "step.ts_druid_query":
		return timeseries.NewDruidQueryStep(name, config)
	case "step.ts_druid_datasource":
		return timeseries.NewDruidDatasourceStep(name, config)
	case "step.ts_druid_compact":
		return timeseries.NewDruidCompactStep(name, config)
	case "step.schema_register":
		return catalog.NewSchemaRegisterStep(name, config)
	case "step.schema_validate":
		return catalog.NewSchemaValidateStep(name, config)
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
	schemas = append(schemas, phase2ModuleSchemas()...)
	return schemas
}

func phase2ModuleSchemas() []sdk.ModuleSchemaData {
	return []sdk.ModuleSchemaData{
		{
			Type:        "timeseries.influxdb",
			Label:       "InfluxDB",
			Category:    "Data Engineering",
			Description: "InfluxDB v2 time-series database module",
			ConfigFields: []sdk.ConfigField{
				{Name: "url", Type: "string", Description: "InfluxDB server URL", Required: true},
				{Name: "token", Type: "string", Description: "InfluxDB authentication token", Required: true},
				{Name: "org", Type: "string", Description: "InfluxDB organization", Required: true},
				{Name: "bucket", Type: "string", Description: "InfluxDB bucket", Required: true},
			},
		},
		{
			Type:        "timeseries.timescaledb",
			Label:       "TimescaleDB",
			Category:    "Data Engineering",
			Description: "TimescaleDB time-series PostgreSQL extension module",
			ConfigFields: []sdk.ConfigField{
				{Name: "connection", Type: "string", Description: "PostgreSQL connection string", Required: true},
				{Name: "maxOpenConns", Type: "number", Description: "Maximum open DB connections", Required: false},
			},
		},
		{
			Type:        "timeseries.clickhouse",
			Label:       "ClickHouse",
			Category:    "Data Engineering",
			Description: "ClickHouse columnar time-series database module",
			ConfigFields: []sdk.ConfigField{
				{Name: "endpoints", Type: "string", Description: "ClickHouse server endpoints", Required: true},
				{Name: "database", Type: "string", Description: "ClickHouse database name", Required: false},
				{Name: "username", Type: "string", Description: "ClickHouse username", Required: false},
			},
		},
		{
			Type:        "timeseries.questdb",
			Label:       "QuestDB",
			Category:    "Data Engineering",
			Description: "QuestDB high-performance time-series database module",
			ConfigFields: []sdk.ConfigField{
				{Name: "ilpEndpoint", Type: "string", Description: "QuestDB ILP TCP endpoint (host:port)", Required: true},
				{Name: "httpEndpoint", Type: "string", Description: "QuestDB HTTP endpoint for queries", Required: true},
			},
		},
		{
			Type:        "timeseries.druid",
			Label:       "Apache Druid",
			Category:    "Data Engineering",
			Description: "Apache Druid OLAP time-series ingestion module",
			ConfigFields: []sdk.ConfigField{
				{Name: "routerUrl", Type: "string", Description: "Druid Router URL", Required: true},
				{Name: "username", Type: "string", Description: "Druid username", Required: false},
				{Name: "password", Type: "string", Description: "Druid password", Required: false},
			},
		},
		{
			Type:        "catalog.schema_registry",
			Label:       "Schema Registry",
			Category:    "Data Engineering",
			Description: "Confluent-compatible Schema Registry for Avro/JSON/Protobuf schemas",
			ConfigFields: []sdk.ConfigField{
				{Name: "endpoint", Type: "string", Description: "Schema Registry URL", Required: true},
				{Name: "username", Type: "string", Description: "Basic auth username", Required: false},
				{Name: "password", Type: "string", Description: "Basic auth password", Required: false},
			},
		},
	}
}
