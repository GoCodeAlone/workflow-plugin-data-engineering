// Package internal implements the workflow-plugin-data-engineering plugin.
package internal

import (
	"fmt"

	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/catalog"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/cdc"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/graph"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/lakehouse"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/migrate"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/quality"
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
		// Graph (Phase 3)
		"graph.neo4j",
		// Data Catalog (Phase 3)
		"catalog.datahub",
		"catalog.openmetadata",
		// Data Quality (Phase 3)
		"quality.checks",
		// Schema Migration (Phase 3)
		"migrate.schema",
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
	case "graph.neo4j":
		return graph.NewNeo4jModule(name, config)
	case "catalog.datahub":
		return catalog.NewDataHubModule(name, config)
	case "catalog.openmetadata":
		return catalog.NewOpenMetadataModule(name, config)
	case "quality.checks":
		return quality.NewChecksModule(name, config)
	case "migrate.schema":
		return migrate.NewSchemaModule(name, config)
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
		"step.cdc_backpressure",
		"step.cdc_monitor",
		// Tenancy steps
		"step.tenant_provision",
		"step.tenant_deprovision",
		"step.tenant_migrate",
		"step.tenant_evaluate_promotion",
		"step.tenant_promote",
		"step.tenant_demote",
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
		"step.ts_archive",
		"step.ts_tier_status",
		// Druid-specific steps
		"step.ts_druid_ingest",
		"step.ts_druid_query",
		"step.ts_druid_datasource",
		"step.ts_druid_compact",
		// Schema Registry steps
		"step.schema_register",
		"step.schema_validate",
		// Graph steps (Phase 3)
		"step.graph_query",
		"step.graph_write",
		"step.graph_import",
		"step.graph_extract_entities",
		"step.graph_link",
		// Catalog steps (Phase 3)
		"step.catalog_register",
		"step.catalog_search",
		"step.catalog_lineage",
		"step.catalog_lineage_query",
		"step.contract_validate",
		// Data Quality steps (Phase 3)
		"step.quality_check",
		"step.quality_schema_validate",
		"step.quality_profile",
		"step.quality_compare",
		"step.quality_anomaly",
		"step.quality_dbt_test",
		"step.quality_soda_check",
		"step.quality_ge_validate",
		// Schema Migration steps (Phase 3)
		"step.migrate_plan",
		"step.migrate_apply",
		"step.migrate_run",
		"step.migrate_rollback",
		"step.migrate_status",
		// Expand-contract + pipeline schema evolution (Phase 4)
		"step.migrate_expand",
		"step.migrate_contract",
		"step.migrate_expand_status",
		"step.schema_evolve_pipeline",
		"step.schema_evolve_verify",
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
	case "step.cdc_backpressure":
		return cdc.NewBackpressureStep(name, config)
	case "step.cdc_monitor":
		return cdc.NewMonitorStep(name, config)
	case "step.tenant_provision":
		return tenancy.NewProvisionStep(name, config)
	case "step.tenant_deprovision":
		return tenancy.NewDeprovisionStep(name, config)
	case "step.tenant_migrate":
		return tenancy.NewMigrateStep(name, config)
	case "step.tenant_evaluate_promotion":
		return tenancy.NewEvaluatePromotionStep(name, config)
	case "step.tenant_promote":
		return tenancy.NewPromoteStep(name, config)
	case "step.tenant_demote":
		return tenancy.NewDemoteStep(name, config)
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
	case "step.ts_archive":
		return timeseries.NewTSArchiveStep(name, config)
	case "step.ts_tier_status":
		return timeseries.NewTSTierStatusStep(name, config)
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
	case "step.graph_query":
		return graph.NewGraphQueryStep(name, config)
	case "step.graph_write":
		return graph.NewGraphWriteStep(name, config)
	case "step.graph_import":
		return graph.NewGraphImportStep(name, config)
	case "step.graph_extract_entities":
		return graph.NewGraphExtractEntitiesStep(name, config)
	case "step.graph_link":
		return graph.NewGraphLinkStep(name, config)
	case "step.catalog_register":
		return catalog.NewCatalogRegisterStep(name, config)
	case "step.catalog_search":
		return catalog.NewCatalogSearchStep(name, config)
	case "step.catalog_lineage":
		return catalog.NewCatalogLineageStep(name, config)
	case "step.catalog_lineage_query":
		return catalog.NewCatalogLineageQueryStep(name, config)
	case "step.contract_validate":
		return catalog.NewContractValidateStep(name, config)
	case "step.quality_check":
		return quality.NewQualityCheckStep(name, config)
	case "step.quality_schema_validate":
		return quality.NewQualitySchemaValidateStep(name, config)
	case "step.quality_profile":
		return quality.NewQualityProfileStep(name, config)
	case "step.quality_compare":
		return quality.NewQualityCompareStep(name, config)
	case "step.quality_anomaly":
		return quality.NewQualityAnomalyStep(name, config)
	case "step.quality_dbt_test":
		return quality.NewDBTTestStep(name, config)
	case "step.quality_soda_check":
		return quality.NewSodaCheckStep(name, config)
	case "step.quality_ge_validate":
		return quality.NewGEValidateStep(name, config)
	case "step.migrate_plan":
		return migrate.NewMigratePlanStep(name, config)
	case "step.migrate_apply":
		return migrate.NewMigrateApplyStep(name, config)
	case "step.migrate_run":
		return migrate.NewMigrateRunStep(name, config)
	case "step.migrate_rollback":
		return migrate.NewMigrateRollbackStep(name, config)
	case "step.migrate_status":
		return migrate.NewMigrateStatusStep(name, config)
	case "step.migrate_expand":
		return migrate.NewMigrateExpandStep(name, config)
	case "step.migrate_contract":
		return migrate.NewMigrateContractStep(name, config)
	case "step.migrate_expand_status":
		return migrate.NewMigrateExpandStatusStep(name, config)
	case "step.schema_evolve_pipeline":
		return migrate.NewSchemaEvolvePipelineStep(name, config)
	case "step.schema_evolve_verify":
		return migrate.NewSchemaEvolveVerifyStep(name, config)
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
	schemas = append(schemas, graph.GraphModuleSchemas()...)
	schemas = append(schemas, phase3CatalogSchemas()...)
	schemas = append(schemas, qualityModuleSchemas()...)
	schemas = append(schemas, migrateModuleSchemas()...)
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

func qualityModuleSchemas() []sdk.ModuleSchemaData {
	return []sdk.ModuleSchemaData{
		{
			Type:        "quality.checks",
			Label:       "Data Quality Checks",
			Category:    "Data Engineering",
			Description: "Go-native data quality checks, profiling, contract validation, and anomaly detection",
			ConfigFields: []sdk.ConfigField{
				{Name: "provider", Type: "string", Description: "Check provider: builtin, dbt, soda, or great_expectations", Required: false, Options: []string{"builtin", "dbt", "soda", "great_expectations"}},
				{Name: "contractsDir", Type: "string", Description: "Directory containing YAML data contracts", Required: false},
				{Name: "database", Type: "string", Description: "Referenced database module name for SQL checks", Required: false},
			},
		},
	}
}

func migrateModuleSchemas() []sdk.ModuleSchemaData {
	return []sdk.ModuleSchemaData{
		{
			Type:        "migrate.schema",
			Label:       "Schema Migration",
			Category:    "Data Engineering",
			Description: "Declarative schema differ and scripted migration runner",
			ConfigFields: []sdk.ConfigField{
				{Name: "strategy", Type: "string", Description: "Migration strategy: declarative, scripted, or both", Required: true, Options: []string{"declarative", "scripted", "both"}},
				{Name: "target", Type: "string", Description: "Target database module name", Required: false},
				{Name: "migrationsDir", Type: "string", Description: "Directory containing NNN_description.{up,down}.sql files", Required: false},
				{Name: "lockTable", Type: "string", Description: "Migration state tracking table (default: schema_migrations)", Required: false},
				{Name: "onBreakingChange", Type: "string", Description: "Policy for breaking changes: block, warn, or blue_green", Required: false, Options: []string{"block", "warn", "blue_green"}},
			},
		},
	}
}

func phase3CatalogSchemas() []sdk.ModuleSchemaData {
	return []sdk.ModuleSchemaData{
		{
			Type:        "catalog.datahub",
			Label:       "DataHub",
			Category:    "Data Engineering",
			Description: "DataHub metadata catalog module",
			ConfigFields: []sdk.ConfigField{
				{Name: "endpoint", Type: "string", Description: "DataHub GMS endpoint URL", Required: true},
				{Name: "token", Type: "string", Description: "DataHub access token", Required: false},
				{Name: "timeout", Type: "string", Description: "HTTP request timeout (e.g. 30s)", Required: false},
			},
		},
		{
			Type:        "catalog.openmetadata",
			Label:       "OpenMetadata",
			Category:    "Data Engineering",
			Description: "OpenMetadata catalog module",
			ConfigFields: []sdk.ConfigField{
				{Name: "endpoint", Type: "string", Description: "OpenMetadata server URL", Required: true},
				{Name: "token", Type: "string", Description: "OpenMetadata JWT token", Required: false},
				{Name: "timeout", Type: "string", Description: "HTTP request timeout (e.g. 30s)", Required: false},
			},
		},
	}
}
