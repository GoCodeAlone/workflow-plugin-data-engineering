package tenancy

import sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"

// TenancyModuleSchema returns the UI schema definition for the data.tenancy module.
func TenancyModuleSchema() sdk.ModuleSchemaData {
	return sdk.ModuleSchemaData{
		Type:        "data.tenancy",
		Label:       "Data Tenancy",
		Category:    "Data Engineering",
		Description: "Multi-tenant data isolation: schema-per-tenant, database-per-tenant, or row-level",
		ConfigFields: []sdk.ConfigField{
			{
				Name:        "strategy",
				Type:        "string",
				Description: "Isolation strategy",
				Required:    true,
				Options:     []string{"schema_per_tenant", "db_per_tenant", "row_level"},
			},
			{
				Name:        "tenant_key",
				Type:        "string",
				Description: "Dot-path to resolve tenant ID from pipeline context (e.g. ctx.tenant_id)",
				Required:    false,
			},
			{
				Name:        "schema_prefix",
				Type:        "string",
				Description: "Prefix for tenant schemas (schema_per_tenant only, e.g. 't_')",
				Required:    false,
			},
			{
				Name:        "connection_template",
				Type:        "string",
				Description: "Connection string template with {{tenant}} placeholder (db_per_tenant only)",
				Required:    false,
			},
			{
				Name:        "tenant_column",
				Type:        "string",
				Description: "Column name containing tenant ID (row_level only, e.g. 'org_id')",
				Required:    false,
			},
			{
				Name:        "tables",
				Type:        "string",
				Description: "Comma-separated tenant-scoped tables for row-level deprovision",
				Required:    false,
			},
		},
	}
}
