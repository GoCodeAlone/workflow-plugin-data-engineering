package lakehouse

import sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"

// LakehouseModuleSchemas returns ModuleSchemaData for all lakehouse module types.
func LakehouseModuleSchemas() []sdk.ModuleSchemaData {
	return []sdk.ModuleSchemaData{
		{
			Type:        "catalog.iceberg",
			Label:       "Iceberg REST Catalog",
			Category:    "Data Engineering",
			Description: "Apache Iceberg REST Catalog client for table management",
			ConfigFields: []sdk.ConfigField{
				{Name: "endpoint", Type: "string", Description: "Iceberg REST Catalog base URL (e.g. https://catalog.example.com/v1)", Required: true},
				{Name: "warehouse", Type: "string", Description: "Default warehouse location (e.g. s3://bucket/warehouse)", Required: false},
				{Name: "credential", Type: "string", Description: "Bearer token for authentication", Required: false},
				{Name: "httpTimeout", Type: "string", Description: "HTTP client timeout (e.g. 30s)", Required: false},
			},
		},
		{
			Type:        "lakehouse.table",
			Label:       "Lakehouse Table",
			Category:    "Data Engineering",
			Description: "Manages an Iceberg table lifecycle — creates on start if absent",
			ConfigFields: []sdk.ConfigField{
				{Name: "catalog", Type: "string", Description: "Name of the catalog.iceberg module to use", Required: true},
				{Name: "namespace", Type: "string", Description: "Namespace parts (e.g. [analytics, raw])", Required: true},
				{Name: "table", Type: "string", Description: "Table name", Required: true},
				{Name: "schema", Type: "object", Description: "Table schema definition with fields array", Required: false},
			},
		},
	}
}
