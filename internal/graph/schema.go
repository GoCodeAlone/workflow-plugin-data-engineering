package graph

import sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"

// GraphModuleSchemas returns schema metadata for all graph module types.
func GraphModuleSchemas() []sdk.ModuleSchemaData {
	return []sdk.ModuleSchemaData{
		{
			Type:        "graph.neo4j",
			Label:       "Neo4j Graph Database",
			Category:    "Data Engineering",
			Description: "Neo4j property graph database module",
			ConfigFields: []sdk.ConfigField{
				{Name: "uri", Type: "string", Description: "Neo4j Bolt/Neo4j URI (e.g. bolt://neo4j:7687)", Required: true},
				{Name: "database", Type: "string", Description: "Neo4j database name", Required: false},
				{Name: "username", Type: "string", Description: "Neo4j username", Required: false},
				{Name: "password", Type: "string", Description: "Neo4j password", Required: false},
				{Name: "maxConnectionPoolSize", Type: "number", Description: "Maximum connection pool size", Required: false},
				{Name: "connectionAcquisitionTimeout", Type: "string", Description: "Connection acquisition timeout (e.g. 30s)", Required: false},
			},
		},
	}
}
