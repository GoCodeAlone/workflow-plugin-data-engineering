package migrate

// moduleSchemas returns JSON Schema definitions for UI forms.
func MigrateSchemas() map[string]any {
	return map[string]any{
		"migrate.schema": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"strategy": map[string]any{
					"type": "string",
					"enum": []string{"declarative", "scripted", "both"},
				},
				"target": map[string]any{"type": "string"},
				"schemas": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type":       "object",
						"properties": map[string]any{"path": map[string]any{"type": "string"}},
					},
				},
				"migrationsDir":    map[string]any{"type": "string"},
				"lockTable":        map[string]any{"type": "string"},
				"onBreakingChange": map[string]any{"type": "string", "enum": []string{"block", "warn", "blue_green"}},
			},
			"required": []string{"strategy"},
		},
		"step.migrate_plan": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"module":  map[string]any{"type": "string"},
				"desired": map[string]any{"type": "string"},
			},
			"required": []string{"module"},
		},
		"step.migrate_apply": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"module": map[string]any{"type": "string"},
				"plan":   map[string]any{"type": "array"},
				"mode":   map[string]any{"type": "string", "enum": []string{"online", "blue_green"}},
			},
			"required": []string{"module"},
		},
		"step.migrate_run": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"module":  map[string]any{"type": "string"},
				"script":  map[string]any{"type": "string"},
				"version": map[string]any{"type": "integer"},
			},
			"required": []string{"module"},
		},
		"step.migrate_rollback": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"module": map[string]any{"type": "string"},
				"steps":  map[string]any{"type": "integer", "default": 1},
			},
			"required": []string{"module"},
		},
		"step.migrate_status": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"module": map[string]any{"type": "string"},
			},
			"required": []string{"module"},
		},
		"step.migrate_expand": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"table":   map[string]any{"type": "string"},
				"changes": map[string]any{"type": "array"},
			},
			"required": []string{"table", "changes"},
		},
		"step.migrate_contract": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"table":   map[string]any{"type": "string"},
				"changes": map[string]any{"type": "array"},
				"verify":  map[string]any{"type": "boolean"},
			},
			"required": []string{"table", "changes"},
		},
		"step.migrate_expand_status": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"table":       map[string]any{"type": "string"},
				"triggerName": map[string]any{"type": "string"},
			},
			"required": []string{"table"},
		},
		"step.schema_evolve_pipeline": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"table":            map[string]any{"type": "string"},
				"namespace":        map[string]any{"type": "string"},
				"change":           map[string]any{"type": "object"},
				"source_db":        map[string]any{"type": "string"},
				"cdc_source":       map[string]any{"type": "string"},
				"schema_registry":  map[string]any{"type": "string"},
				"lakehouse_catalog": map[string]any{"type": "string"},
			},
			"required": []string{"table", "change"},
		},
		"step.schema_evolve_verify": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"table":            map[string]any{"type": "string"},
				"subject":          map[string]any{"type": "string"},
				"source_db":        map[string]any{"type": "string"},
				"schema_registry":  map[string]any{"type": "string"},
				"lakehouse_catalog": map[string]any{"type": "string"},
			},
			"required": []string{"table"},
		},
	}
}
