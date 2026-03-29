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
	}
}
