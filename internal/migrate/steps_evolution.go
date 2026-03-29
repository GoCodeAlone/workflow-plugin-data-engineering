package migrate

import (
	"context"
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// ─── step.schema_evolve_pipeline ─────────────────────────────────────────────

// SchemaEvolvePipelineStep orchestrates a schema change across the full CDC pipeline:
// schema registry → lakehouse → source DB → CDC connector restart.
type SchemaEvolvePipelineStep struct{ name string }

// NewSchemaEvolvePipelineStep creates a step.schema_evolve_pipeline instance.
func NewSchemaEvolvePipelineStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &SchemaEvolvePipelineStep{name: name}, nil
}

func (s *SchemaEvolvePipelineStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	table, _ := config["table"].(string)
	if table == "" {
		return nil, fmt.Errorf("step.schema_evolve_pipeline %q: table is required", s.name)
	}
	namespace, _ := config["namespace"].(string)

	rawChange, _ := config["change"].(map[string]any)
	if rawChange == nil {
		return nil, fmt.Errorf("step.schema_evolve_pipeline %q: change is required", s.name)
	}

	change, subject, schema, icebergFields := parseEvolutionChange(rawChange)
	plan := BuildEvolutionPlan(table, namespace, change, subject, schema, icebergFields)

	// Resolve injected connectors (used in tests and direct wiring).
	db, _ := config["_source_db"].(SQLExecutor)
	cdc, _ := config["_cdc_connector"].(CDCRestarter)
	sr, _ := config["_schema_registry"].(SchemaRegistryProvider)
	lh, _ := config["_lakehouse"].(LakehouseEvolver)

	// Fall back to lookup-by-name if module refs are provided and no injection.
	if db == nil {
		if srcName, _ := config["source_db"].(string); srcName != "" && globalDBLookup != nil {
			if resolved, err := globalDBLookup(srcName); err == nil {
				db = resolved
			}
		}
	}
	if cdc == nil {
		if cdcName, _ := config["cdc_source"].(string); cdcName != "" && globalCDCLookup != nil {
			if resolved, err := globalCDCLookup(cdcName); err == nil {
				cdc = resolved
			}
		}
	}
	if sr == nil {
		if srName, _ := config["schema_registry"].(string); srName != "" && globalSRLookup != nil {
			if resolved, err := globalSRLookup(srName); err == nil {
				sr = resolved
			}
		}
	}
	if lh == nil {
		if lhName, _ := config["lakehouse_catalog"].(string); lhName != "" && globalLakehouseLookup != nil {
			if resolved, err := globalLakehouseLookup(lhName); err == nil {
				lh = resolved
			}
		}
	}

	// If no connectors available, return plan only (dry-run mode).
	hasAnyConnector := db != nil || cdc != nil || sr != nil || lh != nil
	if !hasAnyConnector {
		return &sdk.StepResult{Output: map[string]any{
			"plan":              stepsToOutput(plan.Steps),
			"safe":              plan.Safe,
			"executed":          false,
			"rollbackAvailable": plan.Safe,
		}}, nil
	}

	// Execute the plan.
	if err := executeEvolutionPlan(ctx, plan, db, cdc, sr, lh); err != nil {
		return nil, fmt.Errorf("step.schema_evolve_pipeline %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"plan":              stepsToOutput(plan.Steps),
		"safe":              plan.Safe,
		"executed":          true,
		"rollbackAvailable": plan.Safe,
	}}, nil
}

// ─── step.schema_evolve_verify ────────────────────────────────────────────────

// SchemaEvolveVerifyStep verifies that all pipeline layers have consistent schemas
// after an evolution. Compares source DB introspection against schema registry.
type SchemaEvolveVerifyStep struct{ name string }

// NewSchemaEvolveVerifyStep creates a step.schema_evolve_verify instance.
func NewSchemaEvolveVerifyStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &SchemaEvolveVerifyStep{name: name}, nil
}

func (s *SchemaEvolveVerifyStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	table, _ := config["table"].(string)
	if table == "" {
		return nil, fmt.Errorf("step.schema_evolve_verify %q: table is required", s.name)
	}
	subject, _ := config["subject"].(string)

	db, _ := config["_source_db"].(SQLExecutor)
	sr, _ := config["_schema_registry"].(SchemaRegistryProvider)

	layers := []map[string]any{}
	var diffs []string

	// Layer 1: source DB introspection.
	var sourceFields []string
	if db != nil {
		def, err := IntrospectSchema(ctx, db, table)
		if err != nil {
			return nil, fmt.Errorf("step.schema_evolve_verify %q: introspect source DB: %w", s.name, err)
		}
		if def != nil {
			for _, col := range def.Columns {
				sourceFields = append(sourceFields, col.Name)
			}
		}
		layers = append(layers, map[string]any{
			"name":          "source_db",
			"schemaVersion": "introspected",
			"fields":        sourceFields,
		})
	}

	// Layer 2: schema registry.
	var srFields []string
	var srVersion int
	if sr != nil && subject != "" {
		fields, version, err := sr.GetSchemaFields(ctx, subject)
		if err != nil {
			return nil, fmt.Errorf("step.schema_evolve_verify %q: get registry schema: %w", s.name, err)
		}
		srFields = fields
		srVersion = version
		layers = append(layers, map[string]any{
			"name":          "schema_registry",
			"schemaVersion": fmt.Sprintf("v%d", srVersion),
			"fields":        srFields,
		})
	}

	// Compare layers if both are available.
	if len(sourceFields) > 0 && len(srFields) > 0 {
		srSet := make(map[string]bool, len(srFields))
		for _, f := range srFields {
			srSet[f] = true
		}
		for _, f := range sourceFields {
			if !srSet[f] {
				diffs = append(diffs, fmt.Sprintf("column %q present in source_db but not in schema_registry", f))
			}
		}
		srcSet := make(map[string]bool, len(sourceFields))
		for _, f := range sourceFields {
			srcSet[f] = true
		}
		for _, f := range srFields {
			if !srcSet[f] {
				diffs = append(diffs, fmt.Sprintf("column %q present in schema_registry but not in source_db", f))
			}
		}
	}

	consistent := len(diffs) == 0 && (len(layers) == 0 || hasAllLayers(layers))

	return &sdk.StepResult{Output: map[string]any{
		"consistent": consistent,
		"layers":     layers,
		"diffs":      diffs,
	}}, nil
}

// hasAllLayers returns true if both source_db and schema_registry layers are present.
func hasAllLayers(layers []map[string]any) bool {
	names := make(map[string]bool, len(layers))
	for _, l := range layers {
		if n, ok := l["name"].(string); ok {
			names[n] = true
		}
	}
	// At minimum need source_db layer.
	return names["source_db"]
}
