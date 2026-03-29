package internal_test

import (
	"context"
	"testing"

	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal"
)

func newRoadmapPlugin(t *testing.T) fullPlugin {
	t.Helper()
	p, ok := internal.NewDataEngineeringPlugin("roadmap-test").(fullPlugin)
	if !ok {
		t.Fatal("plugin does not implement all expected provider interfaces")
	}
	return p
}

// ─── Feature 1: Schema evolution pipeline ────────────────────────────────────

func TestPlugin_SchemaEvolvePipeline(t *testing.T) {
	p := newRoadmapPlugin(t)
	ctx := context.Background()

	step, err := p.CreateStep("step.schema_evolve_pipeline", "evolve", nil)
	if err != nil {
		t.Fatalf("CreateStep: %v", err)
	}

	// Dry-run: no executors injected → plan returned, executed=false.
	result, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"table":     "users",
		"namespace": "prod",
		"change": map[string]any{
			"type":        "add_column",
			"description": "add email_lower column",
			"sql":         "ALTER TABLE users ADD COLUMN email_lower text;",
			"safe":        true,
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["executed"] != false {
		t.Errorf("expected executed=false in dry-run, got %v", result.Output["executed"])
	}
	plan, _ := result.Output["plan"].([]map[string]any)
	if len(plan) == 0 {
		t.Error("expected non-empty plan")
	}
}

func TestPlugin_SchemaEvolveVerify(t *testing.T) {
	p := newRoadmapPlugin(t)
	ctx := context.Background()

	step, err := p.CreateStep("step.schema_evolve_verify", "verify", nil)
	if err != nil {
		t.Fatalf("CreateStep: %v", err)
	}

	// No executors → no layers, consistent=true by default (nothing to compare).
	result, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"table": "users",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	layers, _ := result.Output["layers"].([]map[string]any)
	if len(layers) != 0 {
		t.Errorf("expected 0 layers without executors, got %d", len(layers))
	}
}

// ─── Feature 2: Expand-contract migrations ───────────────────────────────────

func TestPlugin_ExpandContract(t *testing.T) {
	p := newRoadmapPlugin(t)
	ctx := context.Background()

	expandStep, err := p.CreateStep("step.migrate_expand", "expand", nil)
	if err != nil {
		t.Fatalf("CreateStep expand: %v", err)
	}

	// Dry-run: no executor → status="dry_run".
	result, err := expandStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"table": "users",
		"changes": []any{
			map[string]any{
				"type":      "rename_column",
				"oldColumn": "email",
				"newColumn": "email_lower",
				"newType":   "text",
				"transform": "lower(email)",
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute expand dry-run: %v", err)
	}
	if result.Output["status"] != "dry_run" {
		t.Errorf("expected status=dry_run, got %v", result.Output["status"])
	}

	contractStep, err := p.CreateStep("step.migrate_contract", "contract", nil)
	if err != nil {
		t.Fatalf("CreateStep contract: %v", err)
	}
	result, err = contractStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"table": "users",
		"changes": []any{
			map[string]any{
				"type":      "rename_column",
				"oldColumn": "email",
				"newColumn": "email_lower",
				"newType":   "text",
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute contract dry-run: %v", err)
	}
	if result.Output["status"] != "dry_run" {
		t.Errorf("expected status=dry_run, got %v", result.Output["status"])
	}

	statusStep, err := p.CreateStep("step.migrate_expand_status", "expand-status", nil)
	if err != nil {
		t.Fatalf("CreateStep expand-status: %v", err)
	}
	result, err = statusStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"table": "users",
	})
	if err != nil {
		t.Fatalf("Execute expand-status dry-run: %v", err)
	}
	if result.Output["expanded"] != false {
		t.Errorf("expected expanded=false in dry-run, got %v", result.Output["expanded"])
	}
}

// ─── Feature 3: Catalog lineage ──────────────────────────────────────────────

func TestPlugin_CatalogLineage(t *testing.T) {
	p := newRoadmapPlugin(t)
	ctx := context.Background()

	lineageStep, err := p.CreateStep("step.catalog_lineage", "lineage", nil)
	if err != nil {
		t.Fatalf("CreateStep catalog_lineage: %v", err)
	}

	// No catalog registered → error.
	_, err = lineageStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"catalog":  "nonexistent",
		"pipeline": "etl",
		"upstream": []any{
			map[string]any{"dataset": "raw.users", "platform": "postgres"},
		},
		"downstream": []any{
			map[string]any{"dataset": "analytics.users", "platform": "iceberg"},
		},
	})
	if err == nil {
		t.Error("expected error for missing catalog")
	}

	queryStep, err := p.CreateStep("step.catalog_lineage_query", "lineage-query", nil)
	if err != nil {
		t.Fatalf("CreateStep catalog_lineage_query: %v", err)
	}
	_, err = queryStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"catalog":   "nonexistent",
		"dataset":   "raw.users",
		"direction": "downstream",
		"depth":     2,
	})
	if err == nil {
		t.Error("expected error for missing catalog in lineage query")
	}
}

// ─── Feature 4: Hot/cold tier management ─────────────────────────────────────

func TestPlugin_TSArchive(t *testing.T) {
	p := newRoadmapPlugin(t)
	ctx := context.Background()

	archiveStep, err := p.CreateStep("step.ts_archive", "archive", nil)
	if err != nil {
		t.Fatalf("CreateStep ts_archive: %v", err)
	}

	// Module not registered → error.
	_, err = archiveStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"module":      "nonexistent",
		"olderThan":   "30d",
		"destination": "file:///tmp/archive",
		"format":      "parquet",
	})
	if err == nil {
		t.Error("expected error for missing ts module")
	}

	tierStatusStep, err := p.CreateStep("step.ts_tier_status", "tier-status", nil)
	if err != nil {
		t.Fatalf("CreateStep ts_tier_status: %v", err)
	}
	_, err = tierStatusStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"module":      "nonexistent",
		"measurement": "sensors",
	})
	if err == nil {
		t.Error("expected error for missing ts module in tier status")
	}
}

// ─── Feature 5: LLM-powered entity extraction ────────────────────────────────

func TestPlugin_LLMExtract(t *testing.T) {
	p := newRoadmapPlugin(t)
	ctx := context.Background()

	step, err := p.CreateStep("step.graph_extract_entities_llm", "llm-extract", nil)
	if err != nil {
		t.Fatalf("CreateStep graph_extract_entities_llm: %v", err)
	}

	// Empty text → empty entities, no error.
	result, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"text":     "",
		"provider": "claude",
		"types":    []any{"person", "org"},
	})
	if err != nil {
		t.Fatalf("Execute with empty text: %v", err)
	}
	entities, _ := result.Output["entities"].([]map[string]any)
	if len(entities) != 0 {
		t.Errorf("expected 0 entities for empty text, got %d", len(entities))
	}

	// Non-empty text but no apiKey → error.
	_, err = step.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"text":     "Alice works at Acme Corp in Paris.",
		"provider": "claude",
	})
	if err == nil {
		t.Error("expected error when apiKey is missing")
	}
}

// ─── Feature 6: ClickHouse materialized views ────────────────────────────────

func TestPlugin_ClickHouseView(t *testing.T) {
	p := newRoadmapPlugin(t)
	ctx := context.Background()

	step, err := p.CreateStep("step.ts_clickhouse_view", "ch-view", nil)
	if err != nil {
		t.Fatalf("CreateStep ts_clickhouse_view: %v", err)
	}

	// Module not registered → error.
	_, err = step.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"module":   "nonexistent",
		"viewName": "hourly_events",
		"action":   "create",
		"query":    "SELECT toStartOfHour(ts) AS hour, count() FROM events GROUP BY hour",
		"orderBy":  "hour",
	})
	if err == nil {
		t.Error("expected error for missing ClickHouse module")
	}

	// Invalid viewName → error.
	_, err = step.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"module":   "any_module",
		"viewName": "invalid-name!",
		"action":   "create",
	})
	if err == nil {
		t.Error("expected error for invalid viewName identifier")
	}
}

// ─── Feature 7: Dynamic tenant tier promotion ────────────────────────────────

func TestPlugin_TenantPromotion(t *testing.T) {
	p := newRoadmapPlugin(t)
	ctx := context.Background()

	// step.tenant_evaluate_promotion: pure logic, no DB needed.
	evalStep, err := p.CreateStep("step.tenant_evaluate_promotion", "eval-promo", nil)
	if err != nil {
		t.Fatalf("CreateStep tenant_evaluate_promotion: %v", err)
	}

	// Metrics below threshold → no promotion.
	result, err := evalStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"tenant_id":    "tenant_a",
		"current_tier": "row_level",
		"metrics": map[string]any{
			"rowCount":  float64(500_000),
			"queryRate": float64(50),
		},
	})
	if err != nil {
		t.Fatalf("Execute evaluate_promotion (below threshold): %v", err)
	}
	if result.Output["should_promote"] != false {
		t.Errorf("expected should_promote=false, got %v", result.Output["should_promote"])
	}

	// Metrics above threshold → promotion recommended.
	result, err = evalStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"tenant_id":    "tenant_b",
		"current_tier": "row_level",
		"metrics": map[string]any{
			"rowCount": float64(5_000_000),
		},
	})
	if err != nil {
		t.Fatalf("Execute evaluate_promotion (above threshold): %v", err)
	}
	if result.Output["should_promote"] != true {
		t.Errorf("expected should_promote=true, got %v", result.Output["should_promote"])
	}
	if result.Output["recommended_tier"] != "schema_per_tenant" {
		t.Errorf("expected recommended_tier=schema_per_tenant, got %v", result.Output["recommended_tier"])
	}

	// step.tenant_promote: no DB → error.
	promoteStep, err := p.CreateStep("step.tenant_promote", "promote", nil)
	if err != nil {
		t.Fatalf("CreateStep tenant_promote: %v", err)
	}
	_, err = promoteStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"tenant_id":       "tenant_a",
		"target_strategy": "schema_per_tenant",
		"tables":          []any{"users", "orders"},
	})
	if err == nil {
		t.Error("expected error for tenant_promote with no DB")
	}

	// step.tenant_demote: no DB → error.
	demoteStep, err := p.CreateStep("step.tenant_demote", "demote", nil)
	if err != nil {
		t.Fatalf("CreateStep tenant_demote: %v", err)
	}
	_, err = demoteStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"tenant_id": "tenant_a",
		"from_tier": "schema_per_tenant",
		"tables":    []any{"users"},
	})
	if err == nil {
		t.Error("expected error for tenant_demote with no DB")
	}
}

// ─── Feature 8: CDC backpressure monitoring ──────────────────────────────────

func TestPlugin_CDCBackpressure(t *testing.T) {
	p := newRoadmapPlugin(t)
	ctx := context.Background()

	bpStep, err := p.CreateStep("step.cdc_backpressure", "bp", nil)
	if err != nil {
		t.Fatalf("CreateStep cdc_backpressure: %v", err)
	}

	// Source not registered → error.
	_, err = bpStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"source_id": "nonexistent",
		"action":    "check",
		"thresholds": map[string]any{
			"lag_bytes":   int64(1_073_741_824),
			"lag_seconds": int64(300),
		},
	})
	if err == nil {
		t.Error("expected error for missing CDC source in backpressure check")
	}

	monitorStep, err := p.CreateStep("step.cdc_monitor", "monitor", nil)
	if err != nil {
		t.Fatalf("CreateStep cdc_monitor: %v", err)
	}
	_, err = monitorStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"source_id": "nonexistent",
	})
	if err == nil {
		t.Error("expected error for missing CDC source in monitor")
	}
}
