package quality

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// setupModule creates a ChecksModule backed by a new sqlmock DB and starts it.
// Returns the mock and a cleanup func.
func setupModule(t *testing.T, modName string) (sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	mod := NewChecksModuleWithExecutor(modName, db)
	if err := mod.Start(context.Background()); err != nil {
		db.Close()
		t.Fatalf("module Start: %v", err)
	}
	cleanup := func() {
		mod.Stop(context.Background())
		db.Close()
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled sqlmock expectations: %v", err)
		}
	}
	return mock, cleanup
}

// ── step.quality_check ────────────────────────────────────────────────────────

func TestQualityCheck_AllTypes(t *testing.T) {
	mock, cleanup := setupModule(t, "qc-allcheck")
	defer cleanup()

	// not_null check: passes
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM events WHERE id IS NULL").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	// row_count check: passes
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM events").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(500))

	step, err := NewQualityCheckStep("s", nil)
	if err != nil {
		t.Fatalf("NewQualityCheckStep: %v", err)
	}
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "qc-allcheck",
		"table":  "events",
		"checks": []any{
			map[string]any{"type": "not_null", "config": map[string]any{"columns": []any{"id"}}},
			map[string]any{"type": "row_count", "config": map[string]any{"min": int64(100)}},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["passed"] != true {
		t.Errorf("expected passed=true, got %v", result.Output["passed"])
	}
	results, _ := result.Output["results"].([]any)
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestQualityCheck_MissingModule(t *testing.T) {
	step, _ := NewQualityCheckStep("s", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "nonexistent-module-xyz",
		"table":  "t",
		"checks": []any{map[string]any{"type": "row_count"}},
	})
	if err == nil {
		t.Error("expected error for missing module")
	}
}

func TestQualityCheck_MissingTable(t *testing.T) {
	_, cleanup := setupModule(t, "qc-notable")
	defer cleanup()

	step, _ := NewQualityCheckStep("s", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "qc-notable",
		"checks": []any{map[string]any{"type": "row_count"}},
	})
	if err == nil {
		t.Error("expected error for missing table")
	}
}

// ── step.quality_schema_validate ──────────────────────────────────────────────

func TestQualitySchemaValidate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mod := NewChecksModuleWithExecutor("qc-schema-val", db)
	if err := mod.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer mod.Stop(context.Background())

	// information_schema query.
	mock.ExpectQuery("SELECT column_name, data_type, is_nullable FROM information_schema.columns").
		WithArgs("public", "orders").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable"}).
			AddRow("id", "bigint", "NO").
			AddRow("total", "numeric", "YES"))

	// Write temp contract file.
	const contractYAML = `
dataset: public.orders
owner: finance
schema:
  columns:
    - name: id
      type: bigint
      nullable: false
`
	contractPath := writeContractFile(t, contractYAML)

	step, _ := NewQualitySchemaValidateStep("s", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":   "qc-schema-val",
		"contract": contractPath,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["schemaOk"] != true {
		t.Errorf("expected schemaOk=true, got %v; errors: %v",
			result.Output["schemaOk"], result.Output["schemaErrors"])
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

// ── step.quality_profile ──────────────────────────────────────────────────────

func TestQualityProfile(t *testing.T) {
	mock, cleanup := setupModule(t, "qc-profile")
	defer cleanup()

	// Row count.
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM metrics").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(50))
	// Column "score": null count, distinct, min/max, values.
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM metrics WHERE score IS NULL").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))
	mock.ExpectQuery("SELECT COUNT\\(DISTINCT score\\) FROM metrics").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(45))
	mock.ExpectQuery("SELECT MIN\\(score\\), MAX\\(score\\) FROM metrics").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(0.0, 100.0))
	mock.ExpectQuery("SELECT score FROM metrics WHERE score IS NOT NULL").
		WillReturnRows(sqlmock.NewRows([]string{"score"}).
			AddRow(10.0).AddRow(50.0).AddRow(90.0))

	step, _ := NewQualityProfileStep("s", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":  "qc-profile",
		"table":   "metrics",
		"columns": []any{"score"},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["table"] != "metrics" {
		t.Errorf("table: got %v", result.Output["table"])
	}
	if result.Output["rowCount"] != int64(50) {
		t.Errorf("rowCount: got %v", result.Output["rowCount"])
	}
}

// ── step.quality_compare ──────────────────────────────────────────────────────

func TestQualityCompare_Pass(t *testing.T) {
	mock, cleanup := setupModule(t, "qc-compare-pass")
	defer cleanup()

	// Baseline count.
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM baseline_orders").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1000))
	// Current count (within 5% tolerance).
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM current_orders").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1030))

	step, _ := NewQualityCompareStep("s", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":   "qc-compare-pass",
		"baseline": "baseline_orders",
		"current":  "current_orders",
		"tolerances": map[string]any{
			"row_count": 0.05,
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["passed"] != true {
		t.Errorf("expected passed=true (3%% drift within 5%% tolerance)")
	}
}

func TestQualityCompare_Drift(t *testing.T) {
	mock, cleanup := setupModule(t, "qc-compare-drift")
	defer cleanup()

	// Baseline count.
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM baseline_orders").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1000))
	// Current count (20% drift — exceeds 5% tolerance).
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM current_orders").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1200))

	step, _ := NewQualityCompareStep("s", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":   "qc-compare-drift",
		"baseline": "baseline_orders",
		"current":  "current_orders",
		"tolerances": map[string]any{
			"row_count": 0.05,
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["passed"] != false {
		t.Error("expected passed=false (20% drift exceeds 5% tolerance)")
	}
}

// ── step.quality_anomaly ──────────────────────────────────────────────────────

func TestQualityAnomaly(t *testing.T) {
	mock, cleanup := setupModule(t, "qc-anomaly")
	defer cleanup()

	// Fetch values for column "latency" — 10 normal values + outlier at 1000.
	// With 10 values near 10 and one at 1000, z-score of 1000 ≈ 3.0 > threshold 2.0.
	mock.ExpectQuery("SELECT latency FROM requests WHERE latency IS NOT NULL").
		WillReturnRows(sqlmock.NewRows([]string{"latency"}).
			AddRow(10.0).AddRow(10.0).AddRow(10.0).AddRow(10.0).AddRow(10.0).
			AddRow(10.0).AddRow(10.0).AddRow(10.0).AddRow(10.0).AddRow(10.0).
			AddRow(1000.0))

	step, _ := NewQualityAnomalyStep("s", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":    "qc-anomaly",
		"table":     "requests",
		"columns":   []any{"latency"},
		"method":    "zscore",
		"threshold": 2.0,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	results, _ := result.Output["results"].([]*AnomalyResult)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Anomalies < 1 {
		t.Errorf("expected outlier at 9999 to be flagged, got %d anomalies", results[0].Anomalies)
	}
	if results[0].Column != "latency" {
		t.Errorf("column: got %q, want latency", results[0].Column)
	}
}
