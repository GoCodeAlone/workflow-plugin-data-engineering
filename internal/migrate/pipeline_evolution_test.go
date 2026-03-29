package migrate

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// ── Mock implementations ─────────────────────────────────────────────────────

type mockCDCRestarter struct {
	stopCalled  bool
	startCalled bool
}

func (m *mockCDCRestarter) Stop(_ context.Context) error {
	m.stopCalled = true
	return nil
}

func (m *mockCDCRestarter) Start(_ context.Context) error {
	m.startCalled = true
	return nil
}

type mockSchemaRegistry struct {
	registerCount int
	compatible    bool
	fields        []string
}

func (m *mockSchemaRegistry) RegisterSubject(_ context.Context, _, _ string) (int, error) {
	m.registerCount++
	return m.registerCount, nil
}

func (m *mockSchemaRegistry) CheckCompatibility(_ context.Context, _, _ string) (bool, error) {
	return m.compatible, nil
}

func (m *mockSchemaRegistry) GetSchemaFields(_ context.Context, _ string) ([]string, int, error) {
	return m.fields, m.registerCount, nil
}

type mockLakehouseEvolver struct {
	evolveCalled bool
}

func (m *mockLakehouseEvolver) EvolveTable(_ context.Context, _ []string, _ string, _ map[string]any) error {
	m.evolveCalled = true
	return nil
}

// ── Tests ────────────────────────────────────────────────────────────────────

func TestSchemaEvolvePipeline_Plan(t *testing.T) {
	step, _ := NewSchemaEvolvePipelineStep("evolve", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"table":     "users",
		"namespace": "prod",
		"change": map[string]any{
			"type":        "add_column",
			"description": "add status column",
			"sql":         "ALTER TABLE users ADD COLUMN status text;",
			"safe":        true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["safe"] != true {
		t.Error("expected safe=true for add_column")
	}
	plan, _ := result.Output["plan"].([]map[string]any)
	if len(plan) == 0 {
		t.Error("expected non-empty plan steps")
	}
	// Should have at least source_db step
	foundSourceDB := false
	for _, step := range plan {
		if step["target"] == "source_db" {
			foundSourceDB = true
		}
	}
	if !foundSourceDB {
		t.Error("expected source_db step in plan")
	}
	if result.Output["executed"] != false {
		t.Errorf("expected executed=false when no executors provided, got %v", result.Output["executed"])
	}
}

func TestSchemaEvolvePipeline_Execute(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectExec("ALTER TABLE users ADD COLUMN status").WillReturnResult(sqlmock.NewResult(0, 0))

	cdc := &mockCDCRestarter{}
	sr := &mockSchemaRegistry{compatible: true, fields: []string{"id", "status"}}
	lh := &mockLakehouseEvolver{}

	step, _ := NewSchemaEvolvePipelineStep("evolve", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"table":     "users",
		"namespace": "prod",
		"change": map[string]any{
			"type":        "add_column",
			"description": "add status column",
			"sql":         "ALTER TABLE users ADD COLUMN status text;",
			"safe":        true,
			"schema":      `{"type":"record","fields":[{"name":"status","type":"string"}]}`,
			"subject":     "users-value",
		},
		"_source_db":       db,
		"_cdc_connector":   cdc,
		"_schema_registry": sr,
		"_lakehouse":       lh,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["executed"] != true {
		t.Errorf("expected executed=true, got %v", result.Output["executed"])
	}
	if !cdc.stopCalled || !cdc.startCalled {
		t.Error("expected CDC connector to be stopped then started (restarted)")
	}
	if sr.registerCount == 0 {
		t.Error("expected schema to be registered in schema registry")
	}
	if !lh.evolveCalled {
		t.Error("expected lakehouse schema evolution to be called")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestSchemaEvolvePipeline_IncompatibleSchema(t *testing.T) {
	sr := &mockSchemaRegistry{compatible: false}

	step, _ := NewSchemaEvolvePipelineStep("evolve", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"table":     "users",
		"namespace": "prod",
		"change": map[string]any{
			"type":    "drop_column",
			"sql":     "ALTER TABLE users DROP COLUMN legacy;",
			"safe":    false,
			"schema":  `{"type":"record","fields":[]}`,
			"subject": "users-value",
		},
		"_schema_registry": sr,
	})
	if err == nil {
		t.Error("expected error for incompatible schema change")
	}
}

func TestSchemaEvolvePipeline_MissingTable(t *testing.T) {
	step, _ := NewSchemaEvolvePipelineStep("evolve", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"change": map[string]any{"type": "add_column", "sql": "ALTER TABLE t ADD COLUMN x text;"},
	})
	if err == nil {
		t.Error("expected error when table is missing")
	}
}

func TestSchemaEvolvePipeline_MissingChange(t *testing.T) {
	step, _ := NewSchemaEvolvePipelineStep("evolve", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"table": "users",
	})
	if err == nil {
		t.Error("expected error when change is missing")
	}
}

func TestSchemaEvolveVerify_Consistent(t *testing.T) {
	db, mock := newMockDB(t)
	// Introspect source DB: table has id and status columns
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(1),
	)
	mock.ExpectQuery("SELECT.*column_name").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}).
			AddRow("id", "bigint", "NO", nil).
			AddRow("status", "text", "YES", nil),
	)
	mock.ExpectQuery("SELECT indexname").WillReturnRows(
		sqlmock.NewRows([]string{"indexname", "indexdef"}),
	)

	sr := &mockSchemaRegistry{fields: []string{"id", "status"}}

	step, _ := NewSchemaEvolveVerifyStep("verify", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"table":            "users",
		"subject":          "users-value",
		"_source_db":       db,
		"_schema_registry": sr,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["consistent"] != true {
		t.Errorf("expected consistent=true, got diffs: %v", result.Output["diffs"])
	}
	layers, _ := result.Output["layers"].([]map[string]any)
	if len(layers) != 2 {
		t.Errorf("expected 2 layers, got %d", len(layers))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestSchemaEvolveVerify_Drift(t *testing.T) {
	db, mock := newMockDB(t)
	// Source DB has id, status, AND new_col (3 columns)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(1),
	)
	mock.ExpectQuery("SELECT.*column_name").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}).
			AddRow("id", "bigint", "NO", nil).
			AddRow("status", "text", "YES", nil).
			AddRow("new_col", "text", "YES", nil),
	)
	mock.ExpectQuery("SELECT indexname").WillReturnRows(
		sqlmock.NewRows([]string{"indexname", "indexdef"}),
	)

	// Schema registry only has id and status (missing new_col)
	sr := &mockSchemaRegistry{fields: []string{"id", "status"}}

	step, _ := NewSchemaEvolveVerifyStep("verify", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"table":            "users",
		"subject":          "users-value",
		"_source_db":       db,
		"_schema_registry": sr,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["consistent"] != false {
		t.Error("expected consistent=false due to schema drift")
	}
	diffs, _ := result.Output["diffs"].([]string)
	if len(diffs) == 0 {
		t.Error("expected non-empty diffs when schema has drifted")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestSchemaEvolveVerify_NoExecutors(t *testing.T) {
	step, _ := NewSchemaEvolveVerifyStep("verify", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"table": "users",
	})
	if err != nil {
		t.Fatal(err)
	}
	// No layers if no executors provided.
	layers, _ := result.Output["layers"].([]map[string]any)
	if len(layers) != 0 {
		t.Errorf("expected no layers without executors, got %d", len(layers))
	}
}
