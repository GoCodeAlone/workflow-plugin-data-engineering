package timeseries

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func newCHModuleForViewTest(t *testing.T, name string, mockConn *mockClickHouseConn) *ClickHouseModule {
	t.Helper()
	m := &ClickHouseModule{
		name:   name,
		config: ClickHouseConfig{Endpoints: []string{"localhost:9000"}, Database: "analytics"},
		conn:   mockConn,
	}
	if err := Register(name, m); err != nil {
		t.Fatalf("register %q: %v", name, err)
	}
	t.Cleanup(func() { Unregister(name) })
	return m
}

func TestClickHouseView_Create(t *testing.T) {
	mockConn := &mockClickHouseConn{}
	name := fmt.Sprintf("ch-view-create-%d", time.Now().UnixNano())
	newCHModuleForViewTest(t, name, mockConn)

	step, _ := NewTSClickHouseViewStep("view1", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":      name,
		"viewName":    "hourly_agg",
		"action":      "create",
		"query":       "SELECT toStartOfHour(ts) as hour, avg(value) as avg_val FROM events GROUP BY hour",
		"engine":      "AggregatingMergeTree()",
		"orderBy":     "hour",
		"partitionBy": "toYYYYMM(hour)",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "created" {
		t.Errorf("expected status=created, got %v", result.Output["status"])
	}
	if result.Output["viewName"] != "hourly_agg" {
		t.Errorf("expected viewName=hourly_agg, got %v", result.Output["viewName"])
	}
	if result.Output["engine"] != "AggregatingMergeTree()" {
		t.Errorf("expected engine=AggregatingMergeTree(), got %v", result.Output["engine"])
	}

	// Verify DDL was executed
	mockConn.mu.Lock()
	queries := mockConn.execQueries
	mockConn.mu.Unlock()
	if len(queries) == 0 {
		t.Fatal("expected DDL to be executed")
	}
	ddl := queries[0]
	if !strings.Contains(ddl, "CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_agg") {
		t.Errorf("DDL missing CREATE MATERIALIZED VIEW: %q", ddl)
	}
	if !strings.Contains(ddl, "AggregatingMergeTree()") {
		t.Errorf("DDL missing engine: %q", ddl)
	}
	if !strings.Contains(ddl, "ORDER BY (hour)") {
		t.Errorf("DDL missing ORDER BY: %q", ddl)
	}
}

func TestClickHouseView_Drop(t *testing.T) {
	mockConn := &mockClickHouseConn{}
	name := fmt.Sprintf("ch-view-drop-%d", time.Now().UnixNano())
	newCHModuleForViewTest(t, name, mockConn)

	step, _ := NewTSClickHouseViewStep("view_drop", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":   name,
		"viewName": "old_view",
		"action":   "drop",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "dropped" {
		t.Errorf("expected status=dropped, got %v", result.Output["status"])
	}

	mockConn.mu.Lock()
	queries := mockConn.execQueries
	mockConn.mu.Unlock()
	if len(queries) == 0 {
		t.Fatal("expected DROP SQL to be executed")
	}
	if !strings.Contains(queries[0], "DROP VIEW IF EXISTS old_view") {
		t.Errorf("expected DROP VIEW IF EXISTS in SQL, got: %q", queries[0])
	}
}

func TestClickHouseView_Status(t *testing.T) {
	mockConn := &mockClickHouseConn{
		queryRows: &mockRows{
			cols: []string{"name", "engine", "total_rows", "total_bytes"},
			colData: [][]any{
				{"my_view", "AggregatingMergeTree", uint64(50000), uint64(1024000)},
			},
		},
	}
	name := fmt.Sprintf("ch-view-status-%d", time.Now().UnixNano())
	newCHModuleForViewTest(t, name, mockConn)

	step, _ := NewTSClickHouseViewStep("view_status", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":   name,
		"viewName": "my_view",
		"action":   "status",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["isActive"] != true {
		t.Errorf("expected isActive=true, got %v", result.Output["isActive"])
	}
	if result.Output["engine"] != "AggregatingMergeTree" {
		t.Errorf("expected engine=AggregatingMergeTree, got %v", result.Output["engine"])
	}
	if result.Output["rows"] != uint64(50000) {
		t.Errorf("expected rows=50000, got %v (%T)", result.Output["rows"], result.Output["rows"])
	}
}

func TestClickHouseView_IdentValidation(t *testing.T) {
	mockConn := &mockClickHouseConn{}
	name := fmt.Sprintf("ch-view-ident-%d", time.Now().UnixNano())
	newCHModuleForViewTest(t, name, mockConn)

	tests := []struct {
		viewName string
		wantErr  bool
	}{
		{"valid_view", false},
		{"_underscore", false},
		{"has space", true},
		{"dash-name", true},
		{"1bad", true},
		{"semi;colon", true},
		{"dot.name", true},
	}
	for _, tc := range tests {
		t.Run(tc.viewName, func(t *testing.T) {
			step, _ := NewTSClickHouseViewStep("ident_test", nil)
			_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
				"module":   name,
				"viewName": tc.viewName,
				"action":   "create",
				"query":    "SELECT 1",
			})
			if tc.wantErr && err == nil {
				t.Errorf("viewName %q: expected error, got nil", tc.viewName)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("viewName %q: unexpected error: %v", tc.viewName, err)
			}
		})
	}
}

func TestClickHouseView_MissingModule(t *testing.T) {
	step, _ := NewTSClickHouseViewStep("view_bad", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":   fmt.Sprintf("nonexistent-%d", time.Now().UnixNano()),
		"viewName": "my_view",
		"action":   "create",
		"query":    "SELECT 1",
	})
	if err == nil {
		t.Fatal("expected error for missing module")
	}
}

func TestClickHouseView_Create_MissingQuery(t *testing.T) {
	mockConn := &mockClickHouseConn{}
	name := fmt.Sprintf("ch-view-noquery-%d", time.Now().UnixNano())
	newCHModuleForViewTest(t, name, mockConn)

	step, _ := NewTSClickHouseViewStep("view_noquery", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":   name,
		"viewName": "my_view",
		"action":   "create",
		// No query
	})
	if err == nil {
		t.Fatal("expected error for missing query")
	}
}

func TestClickHouseView_InvalidAction(t *testing.T) {
	mockConn := &mockClickHouseConn{}
	name := fmt.Sprintf("ch-view-badact-%d", time.Now().UnixNano())
	newCHModuleForViewTest(t, name, mockConn)

	step, _ := NewTSClickHouseViewStep("view_badaction", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":   name,
		"viewName": "my_view",
		"action":   "invalid_action",
	})
	if err == nil {
		t.Fatal("expected error for invalid action")
	}
}

func TestBuildClickHouseViewDDL(t *testing.T) {
	ddl, err := buildClickHouseViewDDL(
		"hourly_stats",
		"SummingMergeTree(amount)",
		"ts",
		"toYYYYMM(ts)",
		"SELECT toStartOfHour(ts) as ts, sum(amount) FROM sales GROUP BY ts",
	)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		substr string
	}{
		{"CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_stats"},
		{"ENGINE = SummingMergeTree(amount)"},
		{"ORDER BY (ts)"},
		{"PARTITION BY (toYYYYMM(ts))"},
		{"AS SELECT"},
	}
	for _, tc := range tests {
		if !strings.Contains(ddl, tc.substr) {
			t.Errorf("DDL missing %q\nGot: %s", tc.substr, ddl)
		}
	}
}
