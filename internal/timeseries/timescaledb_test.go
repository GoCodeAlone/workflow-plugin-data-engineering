package timeseries

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// newMockDB creates a sql.DB backed by sqlmock for testing.
func newMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, mock
}

// newTimescaleModuleWithDB creates a TimescaleModule with an injected *sql.DB (bypassing sql.Open).
func newTimescaleModuleWithDB(t *testing.T, name string, db *sql.DB, hypertables []HypertableConfig) *TimescaleModule {
	t.Helper()
	m := &TimescaleModule{
		name: name,
		config: TimescaleConfig{
			Connection:   "mock://",
			MaxOpenConns: 5,
			MaxIdleConns: 2,
			Hypertables:  hypertables,
		},
		db: db,
	}
	t.Cleanup(func() { Unregister(name) })
	return m
}

func TestTimescaleModule_Init_OpenDB(t *testing.T) {
	_, err := NewTimescaleModule("ts-init", map[string]any{
		"connection": "mock://localhost/db",
	})
	if err != nil {
		t.Fatalf("NewTimescaleModule: %v", err)
	}
}

func TestTimescaleModule_InvalidConfig(t *testing.T) {
	_, err := NewTimescaleModule("ts-bad", map[string]any{})
	if err == nil {
		t.Error("expected error for missing connection, got nil")
	}
}

func TestTimescaleModule_Start_ChecksExtension(t *testing.T) {
	db, mock := newMockDB(t)
	m := newTimescaleModuleWithDB(t, "ts-ext-check", db, nil)

	mock.ExpectQuery(`SELECT extversion FROM pg_extension`).
		WillReturnRows(sqlmock.NewRows([]string{"extversion"}).AddRow("2.13.0"))

	// Directly call checkExtension since we control the DB.
	if err := m.checkExtension(context.Background()); err != nil {
		t.Fatalf("checkExtension: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestTimescaleModule_Start_ChecksExtension_Missing(t *testing.T) {
	db, mock := newMockDB(t)
	m := newTimescaleModuleWithDB(t, "ts-no-ext", db, nil)

	mock.ExpectQuery(`SELECT extversion FROM pg_extension`).
		WillReturnRows(sqlmock.NewRows([]string{"extversion"})) // empty result

	err := m.checkExtension(context.Background())
	if err == nil {
		t.Fatal("expected error when timescaledb extension not installed")
	}
	if !strings.Contains(err.Error(), "not installed") {
		t.Errorf("error %q should mention 'not installed'", err.Error())
	}
}

func TestTimescaleModule_CreateHypertable(t *testing.T) {
	db, mock := newMockDB(t)
	m := newTimescaleModuleWithDB(t, "ts-hyper", db, nil)

	mock.ExpectExec(`SELECT create_hypertable`).
		WithArgs("metrics", "time", "1 day").
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := m.CreateHypertable(context.Background(), "metrics", "time", "1d"); err != nil {
		t.Fatalf("CreateHypertable: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestTimescaleWriter_WritePoint(t *testing.T) {
	db, mock := newMockDB(t)
	m := newTimescaleModuleWithDB(t, fmt.Sprintf("ts-write-%d", time.Now().UnixNano()), db, nil)

	ts := time.Date(2023, 6, 1, 12, 0, 0, 0, time.UTC)
	tags := map[string]string{"host": "server1"}
	fields := map[string]any{"cpu": 42.5}

	mock.ExpectExec(`INSERT INTO`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := m.WritePoint(context.Background(), "cpu_metrics", tags, fields, ts); err != nil {
		t.Fatalf("WritePoint: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestTimescaleWriter_WriteBatch(t *testing.T) {
	db, mock := newMockDB(t)
	m := newTimescaleModuleWithDB(t, fmt.Sprintf("ts-batch-%d", time.Now().UnixNano()), db, nil)

	ts := time.Now()
	points := []Point{
		{Measurement: "cpu", Tags: map[string]string{"h": "s1"}, Fields: map[string]any{"v": 1.0}, Timestamp: ts},
		{Measurement: "mem", Tags: map[string]string{"h": "s1"}, Fields: map[string]any{"v": 80.0}, Timestamp: ts},
	}

	mock.ExpectExec(`INSERT INTO`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO`).WillReturnResult(sqlmock.NewResult(1, 1))

	if err := m.WriteBatch(context.Background(), points); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestTimescaleWriter_Query(t *testing.T) {
	db, mock := newMockDB(t)
	m := newTimescaleModuleWithDB(t, fmt.Sprintf("ts-query-%d", time.Now().UnixNano()), db, nil)

	rows := sqlmock.NewRows([]string{"time", "value", "host"}).
		AddRow(time.Now(), 42.5, "server1").
		AddRow(time.Now(), 38.1, "server2")

	mock.ExpectQuery(`SELECT`).WillReturnRows(rows)

	result, err := m.Query(context.Background(), "SELECT time, value, host FROM cpu_metrics LIMIT 10")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result))
	}
	if result[0]["host"] != "server1" {
		t.Errorf("host = %v, want server1", result[0]["host"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestTimescaleWriter_WritePoint_NotStarted(t *testing.T) {
	m := &TimescaleModule{name: "ts-not-started"}
	err := m.WritePoint(context.Background(), "test", nil, map[string]any{"v": 1}, time.Now())
	if err == nil {
		t.Error("expected error for module not started")
	}
}

func TestTSContinuousQuery_Create(t *testing.T) {
	db, mock := newMockDB(t)
	modName := fmt.Sprintf("ts-cq-create-%d", time.Now().UnixNano())
	m := newTimescaleModuleWithDB(t, modName, db, nil)

	if err := Register(modName, m); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { Unregister(modName) })

	mock.ExpectExec(`CREATE MATERIALIZED VIEW`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`SELECT add_continuous_aggregate_policy`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	step, err := NewTSContinuousQueryStep("cq1", nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":          modName,
		"viewName":        "hourly_metrics",
		"action":          "create",
		"query":           "SELECT time_bucket('1 hour', time) AS bucket, AVG(value) FROM metrics GROUP BY bucket",
		"refreshInterval": "30m",
		"startOffset":     "2h",
		"endOffset":       "1h",
		"materialized":    true,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "created" {
		t.Errorf("status = %v, want created", result.Output["status"])
	}
	if result.Output["viewName"] != "hourly_metrics" {
		t.Errorf("viewName = %v, want hourly_metrics", result.Output["viewName"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestTSContinuousQuery_Refresh(t *testing.T) {
	db, mock := newMockDB(t)
	modName := fmt.Sprintf("ts-cq-refresh-%d", time.Now().UnixNano())
	m := newTimescaleModuleWithDB(t, modName, db, nil)

	if err := Register(modName, m); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { Unregister(modName) })

	mock.ExpectExec(`CALL refresh_continuous_aggregate`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	step, _ := NewTSContinuousQueryStep("cq-refresh", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":   modName,
		"viewName": "hourly_metrics",
		"action":   "refresh",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "refreshed" {
		t.Errorf("status = %v, want refreshed", result.Output["status"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestTSContinuousQuery_Drop(t *testing.T) {
	db, mock := newMockDB(t)
	modName := fmt.Sprintf("ts-cq-drop-%d", time.Now().UnixNano())
	m := newTimescaleModuleWithDB(t, modName, db, nil)

	if err := Register(modName, m); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { Unregister(modName) })

	mock.ExpectExec(`DROP MATERIALIZED VIEW`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	step, _ := NewTSContinuousQueryStep("cq-drop", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":   modName,
		"viewName": "hourly_metrics",
		"action":   "drop",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "dropped" {
		t.Errorf("status = %v, want dropped", result.Output["status"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestTSContinuousQuery_WrongModule(t *testing.T) {
	// Register an InfluxModule under this name to trigger the type-assertion error.
	fakeName := fmt.Sprintf("ts-wrong-type-%d", time.Now().UnixNano())
	// Create a minimal writer that is not *TimescaleModule.
	Register(fakeName, &mockTSWriter{})
	defer Unregister(fakeName)

	step, _ := NewTSContinuousQueryStep("cq-wrong", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":   fakeName,
		"viewName": "v1",
		"action":   "create",
		"query":    "SELECT 1",
	})
	if err == nil {
		t.Error("expected error for wrong module type")
	}
}

// mockTSWriter is a minimal TimeSeriesWriter for testing wrong-type assertions.
type mockTSWriter struct{}

func (m *mockTSWriter) WritePoint(_ context.Context, _ string, _ map[string]string, _ map[string]any, _ time.Time) error {
	return nil
}
func (m *mockTSWriter) WriteBatch(_ context.Context, _ []Point) error { return nil }
func (m *mockTSWriter) Query(_ context.Context, _ string, _ ...any) ([]map[string]any, error) {
	return nil, nil
}
