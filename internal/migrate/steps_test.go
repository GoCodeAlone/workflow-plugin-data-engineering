package migrate

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// registerTestModule creates and starts a SchemaModule and registers it under name.
// Returns a cleanup function that deregisters the module.
func registerTestModule(t *testing.T, name string, config map[string]any) *SchemaModule {
	t.Helper()
	m, err := NewSchemaModule(name, config)
	if err != nil {
		t.Fatalf("NewSchemaModule: %v", err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { m.Stop(context.Background()) }) //nolint:errcheck
	return m.(*SchemaModule)
}

func TestMigratePlan_DetectsAddColumn(t *testing.T) {
	dir := t.TempDir()
	schemaContent := `table: users
columns:
  - name: id
    type: bigint
    primaryKey: true
  - name: email
    type: text
`
	schemaPath := filepath.Join(dir, "users.yaml")
	if err := os.WriteFile(schemaPath, []byte(schemaContent), 0o600); err != nil {
		t.Fatal(err)
	}

	registerTestModule(t, "plan_add_col", map[string]any{
		"strategy": "declarative",
		"schemas":  []any{map[string]any{"path": schemaPath}},
	})

	db, mock := newMockDB(t)
	// introspect: table exists
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(1),
	)
	// columns: only id exists in live
	mock.ExpectQuery("SELECT.*column_name").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}).
			AddRow("id", "bigint", "NO", nil),
	)
	// indexes: none
	mock.ExpectQuery("SELECT indexname").WillReturnRows(
		sqlmock.NewRows([]string{"indexname", "indexdef"}),
	)

	step, _ := NewMigratePlanStep("s", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":    "plan_add_col",
		"_executor": db,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := result.Output["plan"].([]map[string]any)
	if len(plan) == 0 {
		t.Error("expected at least 1 change in plan")
	}
	if result.Output["changeCount"].(int) < 1 {
		t.Error("expected changeCount >= 1")
	}
}

func TestMigratePlan_DetectsBreaking(t *testing.T) {
	dir := t.TempDir()
	schemaContent := `table: events
columns:
  - name: id
    type: bigint
`
	schemaPath := filepath.Join(dir, "events.yaml")
	if err := os.WriteFile(schemaPath, []byte(schemaContent), 0o600); err != nil {
		t.Fatal(err)
	}

	registerTestModule(t, "plan_breaking", map[string]any{
		"strategy": "declarative",
		"schemas":  []any{map[string]any{"path": schemaPath}},
	})

	db, mock := newMockDB(t)
	// table exists, live has id + legacy column
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(1),
	)
	mock.ExpectQuery("SELECT.*column_name").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}).
			AddRow("id", "bigint", "NO", nil).
			AddRow("legacy", "text", "YES", nil),
	)
	mock.ExpectQuery("SELECT indexname").WillReturnRows(
		sqlmock.NewRows([]string{"indexname", "indexdef"}),
	)

	step, _ := NewMigratePlanStep("s", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":    "plan_breaking",
		"_executor": db,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["safe"].(bool) {
		t.Error("expected plan to be unsafe (has drop_column)")
	}
}

func TestMigrateApply_Online(t *testing.T) {
	registerTestModule(t, "apply_online", map[string]any{
		"strategy":        "declarative",
		"onBreakingChange": "warn",
	})

	db, mock := newMockDB(t)
	mock.ExpectExec("ALTER TABLE users ADD COLUMN email").WillReturnResult(sqlmock.NewResult(0, 0))

	step, _ := NewMigrateApplyStep("s", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "apply_online",
		"mode":   "online",
		"plan": []any{
			map[string]any{
				"type":        "add_column",
				"description": "add email",
				"sql":         "ALTER TABLE users ADD COLUMN email TEXT;",
				"breaking":    false,
			},
		},
		"_executor": db,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["status"] != "applied" {
		t.Errorf("expected status=applied, got %v", result.Output["status"])
	}
	if result.Output["changesApplied"].(int) != 1 {
		t.Error("expected 1 change applied")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestMigrateApply_BlocksBreaking(t *testing.T) {
	registerTestModule(t, "apply_block", map[string]any{
		"strategy":        "declarative",
		"onBreakingChange": "block",
	})

	step, _ := NewMigrateApplyStep("s", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "apply_block",
		"plan": []any{
			map[string]any{
				"type":        "drop_column",
				"description": "drop legacy",
				"sql":         "ALTER TABLE t DROP COLUMN legacy;",
				"breaking":    true,
			},
		},
	})
	if err == nil {
		t.Error("expected error for blocked breaking change")
	}
}

func TestMigrateApply_DryRun(t *testing.T) {
	registerTestModule(t, "apply_dry", map[string]any{"strategy": "declarative"})

	step, _ := NewMigrateApplyStep("s", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "apply_dry",
		"plan": []any{
			map[string]any{"type": "add_column", "sql": "ALTER TABLE t ADD COLUMN x TEXT;", "breaking": false},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["status"] != "dry_run" {
		t.Errorf("expected dry_run, got %v", result.Output["status"])
	}
}

func TestMigrateRun_Version(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "002_add_status.up.sql"), []byte("ALTER TABLE orders ADD COLUMN status TEXT;"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "002_add_status.down.sql"), []byte("ALTER TABLE orders DROP COLUMN status;"), 0o600); err != nil {
		t.Fatal(err)
	}

	registerTestModule(t, "run_ver", map[string]any{
		"strategy":      "scripted",
		"migrationsDir": dir,
	})

	db, mock := newMockDB(t)
	mock.ExpectExec("SELECT pg_advisory_lock").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT version, applied_at, checksum FROM schema_migrations").
		WillReturnRows(sqlmock.NewRows([]string{"version", "applied_at", "checksum"}))
	mock.ExpectExec("ALTER TABLE orders ADD COLUMN status").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO schema_migrations").
		WithArgs(2, sqlmock.AnyArg(), "add_status").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("SELECT pg_advisory_unlock").WillReturnResult(sqlmock.NewResult(0, 0))

	step, _ := NewMigrateRunStep("s", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":    "run_ver",
		"version":   2,
		"_executor": db,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["status"] != "migrated" {
		t.Errorf("expected status=migrated, got %v", result.Output["status"])
	}
	if result.Output["version"].(int) != 2 {
		t.Errorf("expected version=2, got %v", result.Output["version"])
	}
}

func TestMigrateRollback(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "001_init.up.sql"), []byte("CREATE TABLE x (id INT);"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "001_init.down.sql"), []byte("DROP TABLE x;"), 0o600); err != nil {
		t.Fatal(err)
	}

	registerTestModule(t, "rollback_mod", map[string]any{
		"strategy":      "scripted",
		"migrationsDir": dir,
	})

	db, mock := newMockDB(t)

	// Status (before)
	mock.ExpectQuery("SELECT version, applied_at, checksum FROM schema_migrations").
		WillReturnRows(sqlmock.NewRows([]string{"version", "applied_at", "checksum"}).
			AddRow(1, time.Now(), "abc"))

	// Rollback advisory lock + loadApplied + execute down + delete + unlock
	mock.ExpectExec("SELECT pg_advisory_lock").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT version, applied_at, checksum FROM schema_migrations").
		WillReturnRows(sqlmock.NewRows([]string{"version", "applied_at", "checksum"}).
			AddRow(1, time.Now(), "abc"))
	mock.ExpectExec("DROP TABLE x").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("DELETE FROM schema_migrations").WithArgs(1).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("SELECT pg_advisory_unlock").WillReturnResult(sqlmock.NewResult(0, 0))

	// Status (after)
	mock.ExpectQuery("SELECT version, applied_at, checksum FROM schema_migrations").
		WillReturnRows(sqlmock.NewRows([]string{"version", "applied_at", "checksum"}))

	step, _ := NewMigrateRollbackStep("s", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":    "rollback_mod",
		"steps":     1,
		"_executor": db,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["status"] != "rolled_back" {
		t.Errorf("expected rolled_back, got %v", result.Output["status"])
	}
	if result.Output["fromVersion"].(int) != 1 {
		t.Errorf("expected fromVersion=1, got %v", result.Output["fromVersion"])
	}
}

func TestMigrateStatus(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "001_init.up.sql"), []byte("SELECT 1;"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "002_v2.up.sql"), []byte("SELECT 2;"), 0o600); err != nil {
		t.Fatal(err)
	}

	registerTestModule(t, "status_mod", map[string]any{
		"strategy":      "scripted",
		"migrationsDir": dir,
	})

	db, mock := newMockDB(t)
	mock.ExpectQuery("SELECT version, applied_at, checksum FROM schema_migrations").
		WillReturnRows(sqlmock.NewRows([]string{"version", "applied_at", "checksum"}).
			AddRow(1, time.Now(), "abc"))

	step, _ := NewMigrateStatusStep("s", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":    "status_mod",
		"_executor": db,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["version"].(int) != 1 {
		t.Errorf("expected version=1, got %v", result.Output["version"])
	}
	if result.Output["pending"].(int) != 1 {
		t.Errorf("expected 1 pending, got %v", result.Output["pending"])
	}
}

func TestMigrateStatus_NoExecutor(t *testing.T) {
	registerTestModule(t, "status_noexec", map[string]any{"strategy": "scripted"})

	step, _ := NewMigrateStatusStep("s", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "status_noexec",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["status"] != "no_executor" {
		t.Errorf("expected no_executor, got %v", result.Output["status"])
	}
}

func TestMigrateStep_MissingModule(t *testing.T) {
	step, _ := NewMigratePlanStep("s", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "does_not_exist",
	})
	if err == nil {
		t.Error("expected error for missing module")
	}
}
