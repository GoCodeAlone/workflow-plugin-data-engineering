package migrate

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// newMockDB returns a *sql.DB backed by go-sqlmock.
// The *sql.DB satisfies SQLExecutor (it has ExecContext and QueryContext).
func newMockDB(t *testing.T) (SQLExecutor, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("open sqlmock: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, mock
}

func TestMigrationRunner_EnsureLockTable(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS schema_migrations").
		WillReturnResult(sqlmock.NewResult(0, 0))

	r := NewMigrationRunner(db, "schema_migrations")
	if err := r.EnsureLockTable(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestMigrationRunner_EnsureLockTable_InvalidName(t *testing.T) {
	db, _ := newMockDB(t)
	r := NewMigrationRunner(db, "bad name!")
	if err := r.EnsureLockTable(context.Background()); err == nil {
		t.Error("expected error for invalid lock table name")
	}
}

func TestMigrationRunner_LoadScripts(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"001_create_users.up.sql":   "CREATE TABLE users (id BIGINT);",
		"001_create_users.down.sql": "DROP TABLE users;",
		"002_add_email.up.sql":      "ALTER TABLE users ADD COLUMN email TEXT;",
		"003_add_index.up.sql":      "CREATE INDEX idx_users_email ON users (email);",
		"003_add_index.down.sql":    "DROP INDEX idx_users_email;",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	scripts, err := LoadScripts(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(scripts) != 3 {
		t.Fatalf("expected 3 scripts, got %d", len(scripts))
	}
	if scripts[0].Version != 1 {
		t.Errorf("first script should be version 1, got %d", scripts[0].Version)
	}
	if scripts[0].DownSQL == "" {
		t.Error("version 1 should have down SQL")
	}
	if scripts[1].DownSQL != "" {
		t.Error("version 2 should have no down SQL")
	}
	for i := 1; i < len(scripts); i++ {
		if scripts[i].Version <= scripts[i-1].Version {
			t.Error("scripts not sorted by version")
		}
	}
}

func TestMigrationRunner_LoadScripts_MissingUpSQL(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "001_orphan.down.sql"), []byte("DROP TABLE x;"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadScripts(dir)
	if err == nil {
		t.Error("expected error when up SQL is missing")
	}
}

func TestMigrationRunner_LoadScripts_Empty(t *testing.T) {
	dir := t.TempDir()
	scripts, err := LoadScripts(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(scripts) != 0 {
		t.Errorf("expected 0 scripts, got %d", len(scripts))
	}
}

func TestMigrationRunner_Apply(t *testing.T) {
	db, mock := newMockDB(t)

	// advisory lock
	mock.ExpectExec("SELECT pg_advisory_lock").WillReturnResult(sqlmock.NewResult(0, 0))
	// loadApplied
	mock.ExpectQuery("SELECT version, applied_at, checksum FROM schema_migrations").
		WillReturnRows(sqlmock.NewRows([]string{"version", "applied_at", "checksum"}))
	// migration 1 up
	mock.ExpectExec("CREATE TABLE users").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO schema_migrations").
		WithArgs(1, sqlmock.AnyArg(), "create_users").
		WillReturnResult(sqlmock.NewResult(0, 1))
	// migration 2 up
	mock.ExpectExec("ALTER TABLE users ADD COLUMN email").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO schema_migrations").
		WithArgs(2, sqlmock.AnyArg(), "add_email").
		WillReturnResult(sqlmock.NewResult(0, 1))
	// advisory unlock
	mock.ExpectExec("SELECT pg_advisory_unlock").WillReturnResult(sqlmock.NewResult(0, 0))

	r := NewMigrationRunner(db, "schema_migrations")
	scripts := []MigrationScript{
		{Version: 1, Description: "create_users", UpSQL: "CREATE TABLE users (id BIGINT);"},
		{Version: 2, Description: "add_email", UpSQL: "ALTER TABLE users ADD COLUMN email TEXT;"},
	}
	if err := r.Apply(context.Background(), scripts); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestMigrationRunner_Apply_SkipsApplied(t *testing.T) {
	db, mock := newMockDB(t)

	// advisory lock
	mock.ExpectExec("SELECT pg_advisory_lock").WillReturnResult(sqlmock.NewResult(0, 0))
	// loadApplied — version 1 is already applied
	rows := sqlmock.NewRows([]string{"version", "applied_at", "checksum"}).
		AddRow(1, time.Now(), "abc123")
	mock.ExpectQuery("SELECT version, applied_at, checksum FROM schema_migrations").
		WillReturnRows(rows)
	// only migration 2 should run
	mock.ExpectExec("ALTER TABLE users ADD COLUMN email").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO schema_migrations").
		WithArgs(2, sqlmock.AnyArg(), "add_email").
		WillReturnResult(sqlmock.NewResult(0, 1))
	// advisory unlock
	mock.ExpectExec("SELECT pg_advisory_unlock").WillReturnResult(sqlmock.NewResult(0, 0))

	r := NewMigrationRunner(db, "schema_migrations")
	scripts := []MigrationScript{
		{Version: 1, Description: "create_users", UpSQL: "CREATE TABLE users (id BIGINT);"},
		{Version: 2, Description: "add_email", UpSQL: "ALTER TABLE users ADD COLUMN email TEXT;"},
	}
	if err := r.Apply(context.Background(), scripts); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("version 1 should have been skipped: %v", err)
	}
}

func TestMigrationRunner_Rollback(t *testing.T) {
	db, mock := newMockDB(t)

	// advisory lock
	mock.ExpectExec("SELECT pg_advisory_lock").WillReturnResult(sqlmock.NewResult(0, 0))
	// loadApplied — versions 1 and 2 applied
	rows := sqlmock.NewRows([]string{"version", "applied_at", "checksum"}).
		AddRow(1, time.Now(), "abc").
		AddRow(2, time.Now(), "def")
	mock.ExpectQuery("SELECT version, applied_at, checksum FROM schema_migrations").
		WillReturnRows(rows)
	// rollback version 2 (most recent first)
	mock.ExpectExec("ALTER TABLE users DROP COLUMN email").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("DELETE FROM schema_migrations WHERE version").
		WithArgs(2).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// advisory unlock
	mock.ExpectExec("SELECT pg_advisory_unlock").WillReturnResult(sqlmock.NewResult(0, 0))

	r := NewMigrationRunner(db, "schema_migrations")
	scripts := []MigrationScript{
		{Version: 1, Description: "create_users", UpSQL: "CREATE TABLE users (id BIGINT);", DownSQL: "DROP TABLE users;"},
		{Version: 2, Description: "add_email", UpSQL: "ALTER TABLE users ADD COLUMN email TEXT;", DownSQL: "ALTER TABLE users DROP COLUMN email;"},
	}
	if err := r.Rollback(context.Background(), scripts, 1); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestMigrationRunner_Rollback_NoDownSQL(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectExec("SELECT pg_advisory_lock").WillReturnResult(sqlmock.NewResult(0, 0))
	rows := sqlmock.NewRows([]string{"version", "applied_at", "checksum"}).
		AddRow(1, time.Now(), "abc")
	mock.ExpectQuery("SELECT version, applied_at, checksum FROM schema_migrations").
		WillReturnRows(rows)
	mock.ExpectExec("SELECT pg_advisory_unlock").WillReturnResult(sqlmock.NewResult(0, 0))

	r := NewMigrationRunner(db, "schema_migrations")
	scripts := []MigrationScript{
		{Version: 1, Description: "create_users", UpSQL: "CREATE TABLE users (id BIGINT);"}, // no DownSQL
	}
	err := r.Rollback(context.Background(), scripts, 1)
	if err == nil {
		t.Error("expected error when down SQL is missing")
	}
}

func TestMigrationRunner_Status(t *testing.T) {
	db, mock := newMockDB(t)

	rows := sqlmock.NewRows([]string{"version", "applied_at", "checksum"}).
		AddRow(1, time.Now(), "abc")
	mock.ExpectQuery("SELECT version, applied_at, checksum FROM schema_migrations").
		WillReturnRows(rows)

	r := NewMigrationRunner(db, "schema_migrations")
	scripts := []MigrationScript{
		{Version: 1, UpSQL: "SELECT 1;"},
		{Version: 2, UpSQL: "SELECT 2;"},
	}
	state, err := r.Status(context.Background(), scripts)
	if err != nil {
		t.Fatal(err)
	}
	if state.Version != 1 {
		t.Errorf("expected version 1, got %d", state.Version)
	}
	if len(state.Applied) != 1 {
		t.Errorf("expected 1 applied, got %d", len(state.Applied))
	}
	if len(state.Pending) != 1 || state.Pending[0].Version != 2 {
		t.Errorf("expected version 2 pending, got %v", state.Pending)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestMigrationRunner_Status_AllPending(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectQuery("SELECT version, applied_at, checksum FROM schema_migrations").
		WillReturnRows(sqlmock.NewRows([]string{"version", "applied_at", "checksum"}))

	r := NewMigrationRunner(db, "schema_migrations")
	scripts := []MigrationScript{
		{Version: 1, UpSQL: "SELECT 1;"},
		{Version: 2, UpSQL: "SELECT 2;"},
	}
	state, err := r.Status(context.Background(), scripts)
	if err != nil {
		t.Fatal(err)
	}
	if state.Version != 0 {
		t.Errorf("expected version 0, got %d", state.Version)
	}
	if len(state.Pending) != 2 {
		t.Errorf("expected 2 pending, got %d", len(state.Pending))
	}
}

func TestMigrationRunner_Checksum(t *testing.T) {
	s1 := sha256sum("SELECT 1;")
	s2 := sha256sum("SELECT 1;")
	s3 := sha256sum("SELECT 2;")
	if s1 != s2 {
		t.Error("same input should produce same checksum")
	}
	if s1 == s3 {
		t.Error("different input should produce different checksum")
	}
	if len(s1) != 64 {
		t.Errorf("expected 64-char hex SHA-256, got %d chars", len(s1))
	}
}
