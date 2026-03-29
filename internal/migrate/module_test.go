package migrate

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestMigrateModule_Init(t *testing.T) {
	tests := []struct {
		name    string
		config  map[string]any
		wantErr bool
	}{
		{"valid declarative", map[string]any{"strategy": "declarative"}, false},
		{"valid scripted", map[string]any{"strategy": "scripted"}, false},
		{"valid both", map[string]any{"strategy": "both"}, false},
		{"missing strategy", map[string]any{}, true},
		{"unknown strategy", map[string]any{"strategy": "unknown"}, true},
		{"unknown onBreakingChange", map[string]any{"strategy": "declarative", "onBreakingChange": "explode"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := NewSchemaModule("test", tt.config)
			if err != nil {
				t.Fatalf("NewSchemaModule: %v", err)
			}
			err = m.Init()
			if (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMigrateModule_Start_Declarative(t *testing.T) {
	dir := t.TempDir()
	schemaContent := `table: users
columns:
  - name: id
    type: bigint
    primaryKey: true
`
	schemaPath := filepath.Join(dir, "users.yaml")
	if err := os.WriteFile(schemaPath, []byte(schemaContent), 0o600); err != nil {
		t.Fatal(err)
	}

	m, err := NewSchemaModule("test_declarative", map[string]any{
		"strategy": "declarative",
		"schemas":  []any{map[string]any{"path": schemaPath}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer m.Stop(context.Background())

	sm := m.(*SchemaModule)
	if len(sm.Schemas()) != 1 {
		t.Errorf("expected 1 schema, got %d", len(sm.Schemas()))
	}
	if sm.Schemas()[0].Table != "users" {
		t.Errorf("expected table=users, got %q", sm.Schemas()[0].Table)
	}

	// Verify registry.
	found, err := LookupModule("test_declarative")
	if err != nil {
		t.Errorf("module not in registry: %v", err)
	}
	if found != sm {
		t.Error("registry returned wrong module")
	}
}

func TestMigrateModule_Start_Scripted(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "001_init.up.sql"), []byte("CREATE TABLE t (id INT);"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "001_init.down.sql"), []byte("DROP TABLE t;"), 0o600); err != nil {
		t.Fatal(err)
	}

	m, err := NewSchemaModule("test_scripted", map[string]any{
		"strategy":      "scripted",
		"migrationsDir": dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer m.Stop(context.Background())

	sm := m.(*SchemaModule)
	if len(sm.Scripts()) != 1 {
		t.Errorf("expected 1 script, got %d", len(sm.Scripts()))
	}
	if sm.Scripts()[0].Version != 1 {
		t.Errorf("expected version 1, got %d", sm.Scripts()[0].Version)
	}
}

func TestMigrateModule_Start_Both(t *testing.T) {
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "tbl.yaml")
	if err := os.WriteFile(schemaPath, []byte("table: tbl\ncolumns:\n  - name: id\n    type: bigint\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	migrDir := filepath.Join(dir, "migrations")
	if err := os.MkdirAll(migrDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(migrDir, "001_init.up.sql"), []byte("SELECT 1;"), 0o600); err != nil {
		t.Fatal(err)
	}

	m, err := NewSchemaModule("test_both", map[string]any{
		"strategy":      "both",
		"schemas":       []any{map[string]any{"path": schemaPath}},
		"migrationsDir": migrDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer m.Stop(context.Background())

	sm := m.(*SchemaModule)
	if len(sm.Schemas()) != 1 {
		t.Errorf("expected 1 schema, got %d", len(sm.Schemas()))
	}
	if len(sm.Scripts()) != 1 {
		t.Errorf("expected 1 script, got %d", len(sm.Scripts()))
	}
}

func TestMigrateModule_Stop_Deregisters(t *testing.T) {
	m, err := NewSchemaModule("test_deregister", map[string]any{"strategy": "declarative"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := LookupModule("test_deregister"); err != nil {
		t.Fatal("expected module in registry after Start")
	}
	if err := m.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := LookupModule("test_deregister"); err == nil {
		t.Error("expected module removed from registry after Stop")
	}
}

func TestMigrateModule_Start_BadSchemaFile(t *testing.T) {
	m, err := NewSchemaModule("test_bad_schema", map[string]any{
		"strategy": "declarative",
		"schemas":  []any{map[string]any{"path": "/nonexistent/path.yaml"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err == nil {
		t.Error("expected error for nonexistent schema file")
	}
}
