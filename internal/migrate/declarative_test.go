package migrate

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiffSchema_AddTable(t *testing.T) {
	desired := SchemaDefinition{
		Table: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: "bigint", PrimaryKey: true},
			{Name: "email", Type: "varchar(255)", Nullable: false},
		},
	}
	plan, err := DiffSchema(desired, SchemaDefinition{})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Changes) == 0 {
		t.Fatal("expected changes")
	}
	if plan.Changes[0].Type != "add_table" {
		t.Errorf("expected add_table, got %s", plan.Changes[0].Type)
	}
	if plan.Changes[0].Breaking {
		t.Error("add_table should not be breaking")
	}
	if !plan.Safe {
		t.Error("plan with only add_table should be safe")
	}
}

func TestDiffSchema_AddColumn(t *testing.T) {
	live := SchemaDefinition{
		Table:   "orders",
		Columns: []ColumnDef{{Name: "id", Type: "bigint"}},
	}
	desired := SchemaDefinition{
		Table: "orders",
		Columns: []ColumnDef{
			{Name: "id", Type: "bigint"},
			{Name: "status", Type: "text"},
		},
	}
	plan, err := DiffSchema(desired, live)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Changes) != 1 || plan.Changes[0].Type != "add_column" {
		t.Fatalf("expected 1 add_column change, got %v", plan.Changes)
	}
	if plan.Changes[0].Breaking {
		t.Error("add_column should not be breaking")
	}
	if !plan.Safe {
		t.Error("plan should be safe")
	}
}

func TestDiffSchema_AddIndex(t *testing.T) {
	live := SchemaDefinition{
		Table:   "events",
		Columns: []ColumnDef{{Name: "id", Type: "bigint"}, {Name: "ts", Type: "timestamptz"}},
	}
	desired := SchemaDefinition{
		Table:   "events",
		Columns: []ColumnDef{{Name: "id", Type: "bigint"}, {Name: "ts", Type: "timestamptz"}},
		Indexes: []IndexDef{{Columns: []string{"ts"}}},
	}
	plan, err := DiffSchema(desired, live)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Changes) != 1 || plan.Changes[0].Type != "add_index" {
		t.Fatalf("expected 1 add_index, got %v", plan.Changes)
	}
	if !plan.Safe {
		t.Error("plan should be safe")
	}
}

func TestDiffSchema_WidenType(t *testing.T) {
	live := SchemaDefinition{
		Table:   "products",
		Columns: []ColumnDef{{Name: "name", Type: "varchar(100)"}},
	}
	desired := SchemaDefinition{
		Table:   "products",
		Columns: []ColumnDef{{Name: "name", Type: "varchar(255)"}},
	}
	plan, err := DiffSchema(desired, live)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Changes) != 1 || plan.Changes[0].Type != "widen_type" {
		t.Fatalf("expected 1 widen_type, got %v", plan.Changes)
	}
	if plan.Changes[0].Breaking {
		t.Error("widen_type should not be breaking")
	}
	if !plan.Safe {
		t.Error("plan should be safe")
	}
}

func TestDiffSchema_DropColumn_Breaking(t *testing.T) {
	live := SchemaDefinition{
		Table:   "users",
		Columns: []ColumnDef{{Name: "id", Type: "bigint"}, {Name: "legacy", Type: "text"}},
	}
	desired := SchemaDefinition{
		Table:   "users",
		Columns: []ColumnDef{{Name: "id", Type: "bigint"}},
	}
	plan, err := DiffSchema(desired, live)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Changes) != 1 || plan.Changes[0].Type != "drop_column" {
		t.Fatalf("expected 1 drop_column, got %v", plan.Changes)
	}
	if !plan.Changes[0].Breaking {
		t.Error("drop_column should be breaking")
	}
	if plan.Safe {
		t.Error("plan should not be safe")
	}
}

func TestDiffSchema_NarrowType_Breaking(t *testing.T) {
	live := SchemaDefinition{
		Table:   "logs",
		Columns: []ColumnDef{{Name: "msg", Type: "varchar(255)"}},
	}
	desired := SchemaDefinition{
		Table:   "logs",
		Columns: []ColumnDef{{Name: "msg", Type: "varchar(50)"}},
	}
	plan, err := DiffSchema(desired, live)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Changes) != 1 || plan.Changes[0].Type != "narrow_type" {
		t.Fatalf("expected 1 narrow_type, got %v", plan.Changes)
	}
	if !plan.Changes[0].Breaking {
		t.Error("narrow_type should be breaking")
	}
	if plan.Safe {
		t.Error("plan should not be safe")
	}
}

func TestDiffSchema_NoChanges(t *testing.T) {
	schema := SchemaDefinition{
		Table:   "items",
		Columns: []ColumnDef{{Name: "id", Type: "bigint"}, {Name: "name", Type: "text"}},
	}
	plan, err := DiffSchema(schema, schema)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Changes) != 0 {
		t.Errorf("expected no changes, got %v", plan.Changes)
	}
	if !plan.Safe {
		t.Error("empty plan should be safe")
	}
}

func TestParseSchemaFile(t *testing.T) {
	dir := t.TempDir()
	content := `
table: shipments
columns:
  - name: id
    type: bigint
    primaryKey: true
  - name: status
    type: text
    nullable: false
indexes:
  - columns: [status]
`
	path := filepath.Join(dir, "shipments.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	def, err := ParseSchemaFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if def.Table != "shipments" {
		t.Errorf("expected table=shipments, got %q", def.Table)
	}
	if len(def.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(def.Columns))
	}
	if len(def.Indexes) != 1 {
		t.Errorf("expected 1 index, got %d", len(def.Indexes))
	}
}

func TestParseSchemaDir(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"users.yaml": "table: users\ncolumns:\n  - name: id\n    type: bigint\n",
		"posts.yaml": "table: posts\ncolumns:\n  - name: id\n    type: bigint\n",
		"README.md":  "not a schema",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	defs, err := ParseSchemaDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 2 {
		t.Errorf("expected 2 schema definitions, got %d", len(defs))
	}
}

func TestDiffSchema_InvalidIdentifier(t *testing.T) {
	_, err := DiffSchema(SchemaDefinition{Table: "drop table--"}, SchemaDefinition{})
	if err == nil {
		t.Error("expected error for invalid table identifier")
	}
}

func TestDiffSchema_NumericWiden(t *testing.T) {
	live := SchemaDefinition{
		Table:   "counters",
		Columns: []ColumnDef{{Name: "val", Type: "int"}},
	}
	desired := SchemaDefinition{
		Table:   "counters",
		Columns: []ColumnDef{{Name: "val", Type: "bigint"}},
	}
	plan, err := DiffSchema(desired, live)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Changes) != 1 || plan.Changes[0].Type != "widen_type" {
		t.Fatalf("expected 1 widen_type, got %v", plan.Changes)
	}
	if !plan.Safe {
		t.Error("int→bigint should be safe")
	}
}
