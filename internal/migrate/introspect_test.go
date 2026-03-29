package migrate

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestIntrospectSchema_TableExists(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(1),
	)
	defPtr := sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}).
		AddRow("id", "bigint", "NO", nil).
		AddRow("name", "character varying", "YES", nil).
		AddRow("created_at", "timestamp with time zone", "NO", "now()")
	mock.ExpectQuery("SELECT.*column_name").WillReturnRows(defPtr)
	mock.ExpectQuery("SELECT indexname").WillReturnRows(
		sqlmock.NewRows([]string{"indexname", "indexdef"}).
			AddRow("idx_users_name", "CREATE INDEX idx_users_name ON users USING btree (name)"),
	)

	def, err := IntrospectSchema(context.Background(), db, "users")
	if err != nil {
		t.Fatal(err)
	}
	if def == nil {
		t.Fatal("expected non-nil definition")
	}
	if def.Table != "users" {
		t.Errorf("expected table=users, got %q", def.Table)
	}
	if len(def.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(def.Columns))
	}
	if def.Columns[0].Name != "id" {
		t.Error("expected first column to be id")
	}
	if def.Columns[0].Type != "bigint" {
		t.Errorf("expected bigint, got %q", def.Columns[0].Type)
	}
	if def.Columns[1].Type != "varchar" {
		t.Errorf("expected varchar, got %q", def.Columns[1].Type)
	}
	if def.Columns[2].Default != "now()" {
		t.Errorf("expected default=now(), got %q", def.Columns[2].Default)
	}
	if len(def.Indexes) != 1 {
		t.Errorf("expected 1 index, got %d", len(def.Indexes))
	}
	if def.Indexes[0].Name != "idx_users_name" {
		t.Errorf("expected index name idx_users_name, got %q", def.Indexes[0].Name)
	}
	if len(def.Indexes[0].Columns) != 1 || def.Indexes[0].Columns[0] != "name" {
		t.Errorf("expected index column=name, got %v", def.Indexes[0].Columns)
	}
}

func TestIntrospectSchema_TableNotExists(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(0),
	)

	def, err := IntrospectSchema(context.Background(), db, "nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if def != nil {
		t.Error("expected nil for non-existent table")
	}
}

func TestIntrospectSchema_InvalidIdentifier(t *testing.T) {
	db, _ := newMockDB(t)
	_, err := IntrospectSchema(context.Background(), db, "bad-name!")
	if err == nil {
		t.Error("expected error for invalid table identifier")
	}
}

func TestIntrospectSchema_WithUniqueIndex(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(1),
	)
	mock.ExpectQuery("SELECT.*column_name").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}).
			AddRow("email", "character varying", "NO", nil),
	)
	mock.ExpectQuery("SELECT indexname").WillReturnRows(
		sqlmock.NewRows([]string{"indexname", "indexdef"}).
			AddRow("idx_users_email", "CREATE UNIQUE INDEX idx_users_email ON users USING btree (email)"),
	)

	def, err := IntrospectSchema(context.Background(), db, "users")
	if err != nil {
		t.Fatal(err)
	}
	if len(def.Indexes) != 1 || !def.Indexes[0].Unique {
		t.Error("expected unique index")
	}
}

func TestIntrospectSchema_NullableColumn(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(1),
	)
	mock.ExpectQuery("SELECT.*column_name").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}).
			AddRow("bio", "text", "YES", nil),
	)
	mock.ExpectQuery("SELECT indexname").WillReturnRows(
		sqlmock.NewRows([]string{"indexname", "indexdef"}),
	)

	def, err := IntrospectSchema(context.Background(), db, "profiles")
	if err != nil {
		t.Fatal(err)
	}
	if !def.Columns[0].Nullable {
		t.Error("expected column to be nullable")
	}
}
