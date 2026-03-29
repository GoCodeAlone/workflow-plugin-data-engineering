package quality

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

const sampleContract = `
dataset: raw.users
owner: data-team
schema:
  columns:
    - name: id
      type: bigint
      nullable: false
    - name: email
      type: varchar
      nullable: false
      pattern: "^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$"
quality:
  - type: row_count
    config:
      min: 1
`

func writeContractFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "contract.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write contract file: %v", err)
	}
	return path
}

// ── ParseContract ─────────────────────────────────────────────────────────────

func TestParseContract(t *testing.T) {
	path := writeContractFile(t, sampleContract)
	c, err := ParseContract(path)
	if err != nil {
		t.Fatalf("ParseContract: %v", err)
	}
	if c.Dataset != "raw.users" {
		t.Errorf("Dataset: got %q, want raw.users", c.Dataset)
	}
	if c.Owner != "data-team" {
		t.Errorf("Owner: got %q, want data-team", c.Owner)
	}
	if len(c.Schema.Columns) != 2 {
		t.Errorf("Schema columns: got %d, want 2", len(c.Schema.Columns))
	}
	if c.Schema.Columns[1].Pattern == "" {
		t.Error("email column pattern should be set")
	}
	if len(c.Quality) != 1 {
		t.Errorf("Quality checks: got %d, want 1", len(c.Quality))
	}
}

func TestParseContract_MissingFile(t *testing.T) {
	_, err := ParseContract("/nonexistent/path/contract.yaml")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestParseContract_InvalidYAML(t *testing.T) {
	path := writeContractFile(t, "{ invalid: yaml: [}")
	_, err := ParseContract(path)
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestParseContract_MissingDataset(t *testing.T) {
	path := writeContractFile(t, "owner: someone\n")
	_, err := ParseContract(path)
	if err == nil {
		t.Error("expected error for missing dataset field")
	}
}

// ── ValidateContract_Pass ─────────────────────────────────────────────────────

func TestValidateContract_Pass(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Schema validation: information_schema.columns
	mock.ExpectQuery("SELECT column_name, data_type, is_nullable FROM information_schema.columns").
		WithArgs("raw", "users").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable"}).
			AddRow("id", "bigint", "NO").
			AddRow("email", "character varying", "NO"))

	// Quality check: row_count
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM raw.users").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(100))

	// Pattern check: email column
	mock.ExpectQuery("SELECT email FROM raw.users WHERE email IS NOT NULL").
		WillReturnRows(sqlmock.NewRows([]string{"email"}).
			AddRow("alice@example.com").
			AddRow("bob@example.com"))

	var q DBQuerier = db
	contract := DataContract{
		Dataset: "raw.users",
		Owner:   "data-team",
		Schema: ContractSchema{
			Columns: []ContractColumn{
				{Name: "id", Type: "bigint", Nullable: false},
				{Name: "email", Type: "varchar", Nullable: false, Pattern: `^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$`},
			},
		},
		Quality: []QualityCheck{
			{Type: "row_count", Config: map[string]any{"min": int64(1)}},
		},
	}

	result, err := ValidateContract(context.Background(), q, contract)
	if err != nil {
		t.Fatalf("ValidateContract: %v", err)
	}
	if !result.Passed {
		t.Errorf("expected pass; schemaOK=%v qualityOK=%v schemaErrors=%v",
			result.SchemaOK, result.QualityOK, result.SchemaErrors)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

// ── ValidateContract_SchemaFail ───────────────────────────────────────────────

func TestValidateContract_SchemaFail(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// information_schema returns only 1 column; contract expects 2.
	mock.ExpectQuery("SELECT column_name, data_type, is_nullable FROM information_schema.columns").
		WithArgs("raw", "users").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable"}).
			AddRow("id", "bigint", "NO"))
	// No quality checks in this test.

	var q DBQuerier = db
	contract := DataContract{
		Dataset: "raw.users",
		Schema: ContractSchema{
			Columns: []ContractColumn{
				{Name: "id", Type: "bigint", Nullable: false},
				{Name: "email", Type: "varchar", Nullable: false},
			},
		},
	}

	result, err := ValidateContract(context.Background(), q, contract)
	if err != nil {
		t.Fatalf("ValidateContract: %v", err)
	}
	if result.SchemaOK {
		t.Error("expected schema validation to fail (missing email column)")
	}
	if result.Passed {
		t.Error("expected overall result to fail")
	}
	if len(result.SchemaErrors) == 0 {
		t.Error("expected schema errors to be non-empty")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

// ── ValidateContract_QualityFail ──────────────────────────────────────────────

func TestValidateContract_QualityFail(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Schema check passes (no schema columns defined).
	// Quality check: row_count fails (0 rows < min 100).
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM users").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	var q DBQuerier = db
	contract := DataContract{
		Dataset: "users",
		Quality: []QualityCheck{
			{Type: "row_count", Config: map[string]any{"min": int64(100)}},
		},
	}

	result, err := ValidateContract(context.Background(), q, contract)
	if err != nil {
		t.Fatalf("ValidateContract: %v", err)
	}
	if result.QualityOK {
		t.Error("expected quality validation to fail")
	}
	if result.Passed {
		t.Error("expected overall result to fail")
	}
	if len(result.QualityResults) == 0 {
		t.Error("expected quality results to be non-empty")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

// ── ValidateContract_PatternCheck ─────────────────────────────────────────────

func TestValidateContract_PatternCheck(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Schema validation.
	mock.ExpectQuery("SELECT column_name, data_type, is_nullable FROM information_schema.columns").
		WithArgs("public", "users").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable"}).
			AddRow("email", "character varying", "NO"))

	// Pattern check: one invalid email (uses fully-qualified "public.users").
	mock.ExpectQuery("SELECT email FROM public.users WHERE email IS NOT NULL").
		WillReturnRows(sqlmock.NewRows([]string{"email"}).
			AddRow("valid@example.com").
			AddRow("not-an-email"))

	var q DBQuerier = db
	contract := DataContract{
		Dataset: "public.users",
		Schema: ContractSchema{
			Columns: []ContractColumn{
				{Name: "email", Type: "varchar", Pattern: `^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$`},
			},
		},
	}

	result, err := ValidateContract(context.Background(), q, contract)
	if err != nil {
		t.Fatalf("ValidateContract: %v", err)
	}
	if result.QualityOK {
		t.Error("expected quality to fail due to pattern violation")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

// ── splitDataset ──────────────────────────────────────────────────────────────

func TestSplitDataset(t *testing.T) {
	tests := []struct {
		input          string
		wantSchema     string
		wantTable      string
	}{
		{"raw.users", "raw", "users"},
		{"users", "", "users"},
		{"public.orders", "public", "orders"},
	}
	for _, tc := range tests {
		s, table := splitDataset(tc.input)
		if s != tc.wantSchema || table != tc.wantTable {
			t.Errorf("splitDataset(%q): got (%q,%q), want (%q,%q)",
				tc.input, s, table, tc.wantSchema, tc.wantTable)
		}
	}
}
