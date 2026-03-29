package quality

import (
	"context"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// newMock is a helper that creates a *sql.DB backed by sqlmock.
func newMock(t *testing.T) (*sqlmock.Sqlmock, *DBQuerier) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled sqlmock expectations: %v", err)
		}
		db.Close()
	})
	var q DBQuerier = db
	return &mock, &q
}

// ── not_null ──────────────────────────────────────────────────────────────────

func TestNotNull_Pass(t *testing.T) {
	mock, q := newMock(t)
	(*mock).ExpectQuery("SELECT COUNT\\(\\*\\) FROM users WHERE id IS NULL").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	(*mock).ExpectQuery("SELECT COUNT\\(\\*\\) FROM users WHERE email IS NULL").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	res, err := RunCheck(context.Background(), *q, "not_null", "users", map[string]any{
		"columns": []any{"id", "email"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Passed {
		t.Errorf("expected pass, got: %s", res.Message)
	}
}

func TestNotNull_Fail(t *testing.T) {
	mock, q := newMock(t)
	(*mock).ExpectQuery("SELECT COUNT\\(\\*\\) FROM orders WHERE user_id IS NULL").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))

	res, err := RunCheck(context.Background(), *q, "not_null", "orders", map[string]any{
		"columns": []any{"user_id"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Passed {
		t.Error("expected fail")
	}
	if res.Value.(int64) != 5 {
		t.Errorf("expected value=5, got %v", res.Value)
	}
}

func TestNotNull_InvalidTable(t *testing.T) {
	_, q := newMock(t)
	_, err := RunCheck(context.Background(), *q, "not_null", "bad; DROP TABLE", map[string]any{
		"columns": []any{"id"},
	})
	if err == nil {
		t.Error("expected error for invalid table name")
	}
}

func TestNotNull_InvalidColumn(t *testing.T) {
	_, q := newMock(t)
	_, err := RunCheck(context.Background(), *q, "not_null", "users", map[string]any{
		"columns": []any{"bad--col"},
	})
	if err == nil {
		t.Error("expected error for invalid column name")
	}
}

func TestNotNull_MissingColumns(t *testing.T) {
	_, q := newMock(t)
	_, err := RunCheck(context.Background(), *q, "not_null", "users", map[string]any{})
	if err == nil {
		t.Error("expected error when columns missing")
	}
}

// ── unique ────────────────────────────────────────────────────────────────────

func TestUnique_Pass(t *testing.T) {
	mock, q := newMock(t)
	(*mock).ExpectQuery("SELECT COUNT\\(\\*\\) FROM").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	res, err := RunCheck(context.Background(), *q, "unique", "users", map[string]any{
		"columns": []any{"id"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Passed {
		t.Errorf("expected pass, got: %s", res.Message)
	}
}

func TestUnique_Fail(t *testing.T) {
	mock, q := newMock(t)
	(*mock).ExpectQuery("SELECT COUNT\\(\\*\\) FROM").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	res, err := RunCheck(context.Background(), *q, "unique", "users", map[string]any{
		"columns": []any{"email"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Passed {
		t.Error("expected fail")
	}
	if res.Value.(int64) != 3 {
		t.Errorf("expected value=3, got %v", res.Value)
	}
}

// ── freshness ─────────────────────────────────────────────────────────────────

func TestFreshness_Pass(t *testing.T) {
	mock, q := newMock(t)
	recent := time.Now().Add(-30 * time.Second)
	(*mock).ExpectQuery("SELECT MAX\\(updated_at\\) FROM events").
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(recent))

	res, err := RunCheck(context.Background(), *q, "freshness", "events", map[string]any{
		"column": "updated_at",
		"maxAge": "1h",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Passed {
		t.Errorf("expected pass, got: %s", res.Message)
	}
}

func TestFreshness_Fail(t *testing.T) {
	mock, q := newMock(t)
	old := time.Now().Add(-2 * time.Hour)
	(*mock).ExpectQuery("SELECT MAX\\(updated_at\\) FROM events").
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(old))

	res, err := RunCheck(context.Background(), *q, "freshness", "events", map[string]any{
		"column": "updated_at",
		"maxAge": "1h",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Passed {
		t.Error("expected fail: data is stale")
	}
}

func TestFreshness_EmptyTable(t *testing.T) {
	mock, q := newMock(t)
	(*mock).ExpectQuery("SELECT MAX\\(created_at\\) FROM logs").
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(nil))

	res, err := RunCheck(context.Background(), *q, "freshness", "logs", map[string]any{
		"column": "created_at",
		"maxAge": "1h",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Passed {
		t.Error("expected fail for empty table")
	}
}

func TestFreshness_MissingConfig(t *testing.T) {
	_, q := newMock(t)
	tests := []map[string]any{
		{},
		{"column": "ts"},
		{"maxAge": "1h"},
		{"column": "ts", "maxAge": "notaduration"},
	}
	for _, cfg := range tests {
		if _, err := RunCheck(context.Background(), *q, "freshness", "t", cfg); err == nil {
			t.Errorf("expected error for config %v", cfg)
		}
	}
}

// ── row_count ─────────────────────────────────────────────────────────────────

func TestRowCount_Pass(t *testing.T) {
	tests := []struct {
		count int64
		min   int64
		max   int64
	}{
		{5000, 1000, 10000},
		{500, -1, -1}, // no bounds
		{1000, 1000, -1},
		{10000, -1, 10000},
	}
	for _, tc := range tests {
		mock, q := newMock(t)
		(*mock).ExpectQuery("SELECT COUNT\\(\\*\\) FROM orders").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(tc.count))

		cfg := map[string]any{}
		if tc.min >= 0 {
			cfg["min"] = tc.min
		}
		if tc.max >= 0 {
			cfg["max"] = tc.max
		}
		res, err := RunCheck(context.Background(), *q, "row_count", "orders", cfg)
		if err != nil {
			t.Fatalf("count=%d: unexpected error: %v", tc.count, err)
		}
		if !res.Passed {
			t.Errorf("count=%d: expected pass, got: %s", tc.count, res.Message)
		}
	}
}

func TestRowCount_Fail(t *testing.T) {
	tests := []struct {
		count int64
		cfg   map[string]any
	}{
		{50, map[string]any{"min": int64(100)}},
		{999999, map[string]any{"max": int64(100000)}},
	}
	for _, tc := range tests {
		mock, q := newMock(t)
		(*mock).ExpectQuery("SELECT COUNT\\(\\*\\) FROM orders").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(tc.count))

		res, err := RunCheck(context.Background(), *q, "row_count", "orders", tc.cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Passed {
			t.Errorf("count=%d cfg=%v: expected fail", tc.count, tc.cfg)
		}
	}
}

// ── referential ───────────────────────────────────────────────────────────────

func TestReferential_Pass(t *testing.T) {
	mock, q := newMock(t)
	(*mock).ExpectQuery("SELECT COUNT\\(\\*\\) FROM orders").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	res, err := RunCheck(context.Background(), *q, "referential", "orders", map[string]any{
		"column":    "user_id",
		"refTable":  "users",
		"refColumn": "id",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Passed {
		t.Errorf("expected pass, got: %s", res.Message)
	}
}

func TestReferential_Fail(t *testing.T) {
	mock, q := newMock(t)
	(*mock).ExpectQuery("SELECT COUNT\\(\\*\\) FROM orders").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(7))

	res, err := RunCheck(context.Background(), *q, "referential", "orders", map[string]any{
		"column":    "user_id",
		"refTable":  "users",
		"refColumn": "id",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Passed {
		t.Error("expected fail: orphan rows found")
	}
	if res.Value.(int64) != 7 {
		t.Errorf("expected value=7, got %v", res.Value)
	}
}

func TestReferential_MissingConfig(t *testing.T) {
	_, q := newMock(t)
	tests := []map[string]any{
		{"refTable": "users", "refColumn": "id"},
		{"column": "user_id", "refColumn": "id"},
		{"column": "user_id", "refTable": "users"},
	}
	for _, cfg := range tests {
		if _, err := RunCheck(context.Background(), *q, "referential", "orders", cfg); err == nil {
			t.Errorf("expected error for config %v", cfg)
		}
	}
}

// ── custom_sql ────────────────────────────────────────────────────────────────

func TestCustomSQL_Pass(t *testing.T) {
	mock, q := newMock(t)
	(*mock).ExpectQuery("SELECT COUNT\\(\\*\\) FROM orders WHERE total < 0").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	res, err := RunCheck(context.Background(), *q, "custom_sql", "", map[string]any{
		"query":     "SELECT COUNT(*) FROM orders WHERE total < 0",
		"threshold": int64(0),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Passed {
		t.Errorf("expected pass, got: %s", res.Message)
	}
}

func TestCustomSQL_Fail(t *testing.T) {
	mock, q := newMock(t)
	(*mock).ExpectQuery("SELECT COUNT\\(\\*\\) FROM orders WHERE total < 0").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	res, err := RunCheck(context.Background(), *q, "custom_sql", "", map[string]any{
		"query":     "SELECT COUNT(*) FROM orders WHERE total < 0",
		"threshold": int64(0),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Passed {
		t.Error("expected fail: result exceeds threshold")
	}
}

func TestCustomSQL_MissingQuery(t *testing.T) {
	_, q := newMock(t)
	_, err := RunCheck(context.Background(), *q, "custom_sql", "", map[string]any{})
	if err == nil {
		t.Error("expected error for missing query")
	}
}

// ── unknown type ──────────────────────────────────────────────────────────────

func TestRunCheck_UnknownType(t *testing.T) {
	_, q := newMock(t)
	_, err := RunCheck(context.Background(), *q, "nonexistent_check", "t", nil)
	if err == nil {
		t.Error("expected error for unknown check type")
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func TestStringSliceVal(t *testing.T) {
	m := map[string]any{
		"a": []string{"x", "y"},
		"b": []any{"p", "q"},
		"c": "not-a-slice",
	}
	if got := stringSliceVal(m, "a"); len(got) != 2 || got[0] != "x" {
		t.Errorf("a: %v", got)
	}
	if got := stringSliceVal(m, "b"); len(got) != 2 || got[0] != "p" {
		t.Errorf("b: %v", got)
	}
	if got := stringSliceVal(m, "c"); len(got) != 0 {
		t.Errorf("c: expected nil/empty, got %v", got)
	}
	if got := stringSliceVal(m, "missing"); got != nil {
		t.Errorf("missing: expected nil, got %v", got)
	}
}

func TestInt64Val(t *testing.T) {
	m := map[string]any{"a": int(5), "b": float64(3.7), "c": int64(99)}
	if got := int64Val(m, "a", 0); got != 5 {
		t.Errorf("a: got %d", got)
	}
	if got := int64Val(m, "b", 0); got != 3 {
		t.Errorf("b: got %d", got)
	}
	if got := int64Val(m, "c", 0); got != 99 {
		t.Errorf("c: got %d", got)
	}
	if got := int64Val(m, "missing", 42); got != 42 {
		t.Errorf("missing: got %d", got)
	}
}

func TestValidateIdent(t *testing.T) {
	valid := []string{"users", "raw.users", "my_table", "T1", "_col"}
	for _, id := range valid {
		if err := validateIdent(id); err != nil {
			t.Errorf("validateIdent(%q): unexpected error: %v", id, err)
		}
	}
	invalid := []string{"", "bad-col", "foo bar", "1bad", "'; DROP TABLE users"}
	for _, id := range invalid {
		if err := validateIdent(id); err == nil {
			t.Errorf("validateIdent(%q): expected error", id)
		}
	}
}

// Prevent "declared and not used" error for fmt import.
var _ = fmt.Sprintf
