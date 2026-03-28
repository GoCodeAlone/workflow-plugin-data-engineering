package tenancy

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// testExec captures SQL calls and args for verification.
type testExec struct {
	queries []string
	execArgs [][]any      // args per call
	failAt  map[int]bool // call index (1-based) → should fail
	calls   int
}

func newTestExec(failAt ...int) *testExec {
	e := &testExec{failAt: make(map[int]bool)}
	for _, n := range failAt {
		e.failAt[n] = true
	}
	return e
}

func (e *testExec) fn(ctx context.Context, sql string, args ...any) error {
	e.calls++
	e.queries = append(e.queries, sql)
	e.execArgs = append(e.execArgs, args)
	if e.failAt[e.calls] {
		return fmt.Errorf("simulated error on call %d: %s", e.calls, sql)
	}
	return nil
}

// ─── SchemaPerTenant ─────────────────────────────────────────────────────────

func TestSchemaPerTenant_ResolveTable(t *testing.T) {
	tests := []struct {
		prefix   string
		tenantID string
		table    string
		want     string
	}{
		{"t_", "acme", "users", "t_acme.users"},
		{"tenant_", "corp", "orders", "tenant_corp.orders"},
		{"", "x", "items", "x.items"},
	}
	for _, tc := range tests {
		s := NewSchemaPerTenant(tc.prefix, noopSQLExecutor)
		got := s.ResolveTable(tc.tenantID, tc.table)
		if got != tc.want {
			t.Errorf("prefix=%q tenant=%q table=%q: got %q want %q",
				tc.prefix, tc.tenantID, tc.table, got, tc.want)
		}
	}
}

func TestSchemaPerTenant_ResolveConnection(t *testing.T) {
	s := NewSchemaPerTenant("t_", noopSQLExecutor)
	conn := "postgres://localhost/mydb"
	if got := s.ResolveConnection("acme", conn); got != conn {
		t.Errorf("expected connection unchanged, got %q", got)
	}
}

func TestSchemaPerTenant_TenantFilter(t *testing.T) {
	s := NewSchemaPerTenant("t_", noopSQLExecutor)
	col, val := s.TenantFilter("acme")
	if col != "" || val != "" {
		t.Errorf("expected empty filter, got col=%q val=%q", col, val)
	}
}

func TestSchemaPerTenant_ProvisionTenant(t *testing.T) {
	exec := newTestExec()
	s := NewSchemaPerTenant("t_", exec.fn)
	if err := s.ProvisionTenant(context.Background(), "acme"); err != nil {
		t.Fatal(err)
	}
	if len(exec.queries) != 1 {
		t.Fatalf("expected 1 SQL call, got %d", len(exec.queries))
	}
	want := "CREATE SCHEMA IF NOT EXISTS t_acme"
	if exec.queries[0] != want {
		t.Errorf("SQL = %q, want %q", exec.queries[0], want)
	}
}

func TestSchemaPerTenant_DeprovisionTenant(t *testing.T) {
	tests := []struct {
		mode    string
		wantSQL string
		wantErr bool
	}{
		{"archive", "ALTER SCHEMA t_acme RENAME TO t_acme_archived", false},
		{"delete", "DROP SCHEMA t_acme CASCADE", false},
		{"unknown", "", true},
	}
	for _, tc := range tests {
		exec := newTestExec()
		s := NewSchemaPerTenant("t_", exec.fn)
		err := s.DeprovisionTenant(context.Background(), "acme", tc.mode)
		if tc.wantErr {
			if err == nil {
				t.Errorf("mode=%q: expected error, got nil", tc.mode)
			}
			continue
		}
		if err != nil {
			t.Errorf("mode=%q: unexpected error: %v", tc.mode, err)
			continue
		}
		if len(exec.queries) != 1 || exec.queries[0] != tc.wantSQL {
			t.Errorf("mode=%q: SQL = %q, want %q", tc.mode, exec.queries, tc.wantSQL)
		}
	}
}

// ─── DBPerTenant ─────────────────────────────────────────────────────────────

func TestDBPerTenant_ResolveTable(t *testing.T) {
	d := NewDBPerTenant(noopSQLExecutor)
	if got := d.ResolveTable("acme", "users"); got != "users" {
		t.Errorf("expected table unchanged, got %q", got)
	}
}

func TestDBPerTenant_ResolveConnection(t *testing.T) {
	tests := []struct {
		template string
		tenantID string
		want     string
	}{
		{"postgres://localhost/tenant_{{tenant}}", "acme", "postgres://localhost/tenant_acme"},
		{"host={{tenant}}.db.internal", "corp", "host=corp.db.internal"},
		{"no-placeholder", "x", "no-placeholder"},
	}
	d := NewDBPerTenant(noopSQLExecutor)
	for _, tc := range tests {
		got := d.ResolveConnection(tc.tenantID, tc.template)
		if got != tc.want {
			t.Errorf("template=%q tenant=%q: got %q want %q", tc.template, tc.tenantID, got, tc.want)
		}
	}
}

func TestDBPerTenant_TenantFilter(t *testing.T) {
	d := NewDBPerTenant(noopSQLExecutor)
	col, val := d.TenantFilter("acme")
	if col != "" || val != "" {
		t.Errorf("expected empty filter, got col=%q val=%q", col, val)
	}
}

func TestDBPerTenant_ProvisionTenant(t *testing.T) {
	exec := newTestExec()
	d := NewDBPerTenant(exec.fn)
	if err := d.ProvisionTenant(context.Background(), "acme"); err != nil {
		t.Fatal(err)
	}
	want := "CREATE DATABASE acme"
	if len(exec.queries) != 1 || exec.queries[0] != want {
		t.Errorf("SQL = %v, want [%q]", exec.queries, want)
	}
}

func TestDBPerTenant_DeprovisionTenant(t *testing.T) {
	tests := []struct {
		mode    string
		wantSQL string
		wantErr bool
	}{
		{"archive", "ALTER DATABASE acme RENAME TO acme_archived", false},
		{"delete", "DROP DATABASE acme", false},
		{"bad", "", true},
	}
	for _, tc := range tests {
		exec := newTestExec()
		d := NewDBPerTenant(exec.fn)
		err := d.DeprovisionTenant(context.Background(), "acme", tc.mode)
		if tc.wantErr {
			if err == nil {
				t.Errorf("mode=%q: expected error, got nil", tc.mode)
			}
			continue
		}
		if err != nil {
			t.Errorf("mode=%q: unexpected error: %v", tc.mode, err)
			continue
		}
		if len(exec.queries) != 1 || exec.queries[0] != tc.wantSQL {
			t.Errorf("mode=%q: SQL = %v, want [%q]", tc.mode, exec.queries, tc.wantSQL)
		}
	}
}

// ─── RowLevel ────────────────────────────────────────────────────────────────

func TestRowLevel_ResolveTable(t *testing.T) {
	r := NewRowLevel("org_id", []string{"users"}, noopSQLExecutor)
	if got := r.ResolveTable("acme", "users"); got != "users" {
		t.Errorf("expected table unchanged, got %q", got)
	}
}

func TestRowLevel_ResolveConnection(t *testing.T) {
	r := NewRowLevel("org_id", nil, noopSQLExecutor)
	conn := "postgres://localhost/shared"
	if got := r.ResolveConnection("acme", conn); got != conn {
		t.Errorf("expected connection unchanged, got %q", got)
	}
}

func TestRowLevel_TenantFilter(t *testing.T) {
	tests := []struct {
		col      string
		tenantID string
	}{
		{"org_id", "acme"},
		{"tenant_id", "corp"},
	}
	for _, tc := range tests {
		r := NewRowLevel(tc.col, nil, noopSQLExecutor)
		col, val := r.TenantFilter(tc.tenantID)
		if col != tc.col || val != tc.tenantID {
			t.Errorf("TenantFilter(%q): got (%q,%q) want (%q,%q)",
				tc.tenantID, col, val, tc.col, tc.tenantID)
		}
	}
}

func TestRowLevel_ProvisionTenant_IsNoop(t *testing.T) {
	exec := newTestExec()
	r := NewRowLevel("org_id", []string{"users"}, exec.fn)
	if err := r.ProvisionTenant(context.Background(), "acme"); err != nil {
		t.Fatal(err)
	}
	if exec.calls != 0 {
		t.Errorf("expected no SQL for provision, got %d calls", exec.calls)
	}
}

func TestRowLevel_DeprovisionTenant_Delete(t *testing.T) {
	exec := newTestExec()
	tables := []string{"users", "orders", "items"}
	r := NewRowLevel("org_id", tables, exec.fn)
	if err := r.DeprovisionTenant(context.Background(), "acme", "delete"); err != nil {
		t.Fatal(err)
	}
	if len(exec.queries) != 3 {
		t.Fatalf("expected 3 DELETE SQL calls, got %d: %v", len(exec.queries), exec.queries)
	}
	// tenantID must be a parameterized argument, not interpolated into the SQL string.
	wantSQL := []string{
		"DELETE FROM users WHERE org_id = $1",
		"DELETE FROM orders WHERE org_id = $1",
		"DELETE FROM items WHERE org_id = $1",
	}
	for i, want := range wantSQL {
		if exec.queries[i] != want {
			t.Errorf("query[%d] = %q, want %q", i, exec.queries[i], want)
		}
		if len(exec.execArgs[i]) != 1 || exec.execArgs[i][0] != "acme" {
			t.Errorf("query[%d] args = %v, want [acme]", i, exec.execArgs[i])
		}
	}
}

func TestRowLevel_DeprovisionTenant_Archive_IsNoop(t *testing.T) {
	exec := newTestExec()
	r := NewRowLevel("org_id", []string{"users"}, exec.fn)
	if err := r.DeprovisionTenant(context.Background(), "acme", "archive"); err != nil {
		t.Fatal(err)
	}
	if exec.calls != 0 {
		t.Errorf("expected no SQL for archive mode in row_level, got %d calls", exec.calls)
	}
}

func TestRowLevel_DeprovisionTenant_Delete_ExecutorError(t *testing.T) {
	exec := newTestExec(1) // fail on first call
	r := NewRowLevel("org_id", []string{"users", "orders"}, exec.fn)
	err := r.DeprovisionTenant(context.Background(), "acme", "delete")
	if err == nil {
		t.Fatal("expected error when executor fails")
	}
}

var errSentinel1 = fmt.Errorf("sentinel-1")
var errSentinel2 = fmt.Errorf("sentinel-2")

func TestRowLevel_DeprovisionTenant_MultiError_ErrorsIsUnwrapsIndividualErrors(t *testing.T) {
	// Both tables fail — errors.Is must unwrap individual errors (requires errors.Join, not %v).
	calls := 0
	exec := SQLExecutor(func(_ context.Context, _ string, _ ...any) error {
		calls++
		if calls == 1 {
			return errSentinel1
		}
		return errSentinel2
	})
	r := NewRowLevel("org_id", []string{"users", "orders"}, exec)
	err := r.DeprovisionTenant(context.Background(), "acme", "delete")
	if err == nil {
		t.Fatal("expected error when executor fails")
	}
	if !errors.Is(err, errSentinel1) {
		t.Errorf("errors.Is(err, errSentinel1) = false; errors.Join not used (got: %v)", err)
	}
	if !errors.Is(err, errSentinel2) {
		t.Errorf("errors.Is(err, errSentinel2) = false; errors.Join not used (got: %v)", err)
	}
}

// ─── validateIdentifier ───────────────────────────────────────────────────────

func TestValidateIdentifier_ValidInputs(t *testing.T) {
	valid := []string{"acme", "corp123", "tenant_1", "UPPER", "a", "abc_DEF_123"}
	for _, id := range valid {
		if err := validateIdentifier(id); err != nil {
			t.Errorf("validateIdentifier(%q) returned unexpected error: %v", id, err)
		}
	}
}

func TestValidateIdentifier_RejectsEmpty(t *testing.T) {
	if err := validateIdentifier(""); err == nil {
		t.Error("expected error for empty identifier")
	}
}

func TestValidateIdentifier_RejectsDangerousChars(t *testing.T) {
	dangerous := []string{
		"foo; DROP DATABASE admin",
		"foo'bar",
		`foo"bar`,
		"foo-bar",
		"foo.bar",
		"foo bar",
		"foo/bar",
		"foo\x00bar",
	}
	for _, id := range dangerous {
		if err := validateIdentifier(id); err == nil {
			t.Errorf("validateIdentifier(%q): expected error for dangerous identifier", id)
		}
	}
}

// ─── SchemaPerTenant — identifier validation ──────────────────────────────────

func TestSchemaPerTenant_ProvisionTenant_RejectsInvalidTenantID(t *testing.T) {
	exec := newTestExec()
	s := NewSchemaPerTenant("t_", exec.fn)
	err := s.ProvisionTenant(context.Background(), "bad-tenant!")
	if err == nil {
		t.Fatal("expected error for invalid tenantID")
	}
	if exec.calls != 0 {
		t.Error("executor should not be called with invalid tenantID")
	}
}

func TestSchemaPerTenant_DeprovisionTenant_RejectsInvalidTenantID(t *testing.T) {
	for _, mode := range []string{"archive", "delete"} {
		exec := newTestExec()
		s := NewSchemaPerTenant("t_", exec.fn)
		err := s.DeprovisionTenant(context.Background(), "bad; DROP", mode)
		if err == nil {
			t.Errorf("mode=%q: expected error for invalid tenantID", mode)
		}
		if exec.calls != 0 {
			t.Errorf("mode=%q: executor should not be called with invalid tenantID", mode)
		}
	}
}

// ─── DBPerTenant — identifier validation ─────────────────────────────────────

func TestDBPerTenant_ProvisionTenant_RejectsInvalidTenantID(t *testing.T) {
	exec := newTestExec()
	d := NewDBPerTenant(exec.fn)
	err := d.ProvisionTenant(context.Background(), "bad-tenant!")
	if err == nil {
		t.Fatal("expected error for invalid tenantID")
	}
	if exec.calls != 0 {
		t.Error("executor should not be called with invalid tenantID")
	}
}

func TestDBPerTenant_DeprovisionTenant_RejectsInvalidTenantID(t *testing.T) {
	for _, mode := range []string{"archive", "delete"} {
		exec := newTestExec()
		d := NewDBPerTenant(exec.fn)
		err := d.DeprovisionTenant(context.Background(), "bad; DROP", mode)
		if err == nil {
			t.Errorf("mode=%q: expected error for invalid tenantID", mode)
		}
		if exec.calls != 0 {
			t.Errorf("mode=%q: executor should not be called with invalid tenantID", mode)
		}
	}
}
