package tenancy

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// mockStrategy is a controllable TenancyStrategy for step tests.
type mockStrategy struct {
	resolveTableFn      func(tenantID, table string) string
	resolveConnFn       func(tenantID, base string) string
	tenantFilterFn      func(tenantID string) (string, string)
	provisionFn         func(ctx context.Context, tenantID string) error
	deprovisionFn       func(ctx context.Context, tenantID string, mode string) error
	provisionCallCount  atomic.Int64
	deprovisionCallCount atomic.Int64
}

func (m *mockStrategy) ResolveTable(tenantID, table string) string {
	if m.resolveTableFn != nil {
		return m.resolveTableFn(tenantID, table)
	}
	return table
}
func (m *mockStrategy) ResolveConnection(tenantID, base string) string {
	if m.resolveConnFn != nil {
		return m.resolveConnFn(tenantID, base)
	}
	return base
}
func (m *mockStrategy) TenantFilter(tenantID string) (string, string) {
	if m.tenantFilterFn != nil {
		return m.tenantFilterFn(tenantID)
	}
	return "", ""
}
func (m *mockStrategy) ProvisionTenant(ctx context.Context, tenantID string) error {
	m.provisionCallCount.Add(1)
	if m.provisionFn != nil {
		return m.provisionFn(ctx, tenantID)
	}
	return nil
}
func (m *mockStrategy) DeprovisionTenant(ctx context.Context, tenantID string, mode string) error {
	m.deprovisionCallCount.Add(1)
	if m.deprovisionFn != nil {
		return m.deprovisionFn(ctx, tenantID, mode)
	}
	return nil
}

// ─── tenant_provision ────────────────────────────────────────────────────────

func TestTenantProvisionStep_RequiresTenantID(t *testing.T) {
	s := &provisionStep{name: "test", strategy: &mockStrategy{}}
	_, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{})
	if err == nil {
		t.Fatal("expected error when tenant_id missing")
	}
}

func TestTenantProvisionStep_SchemaStrategy(t *testing.T) {
	exec := newTestExec()
	strategy := NewSchemaPerTenant("t_", exec.fn)
	s := &provisionStep{name: "test", strategy: strategy}

	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id": "acme",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["status"] != "provisioned" {
		t.Errorf("status = %v, want provisioned", result.Output["status"])
	}
	if result.Output["tenant_id"] != "acme" {
		t.Errorf("tenant_id = %v, want acme", result.Output["tenant_id"])
	}
	if exec.calls != 1 {
		t.Errorf("expected 1 SQL call, got %d", exec.calls)
	}
}

func TestTenantProvisionStep_DBStrategy(t *testing.T) {
	exec := newTestExec()
	strategy := NewDBPerTenant(exec.fn)
	s := &provisionStep{name: "test", strategy: strategy}

	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id": "corp",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["status"] != "provisioned" {
		t.Errorf("status = %v, want provisioned", result.Output["status"])
	}
	// DB strategy generates CREATE DATABASE SQL
	if len(exec.queries) != 1 || exec.queries[0] != "CREATE DATABASE corp" {
		t.Errorf("unexpected SQL: %v", exec.queries)
	}
}

func TestTenantProvisionStep_RowLevelStrategy(t *testing.T) {
	exec := newTestExec()
	strategy := NewRowLevel("org_id", nil, exec.fn)
	s := &provisionStep{name: "test", strategy: strategy}

	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id": "acme",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["status"] != "provisioned" {
		t.Errorf("status = %v, want provisioned", result.Output["status"])
	}
	// Row-level provision is a no-op
	if exec.calls != 0 {
		t.Errorf("expected no SQL for row_level provision, got %d", exec.calls)
	}
}

func TestTenantProvisionStep_StrategyError(t *testing.T) {
	mock := &mockStrategy{
		provisionFn: func(_ context.Context, _ string) error {
			return errors.New("db unavailable")
		},
	}
	s := &provisionStep{name: "test", strategy: mock}
	_, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id": "acme",
	})
	if err == nil {
		t.Fatal("expected error from failing strategy")
	}
}

// ─── tenant_deprovision ──────────────────────────────────────────────────────

func TestTenantDeprovisionStep_RequiresTenantID(t *testing.T) {
	s := &deprovisionStep{name: "test", strategy: &mockStrategy{}}
	_, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"mode": "delete",
	})
	if err == nil {
		t.Fatal("expected error when tenant_id missing")
	}
}

func TestTenantDeprovisionStep_RequiresMode(t *testing.T) {
	s := &deprovisionStep{name: "test", strategy: &mockStrategy{}}
	_, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id": "acme",
	})
	if err == nil {
		t.Fatal("expected error when mode missing")
	}
}

func TestTenantDeprovisionStep_Delete(t *testing.T) {
	mock := &mockStrategy{}
	s := &deprovisionStep{name: "test", strategy: mock}

	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id": "acme",
		"mode":      "delete",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["status"] != "deprovisioned" {
		t.Errorf("status = %v, want deprovisioned", result.Output["status"])
	}
	if result.Output["tenant_id"] != "acme" {
		t.Errorf("tenant_id = %v, want acme", result.Output["tenant_id"])
	}
	if result.Output["mode"] != "delete" {
		t.Errorf("mode = %v, want delete", result.Output["mode"])
	}
	if mock.deprovisionCallCount.Load() != 1 {
		t.Errorf("expected 1 deprovision call, got %d", mock.deprovisionCallCount.Load())
	}
}

func TestTenantDeprovisionStep_Archive(t *testing.T) {
	exec := newTestExec()
	strategy := NewSchemaPerTenant("t_", exec.fn)
	s := &deprovisionStep{name: "test", strategy: strategy}

	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id": "corp",
		"mode":      "archive",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["mode"] != "archive" {
		t.Errorf("mode = %v, want archive", result.Output["mode"])
	}
	wantSQL := "ALTER SCHEMA t_corp RENAME TO t_corp_archived"
	if len(exec.queries) != 1 || exec.queries[0] != wantSQL {
		t.Errorf("SQL = %v, want [%q]", exec.queries, wantSQL)
	}
}

// ─── tenant_migrate ──────────────────────────────────────────────────────────

func TestTenantMigrateStep_Parallel(t *testing.T) {
	mock := &mockStrategy{}
	s := &migrateStep{name: "test", strategy: mock}

	tenants := []any{"t1", "t2", "t3", "t4", "t5"}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_ids":  tenants,
		"parallelism": 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["status"] != "completed" {
		t.Errorf("status = %v, want completed", result.Output["status"])
	}
	if mock.provisionCallCount.Load() != 5 {
		t.Errorf("expected 5 provision calls, got %d", mock.provisionCallCount.Load())
	}
	if result.Output["count"].(int) != 5 {
		t.Errorf("count = %v, want 5", result.Output["count"])
	}
}

func TestTenantMigrateStep_CircuitBreaker(t *testing.T) {
	// All tenants fail → circuit breaker should stop after failure_threshold consecutive failures
	mock := &mockStrategy{
		provisionFn: func(_ context.Context, tenantID string) error {
			return errors.New("migration failed for " + tenantID)
		},
	}
	s := &migrateStep{name: "test", strategy: mock}

	tenants := []any{"t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10"}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_ids":        tenants,
		"parallelism":       1, // serial to make circuit-breaker deterministic
		"failure_threshold": 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["status"] != "stopped" {
		t.Errorf("status = %v, want stopped", result.Output["status"])
	}
	// With failure_threshold=3, should stop after 3 failures
	calls := mock.provisionCallCount.Load()
	if calls != 3 {
		t.Errorf("expected exactly 3 provision calls before circuit break, got %d", calls)
	}
	failed, ok := result.Output["failed"].([]string)
	if !ok || len(failed) != 3 {
		t.Errorf("expected 3 failed tenants, got %v", result.Output["failed"])
	}
}

func TestTenantMigrateStep_CircuitBreaker_ResetOnSuccess(t *testing.T) {
	// Pattern: success, fail, fail, success, fail, fail, fail → stops at 3rd consecutive
	calls := atomic.Int64{}
	mock := &mockStrategy{
		provisionFn: func(_ context.Context, tenantID string) error {
			n := calls.Add(1)
			// Call 1: success, 2: fail, 3: fail, 4: success, 5: fail, 6: fail, 7: fail
			switch n {
			case 2, 3, 5, 6, 7:
				return errors.New("fail")
			}
			return nil
		},
	}
	s := &migrateStep{name: "test", strategy: mock}

	tenants := []any{"t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10"}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_ids":        tenants,
		"parallelism":       1,
		"failure_threshold": 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Circuit should trip at call 7 (3rd consecutive fail after reset)
	if result.Output["status"] != "stopped" {
		t.Errorf("status = %v, want stopped", result.Output["status"])
	}
	if calls.Load() != 7 {
		t.Errorf("expected 7 provision calls, got %d", calls.Load())
	}
}

func TestTenantMigrateStep_AllSucceed(t *testing.T) {
	mock := &mockStrategy{}
	s := &migrateStep{name: "test", strategy: mock}

	tenants := []any{"t1", "t2", "t3"}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_ids":        tenants,
		"parallelism":       2,
		"failure_threshold": 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["status"] != "completed" {
		t.Errorf("status = %v, want completed", result.Output["status"])
	}
	if len(result.Output["failed"].([]string)) != 0 {
		t.Errorf("expected no failures, got %v", result.Output["failed"])
	}
}

func TestTenantMigrateStep_RequiresTenantIDs(t *testing.T) {
	s := &migrateStep{name: "test", strategy: &mockStrategy{}}
	_, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{})
	if err == nil {
		t.Fatal("expected error when tenant_ids missing")
	}
}

// ─── NewXxxStep constructors ─────────────────────────────────────────────────

func TestNewProvisionStep_SchemaStrategyFromConfig(t *testing.T) {
	inst, err := NewProvisionStep("test", map[string]any{
		"strategy":      "schema_per_tenant",
		"schema_prefix": "t_",
	})
	if err != nil {
		t.Fatal(err)
	}
	if inst == nil {
		t.Fatal("expected non-nil step instance")
	}
}

func TestNewProvisionStep_InvalidStrategy(t *testing.T) {
	_, err := NewProvisionStep("test", map[string]any{
		"strategy": "invalid",
	})
	if err == nil {
		t.Fatal("expected error for invalid strategy")
	}
}

func TestNewDeprovisionStep_DBStrategyFromConfig(t *testing.T) {
	inst, err := NewDeprovisionStep("test", map[string]any{
		"strategy":            "db_per_tenant",
		"connection_template": "postgres://localhost/{{tenant}}",
	})
	if err != nil {
		t.Fatal(err)
	}
	if inst == nil {
		t.Fatal("expected non-nil step instance")
	}
}

func TestNewMigrateStep_RowLevelFromConfig(t *testing.T) {
	inst, err := NewMigrateStep("test", map[string]any{
		"strategy":      "row_level",
		"tenant_column": "org_id",
	})
	if err != nil {
		t.Fatal(err)
	}
	if inst == nil {
		t.Fatal("expected non-nil step instance")
	}
}
