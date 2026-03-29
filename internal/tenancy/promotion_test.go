package tenancy

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// ─── PromotionEvaluator ───────────────────────────────────────────────────────

func TestEvaluatePromotion_ShouldPromote(t *testing.T) {
	eval := &PromotionEvaluator{
		Thresholds: map[string]float64{
			"rowCount": 1_000_000,
		},
	}
	result := eval.Evaluate("acme", TierRowLevel, map[string]float64{
		"rowCount": 1_500_000,
	})
	if !result.ShouldPromote {
		t.Error("expected ShouldPromote=true")
	}
	if result.RecommendedTier != TierSchemaPerTenant {
		t.Errorf("RecommendedTier = %q, want %q", result.RecommendedTier, TierSchemaPerTenant)
	}
	if result.CurrentTier != TierRowLevel {
		t.Errorf("CurrentTier = %q, want %q", result.CurrentTier, TierRowLevel)
	}
	if result.TenantID != "acme" {
		t.Errorf("TenantID = %q, want acme", result.TenantID)
	}
	if result.Reason == "" {
		t.Error("expected non-empty Reason")
	}
}

func TestEvaluatePromotion_NoPromote(t *testing.T) {
	eval := &PromotionEvaluator{
		Thresholds: map[string]float64{
			"rowCount":  1_000_000,
			"queryRate": 100,
		},
	}
	result := eval.Evaluate("corp", TierRowLevel, map[string]float64{
		"rowCount":  500_000,
		"queryRate": 50,
	})
	if result.ShouldPromote {
		t.Error("expected ShouldPromote=false")
	}
	if result.RecommendedTier != TierRowLevel {
		t.Errorf("RecommendedTier = %q, want %q", result.RecommendedTier, TierRowLevel)
	}
}

func TestEvaluatePromotion_SchemaToDBPromotion(t *testing.T) {
	eval := &PromotionEvaluator{
		Thresholds: map[string]float64{"storageBytes": 10_737_418_240},
	}
	result := eval.Evaluate("bigcorp", TierSchemaPerTenant, map[string]float64{
		"storageBytes": 20_000_000_000,
	})
	if !result.ShouldPromote {
		t.Error("expected ShouldPromote=true")
	}
	if result.RecommendedTier != TierDBPerTenant {
		t.Errorf("RecommendedTier = %q, want %q", result.RecommendedTier, TierDBPerTenant)
	}
}

func TestEvaluatePromotion_AlreadyAtTop(t *testing.T) {
	eval := &PromotionEvaluator{
		Thresholds: map[string]float64{"rowCount": 1},
	}
	result := eval.Evaluate("giant", TierDBPerTenant, map[string]float64{"rowCount": 9999999})
	if !result.ShouldPromote {
		t.Error("expected ShouldPromote=true (threshold exceeded)")
	}
	// Already at top tier — recommended stays the same.
	if result.RecommendedTier != TierDBPerTenant {
		t.Errorf("RecommendedTier = %q, want %q (no higher tier)", result.RecommendedTier, TierDBPerTenant)
	}
}

// ─── step.tenant_evaluate_promotion ──────────────────────────────────────────

func TestEvaluatePromotionStep_ShouldPromote(t *testing.T) {
	s := &evaluatePromotionStep{name: "test"}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id":    "acme",
		"current_tier": TierRowLevel,
		"metrics": map[string]any{
			"rowCount": float64(2_000_000),
		},
		"thresholds": map[string]any{
			"rowCount": float64(1_000_000),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["should_promote"] != true {
		t.Errorf("should_promote = %v, want true", result.Output["should_promote"])
	}
	if result.Output["recommended_tier"] != TierSchemaPerTenant {
		t.Errorf("recommended_tier = %v, want %q", result.Output["recommended_tier"], TierSchemaPerTenant)
	}
}

func TestEvaluatePromotionStep_NoPromote(t *testing.T) {
	s := &evaluatePromotionStep{name: "test"}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id":    "small",
		"current_tier": TierRowLevel,
		"metrics": map[string]any{
			"rowCount": float64(100),
		},
		"thresholds": map[string]any{
			"rowCount": float64(1_000_000),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["should_promote"] != false {
		t.Errorf("should_promote = %v, want false", result.Output["should_promote"])
	}
}

func TestEvaluatePromotionStep_RequiresTenantID(t *testing.T) {
	s := &evaluatePromotionStep{name: "test"}
	_, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{})
	if err == nil {
		t.Fatal("expected error for missing tenant_id")
	}
}

func TestEvaluatePromotionStep_DefaultsToRowLevel(t *testing.T) {
	s := &evaluatePromotionStep{name: "test"}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id": "acme",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["current_tier"] != TierRowLevel {
		t.Errorf("current_tier = %v, want %q (default)", result.Output["current_tier"], TierRowLevel)
	}
}

// ─── step.tenant_promote (sqlmock) ───────────────────────────────────────────

func TestTenantPromote_SchemaPerTenant(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// CREATE SCHEMA IF NOT EXISTS t_acme
	mock.ExpectExec("CREATE SCHEMA").
		WillReturnResult(sqlmock.NewResult(0, 0))
	// INSERT INTO t_acme.users SELECT * FROM users WHERE tenant_id = $1
	mock.ExpectExec("INSERT INTO t_acme.users").
		WithArgs("acme").
		WillReturnResult(sqlmock.NewResult(0, 5))
	// SELECT COUNT(*) FROM users WHERE tenant_id = $1
	mock.ExpectQuery("SELECT COUNT").
		WithArgs("acme").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))

	s := &promoteStep{name: "test", db: db}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id":       "acme",
		"target_strategy": TierSchemaPerTenant,
		"schema_prefix":   "t_",
		"tables":          []string{"users"},
		"from_tier":       TierRowLevel,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "promoted" {
		t.Errorf("status = %v, want promoted", result.Output["status"])
	}
	if result.Output["rows_migrated"].(int64) != 5 {
		t.Errorf("rows_migrated = %v, want 5", result.Output["rows_migrated"])
	}
	if result.Output["from_tier"] != TierRowLevel {
		t.Errorf("from_tier = %v, want %q", result.Output["from_tier"], TierRowLevel)
	}
	if result.Output["to_tier"] != TierSchemaPerTenant {
		t.Errorf("to_tier = %v, want %q", result.Output["to_tier"], TierSchemaPerTenant)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestTenantPromote_RowCountVerification(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// CREATE SCHEMA succeeds.
	mock.ExpectExec("CREATE SCHEMA").
		WillReturnResult(sqlmock.NewResult(0, 0))
	// INSERT copies 3 rows.
	mock.ExpectExec("INSERT INTO t_corp.orders").
		WithArgs("corp").
		WillReturnResult(sqlmock.NewResult(0, 3))
	// But source reports 5 rows → mismatch.
	mock.ExpectQuery("SELECT COUNT").
		WithArgs("corp").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))

	s := &promoteStep{name: "test", db: db}
	_, err = s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id":       "corp",
		"target_strategy": TierSchemaPerTenant,
		"schema_prefix":   "t_",
		"tables":          []string{"orders"},
	})
	if err == nil {
		t.Fatal("expected row count mismatch error")
	}
}

func TestTenantPromote_WithDeleteFromShared(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectExec("CREATE SCHEMA").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO t_del.users").
		WithArgs("del").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectQuery("SELECT COUNT").
		WithArgs("del").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	// DELETE FROM users WHERE tenant_id = $1
	mock.ExpectExec("DELETE FROM users").
		WithArgs("del").
		WillReturnResult(sqlmock.NewResult(0, 2))

	s := &promoteStep{name: "test", db: db}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id":          "del",
		"target_strategy":    TierSchemaPerTenant,
		"schema_prefix":      "t_",
		"tables":             []string{"users"},
		"delete_from_shared": true,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "promoted" {
		t.Errorf("status = %v, want promoted", result.Output["status"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestTenantPromote_RequiresTenantID(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	s := &promoteStep{name: "test", db: db}
	_, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"target_strategy": TierSchemaPerTenant,
		"tables":          []string{"users"},
	})
	if err == nil {
		t.Fatal("expected error for missing tenant_id")
	}
}

func TestTenantPromote_RequiresTargetStrategy(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	s := &promoteStep{name: "test", db: db}
	_, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id": "acme",
		"tables":    []string{"users"},
	})
	if err == nil {
		t.Fatal("expected error for missing target_strategy")
	}
}

func TestTenantPromote_InvalidTenantID(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	s := &promoteStep{name: "test", db: db}
	_, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id":       "bad; DROP",
		"target_strategy": TierSchemaPerTenant,
		"tables":          []string{"users"},
	})
	if err == nil {
		t.Fatal("expected error for invalid tenant_id")
	}
}

func TestTenantPromote_NoDB(t *testing.T) {
	s := &promoteStep{name: "test", db: nil}
	_, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id":       "acme",
		"target_strategy": TierSchemaPerTenant,
		"tables":          []string{"users"},
	})
	if err == nil {
		t.Fatal("expected error when no db configured")
	}
}

// ─── step.tenant_demote (sqlmock) ────────────────────────────────────────────

func TestTenantDemote(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// INSERT INTO users SELECT * FROM t_acme.users
	mock.ExpectExec("INSERT INTO users").
		WillReturnResult(sqlmock.NewResult(0, 4))
	// SELECT COUNT(*) FROM t_acme.users
	mock.ExpectQuery("SELECT COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(4))

	s := &demoteStep{name: "test", db: db}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id":     "acme",
		"from_tier":     TierSchemaPerTenant,
		"schema_prefix": "t_",
		"tables":        []string{"users"},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "demoted" {
		t.Errorf("status = %v, want demoted", result.Output["status"])
	}
	if result.Output["to_tier"] != TierRowLevel {
		t.Errorf("to_tier = %v, want %q", result.Output["to_tier"], TierRowLevel)
	}
	if result.Output["rows_migrated"].(int64) != 4 {
		t.Errorf("rows_migrated = %v, want 4", result.Output["rows_migrated"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestTenantDemote_RequiresTenantID(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	s := &demoteStep{name: "test", db: db}
	_, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tables": []string{"users"},
	})
	if err == nil {
		t.Fatal("expected error for missing tenant_id")
	}
}

func TestTenantDemote_RowCountMismatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectExec("INSERT INTO").
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectQuery("SELECT COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(7))

	s := &demoteStep{name: "test", db: db}
	_, err = s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id":     "acme",
		"from_tier":     TierSchemaPerTenant,
		"schema_prefix": "t_",
		"tables":        []string{"users"},
	})
	if err == nil {
		t.Fatal("expected row count mismatch error")
	}
}

func TestTenantDemote_NoDB(t *testing.T) {
	s := &demoteStep{name: "test", db: nil}
	_, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"tenant_id": "acme",
		"tables":    []string{"users"},
	})
	if err == nil {
		t.Fatal("expected error when no db configured")
	}
}

// ─── NewXxxStep constructors ──────────────────────────────────────────────────

func TestNewEvaluatePromotionStep(t *testing.T) {
	inst, err := NewEvaluatePromotionStep("test", nil)
	if err != nil || inst == nil {
		t.Fatalf("NewEvaluatePromotionStep: %v", err)
	}
}

func TestNewPromoteStep(t *testing.T) {
	inst, err := NewPromoteStep("test", nil)
	if err != nil || inst == nil {
		t.Fatalf("NewPromoteStep: %v", err)
	}
}

func TestNewDemoteStep(t *testing.T) {
	inst, err := NewDemoteStep("test", nil)
	if err != nil || inst == nil {
		t.Fatalf("NewDemoteStep: %v", err)
	}
}
