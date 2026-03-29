package tenancy

import (
	"context"
	"database/sql"
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// Tier constants for the three supported isolation levels.
const (
	TierRowLevel        = "row_level"
	TierSchemaPerTenant = "schema_per_tenant"
	TierDBPerTenant     = "db_per_tenant"
)

// PromotionEvaluator compares tenant metrics against configured thresholds and
// recommends the next isolation tier when any threshold is exceeded.
type PromotionEvaluator struct {
	Thresholds map[string]float64
}

// PromotionResult holds the outcome of a promotion evaluation.
type PromotionResult struct {
	TenantID        string             `json:"tenant_id"`
	CurrentTier     string             `json:"current_tier"`
	RecommendedTier string             `json:"recommended_tier"`
	ShouldPromote   bool               `json:"should_promote"`
	Reason          string             `json:"reason"`
	Metrics         map[string]float64 `json:"metrics"`
}

// Evaluate returns a promotion recommendation for the tenant.
// It iterates over metrics and returns as soon as a threshold is exceeded.
func (e *PromotionEvaluator) Evaluate(tenantID, currentTier string, metrics map[string]float64) PromotionResult {
	result := PromotionResult{
		TenantID:        tenantID,
		CurrentTier:     currentTier,
		RecommendedTier: currentTier,
		Metrics:         metrics,
	}
	for metric, value := range metrics {
		threshold, ok := e.Thresholds[metric]
		if !ok {
			continue
		}
		if value > threshold {
			result.ShouldPromote = true
			result.RecommendedTier = nextTier(currentTier)
			result.Reason = fmt.Sprintf("%s (%.0f) exceeds threshold (%.0f)", metric, value, threshold)
			return result
		}
	}
	return result
}

// nextTier returns the next higher isolation tier.
func nextTier(current string) string {
	switch current {
	case TierRowLevel:
		return TierSchemaPerTenant
	case TierSchemaPerTenant:
		return TierDBPerTenant
	default:
		return current
	}
}

// PromotionDB is the minimal database interface required by promotion steps.
// *sql.DB satisfies this interface, enabling sqlmock injection in tests.
type PromotionDB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// ─── step.tenant_evaluate_promotion ──────────────────────────────────────────

type evaluatePromotionStep struct {
	name string
}

// NewEvaluatePromotionStep creates a step.tenant_evaluate_promotion instance.
func NewEvaluatePromotionStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &evaluatePromotionStep{name: name}, nil
}

func (s *evaluatePromotionStep) Execute(
	ctx context.Context,
	_ map[string]any,
	_ map[string]map[string]any,
	_, _ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	tenantID, _ := stringVal(config, "tenant_id")
	if tenantID == "" {
		return nil, fmt.Errorf("step.tenant_evaluate_promotion %q: tenant_id is required", s.name)
	}
	currentTier, _ := stringVal(config, "current_tier")
	if currentTier == "" {
		currentTier = TierRowLevel
	}

	// Parse metrics.
	metrics := map[string]float64{}
	if m, ok := config["metrics"].(map[string]any); ok {
		for k, v := range m {
			metrics[k] = toFloat64(v)
		}
	}

	// Parse thresholds (defaults match task spec).
	thresholds := map[string]float64{
		"rowCount":     1_000_000,
		"queryRate":    100,
		"storageBytes": 10_737_418_240,
	}
	if t, ok := config["thresholds"].(map[string]any); ok {
		for k, v := range t {
			thresholds[k] = toFloat64(v)
		}
	}

	evaluator := &PromotionEvaluator{Thresholds: thresholds}
	r := evaluator.Evaluate(tenantID, currentTier, metrics)

	return &sdk.StepResult{Output: map[string]any{
		"tenant_id":        r.TenantID,
		"current_tier":     r.CurrentTier,
		"recommended_tier": r.RecommendedTier,
		"should_promote":   r.ShouldPromote,
		"reason":           r.Reason,
		"metrics":          metrics,
	}}, nil
}

// ─── step.tenant_promote ──────────────────────────────────────────────────────

// promoteStep implements step.tenant_promote.
// It provisions the target namespace then copies data using INSERT INTO … SELECT.
type promoteStep struct {
	name string
	db   PromotionDB // nil unless injected (e.g. via sqlmock in tests)
}

// NewPromoteStep creates a step.tenant_promote instance.
func NewPromoteStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &promoteStep{name: name}, nil
}

func (s *promoteStep) Execute(
	ctx context.Context,
	_ map[string]any,
	_ map[string]map[string]any,
	_, _ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	if s.db == nil {
		return nil, fmt.Errorf("step.tenant_promote %q: no database connection configured", s.name)
	}

	tenantID, _ := stringVal(config, "tenant_id")
	if tenantID == "" {
		return nil, fmt.Errorf("step.tenant_promote %q: tenant_id is required", s.name)
	}
	if err := validateIdentifier(tenantID); err != nil {
		return nil, fmt.Errorf("step.tenant_promote %q: %w", s.name, err)
	}

	targetStrategy, _ := stringVal(config, "target_strategy")
	if targetStrategy == "" {
		return nil, fmt.Errorf("step.tenant_promote %q: target_strategy is required", s.name)
	}

	tables := listVal(config, "tables")
	if len(tables) == 0 {
		return nil, fmt.Errorf("step.tenant_promote %q: tables is required", s.name)
	}

	schemaPrefix, _ := stringVal(config, "schema_prefix")
	tenantColumn, _ := stringVal(config, "tenant_column")
	if tenantColumn == "" {
		tenantColumn = "tenant_id"
	}

	deleteFromShared := false
	if v, ok := config["delete_from_shared"].(bool); ok {
		deleteFromShared = v
	}

	fromTier, _ := stringVal(config, "from_tier")
	if fromTier == "" {
		fromTier = TierRowLevel
	}

	// Provision the target namespace (CREATE SCHEMA or CREATE DATABASE).
	exec := sqlExecAdapter(s.db)
	switch targetStrategy {
	case TierSchemaPerTenant:
		st := NewSchemaPerTenant(schemaPrefix, exec)
		if err := st.ProvisionTenant(ctx, tenantID); err != nil {
			return nil, fmt.Errorf("step.tenant_promote %q: provision: %w", s.name, err)
		}
	case TierDBPerTenant:
		st := NewDBPerTenant(exec)
		if err := st.ProvisionTenant(ctx, tenantID); err != nil {
			return nil, fmt.Errorf("step.tenant_promote %q: provision: %w", s.name, err)
		}
	default:
		return nil, fmt.Errorf("step.tenant_promote %q: unknown target_strategy %q", s.name, targetStrategy)
	}

	targetPrefix := ""
	if targetStrategy == TierSchemaPerTenant {
		targetPrefix = schemaPrefix + tenantID + "."
	}

	totalRows := int64(0)
	for _, table := range tables {
		targetTable := targetPrefix + table

		// Copy data: INSERT INTO target SELECT * FROM source WHERE tenant_id = $1
		res, err := s.db.ExecContext(ctx,
			fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WHERE %s = $1", targetTable, table, tenantColumn),
			tenantID)
		if err != nil {
			return nil, fmt.Errorf("step.tenant_promote %q: copy %s: %w", s.name, table, err)
		}
		rowsCopied, _ := res.RowsAffected()

		// Verify row count in source matches what was copied.
		var sourceCount int64
		row := s.db.QueryRowContext(ctx,
			fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = $1", table, tenantColumn),
			tenantID)
		if err := row.Scan(&sourceCount); err != nil {
			return nil, fmt.Errorf("step.tenant_promote %q: count %s: %w", s.name, table, err)
		}
		if rowsCopied != sourceCount {
			return nil, fmt.Errorf("step.tenant_promote %q: row count mismatch for %s: copied %d, source has %d",
				s.name, table, rowsCopied, sourceCount)
		}
		totalRows += rowsCopied

		// Optionally remove tenant data from the shared table.
		if deleteFromShared {
			if _, err := s.db.ExecContext(ctx,
				fmt.Sprintf("DELETE FROM %s WHERE %s = $1", table, tenantColumn),
				tenantID); err != nil {
				return nil, fmt.Errorf("step.tenant_promote %q: delete from %s: %w", s.name, table, err)
			}
		}
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":        "promoted",
		"tenant_id":     tenantID,
		"from_tier":     fromTier,
		"to_tier":       targetStrategy,
		"rows_migrated": totalRows,
		"tables":        tables,
	}}, nil
}

// ─── step.tenant_demote ───────────────────────────────────────────────────────

// demoteStep implements step.tenant_demote.
// It copies data from a dedicated namespace back to the shared table.
type demoteStep struct {
	name string
	db   PromotionDB
}

// NewDemoteStep creates a step.tenant_demote instance.
func NewDemoteStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &demoteStep{name: name}, nil
}

func (s *demoteStep) Execute(
	ctx context.Context,
	_ map[string]any,
	_ map[string]map[string]any,
	_, _ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	if s.db == nil {
		return nil, fmt.Errorf("step.tenant_demote %q: no database connection configured", s.name)
	}

	tenantID, _ := stringVal(config, "tenant_id")
	if tenantID == "" {
		return nil, fmt.Errorf("step.tenant_demote %q: tenant_id is required", s.name)
	}
	if err := validateIdentifier(tenantID); err != nil {
		return nil, fmt.Errorf("step.tenant_demote %q: %w", s.name, err)
	}

	fromTier, _ := stringVal(config, "from_tier")
	if fromTier == "" {
		fromTier = TierSchemaPerTenant
	}

	tables := listVal(config, "tables")
	if len(tables) == 0 {
		return nil, fmt.Errorf("step.tenant_demote %q: tables is required", s.name)
	}

	schemaPrefix, _ := stringVal(config, "schema_prefix")

	sourcePrefix := ""
	if fromTier == TierSchemaPerTenant {
		sourcePrefix = schemaPrefix + tenantID + "."
	}

	totalRows := int64(0)
	for _, table := range tables {
		sourceTable := sourcePrefix + table

		// Copy data back to the shared table.
		res, err := s.db.ExecContext(ctx,
			fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", table, sourceTable))
		if err != nil {
			return nil, fmt.Errorf("step.tenant_demote %q: copy %s: %w", s.name, table, err)
		}
		rowsCopied, _ := res.RowsAffected()

		// Verify row count matches.
		var sourceCount int64
		row := s.db.QueryRowContext(ctx,
			fmt.Sprintf("SELECT COUNT(*) FROM %s", sourceTable))
		if err := row.Scan(&sourceCount); err != nil {
			return nil, fmt.Errorf("step.tenant_demote %q: count %s: %w", s.name, table, err)
		}
		if rowsCopied != sourceCount {
			return nil, fmt.Errorf("step.tenant_demote %q: row count mismatch for %s: copied %d, source has %d",
				s.name, table, rowsCopied, sourceCount)
		}
		totalRows += rowsCopied
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":        "demoted",
		"tenant_id":     tenantID,
		"from_tier":     fromTier,
		"to_tier":       TierRowLevel,
		"rows_migrated": totalRows,
	}}, nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// sqlExecAdapter adapts a PromotionDB to a SQLExecutor.
func sqlExecAdapter(db PromotionDB) SQLExecutor {
	return func(ctx context.Context, query string, args ...any) error {
		_, err := db.ExecContext(ctx, query, args...)
		return err
	}
}

// toFloat64 converts common numeric types to float64.
func toFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case int32:
		return float64(n)
	}
	return 0
}
