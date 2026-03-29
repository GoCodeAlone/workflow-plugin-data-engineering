package quality

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// QualityCheck describes a quality assertion to run against a table.
type QualityCheck struct {
	Type   string         `json:"type"             yaml:"type"`
	Config map[string]any `json:"config,omitempty" yaml:"config,omitempty"`
}

// CheckResult is the outcome of one quality check.
type CheckResult struct {
	Check   string `json:"check"`
	Passed  bool   `json:"passed"`
	Message string `json:"message"`
	Value   any    `json:"value,omitempty"`
}

// QualityChecker runs a single quality check against a table.
type QualityChecker interface {
	Run(ctx context.Context, exec DBQuerier, table string, config map[string]any) (*CheckResult, error)
}

// builtinCheckers is the registry of built-in check types.
var builtinCheckers = map[string]QualityChecker{
	"not_null":    &notNullChecker{},
	"unique":      &uniqueChecker{},
	"freshness":   &freshnessChecker{},
	"row_count":   &rowCountChecker{},
	"referential": &referentialChecker{},
	"custom_sql":  &customSQLChecker{},
}

// RunCheck executes a named quality check type against a table.
func RunCheck(ctx context.Context, exec DBQuerier, checkType, table string, config map[string]any) (*CheckResult, error) {
	c, ok := builtinCheckers[checkType]
	if !ok {
		return nil, fmt.Errorf("unknown check type %q", checkType)
	}
	return c.Run(ctx, exec, table, config)
}

// ── not_null ──────────────────────────────────────────────────────────────────

type notNullChecker struct{}

func (c *notNullChecker) Run(ctx context.Context, exec DBQuerier, table string, config map[string]any) (*CheckResult, error) {
	if err := validateIdent(table); err != nil {
		return nil, fmt.Errorf("not_null: table: %w", err)
	}
	columns := stringSliceVal(config, "columns")
	if len(columns) == 0 {
		return nil, fmt.Errorf("not_null: columns is required")
	}

	var totalNulls int64
	var messages []string
	for _, col := range columns {
		if err := validateIdent(col); err != nil {
			return nil, fmt.Errorf("not_null: column: %w", err)
		}
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NULL", table, col)
		var count int64
		if err := exec.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return nil, fmt.Errorf("not_null: query column %q: %w", col, err)
		}
		if count > 0 {
			messages = append(messages, fmt.Sprintf("column %q has %d null(s)", col, count))
			totalNulls += count
		}
	}

	passed := len(messages) == 0
	msg := "all columns have no nulls"
	if !passed {
		msg = strings.Join(messages, "; ")
	}
	return &CheckResult{Check: "not_null", Passed: passed, Message: msg, Value: totalNulls}, nil
}

// ── unique ────────────────────────────────────────────────────────────────────

type uniqueChecker struct{}

func (c *uniqueChecker) Run(ctx context.Context, exec DBQuerier, table string, config map[string]any) (*CheckResult, error) {
	if err := validateIdent(table); err != nil {
		return nil, fmt.Errorf("unique: table: %w", err)
	}
	columns := stringSliceVal(config, "columns")
	if len(columns) == 0 {
		return nil, fmt.Errorf("unique: columns is required")
	}

	var totalDupes int64
	var messages []string
	for _, col := range columns {
		if err := validateIdent(col); err != nil {
			return nil, fmt.Errorf("unique: column: %w", err)
		}
		query := fmt.Sprintf(
			"SELECT COUNT(*) FROM (SELECT %s FROM %s GROUP BY %s HAVING COUNT(*) > 1) AS _q",
			col, table, col,
		)
		var count int64
		if err := exec.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return nil, fmt.Errorf("unique: query column %q: %w", col, err)
		}
		if count > 0 {
			messages = append(messages, fmt.Sprintf("column %q has %d duplicate value(s)", col, count))
			totalDupes += count
		}
	}

	passed := len(messages) == 0
	msg := "all columns are unique"
	if !passed {
		msg = strings.Join(messages, "; ")
	}
	return &CheckResult{Check: "unique", Passed: passed, Message: msg, Value: totalDupes}, nil
}

// ── freshness ─────────────────────────────────────────────────────────────────

type freshnessChecker struct{}

func (c *freshnessChecker) Run(ctx context.Context, exec DBQuerier, table string, config map[string]any) (*CheckResult, error) {
	if err := validateIdent(table); err != nil {
		return nil, fmt.Errorf("freshness: table: %w", err)
	}
	col, _ := config["column"].(string)
	if col == "" {
		return nil, fmt.Errorf("freshness: column is required")
	}
	if err := validateIdent(col); err != nil {
		return nil, fmt.Errorf("freshness: column: %w", err)
	}
	maxAgeStr, _ := config["maxAge"].(string)
	if maxAgeStr == "" {
		return nil, fmt.Errorf("freshness: maxAge is required")
	}
	maxAge, err := time.ParseDuration(maxAgeStr)
	if err != nil {
		return nil, fmt.Errorf("freshness: maxAge %q: %w", maxAgeStr, err)
	}

	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", col, table)
	var lastUpdated sql.NullTime
	if err := exec.QueryRowContext(ctx, query).Scan(&lastUpdated); err != nil {
		return nil, fmt.Errorf("freshness: query: %w", err)
	}

	if !lastUpdated.Valid {
		return &CheckResult{
			Check:   "freshness",
			Passed:  false,
			Message: "table is empty (no rows found)",
			Value:   nil,
		}, nil
	}

	age := time.Since(lastUpdated.Time)
	passed := age <= maxAge
	msg := fmt.Sprintf("last update was %v ago (max allowed: %v)", age.Round(time.Second), maxAge)
	return &CheckResult{
		Check:   "freshness",
		Passed:  passed,
		Message: msg,
		Value:   lastUpdated.Time,
	}, nil
}

// ── row_count ─────────────────────────────────────────────────────────────────

type rowCountChecker struct{}

func (c *rowCountChecker) Run(ctx context.Context, exec DBQuerier, table string, config map[string]any) (*CheckResult, error) {
	if err := validateIdent(table); err != nil {
		return nil, fmt.Errorf("row_count: table: %w", err)
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	var count int64
	if err := exec.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return nil, fmt.Errorf("row_count: query: %w", err)
	}

	min := int64Val(config, "min", -1)
	max := int64Val(config, "max", -1)

	passed := true
	var msgs []string
	if min >= 0 && count < min {
		passed = false
		msgs = append(msgs, fmt.Sprintf("row count %d is below minimum %d", count, min))
	}
	if max >= 0 && count > max {
		passed = false
		msgs = append(msgs, fmt.Sprintf("row count %d exceeds maximum %d", count, max))
	}

	msg := fmt.Sprintf("row count: %d", count)
	if len(msgs) > 0 {
		msg = strings.Join(msgs, "; ")
	}
	return &CheckResult{Check: "row_count", Passed: passed, Message: msg, Value: count}, nil
}

// ── referential ───────────────────────────────────────────────────────────────

type referentialChecker struct{}

func (c *referentialChecker) Run(ctx context.Context, exec DBQuerier, table string, config map[string]any) (*CheckResult, error) {
	if err := validateIdent(table); err != nil {
		return nil, fmt.Errorf("referential: table: %w", err)
	}
	col, _ := config["column"].(string)
	refTable, _ := config["refTable"].(string)
	refCol, _ := config["refColumn"].(string)

	for name, v := range map[string]string{"column": col, "refTable": refTable, "refColumn": refCol} {
		if v == "" {
			return nil, fmt.Errorf("referential: %s is required", name)
		}
		if err := validateIdent(v); err != nil {
			return nil, fmt.Errorf("referential: %s: %w", name, err)
		}
	}

	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s t LEFT JOIN %s r ON t.%s = r.%s WHERE r.%s IS NULL",
		table, refTable, col, refCol, refCol,
	)
	var orphans int64
	if err := exec.QueryRowContext(ctx, query).Scan(&orphans); err != nil {
		return nil, fmt.Errorf("referential: query: %w", err)
	}

	passed := orphans == 0
	msg := "no orphan rows found"
	if !passed {
		msg = fmt.Sprintf("%d orphan row(s) in %s.%s not found in %s.%s", orphans, table, col, refTable, refCol)
	}
	return &CheckResult{Check: "referential", Passed: passed, Message: msg, Value: orphans}, nil
}

// ── custom_sql ────────────────────────────────────────────────────────────────

type customSQLChecker struct{}

func (c *customSQLChecker) Run(ctx context.Context, exec DBQuerier, _ string, config map[string]any) (*CheckResult, error) {
	query, _ := config["query"].(string)
	if query == "" {
		return nil, fmt.Errorf("custom_sql: query is required")
	}
	threshold := int64Val(config, "threshold", 0)

	var result int64
	if err := exec.QueryRowContext(ctx, query).Scan(&result); err != nil {
		return nil, fmt.Errorf("custom_sql: query: %w", err)
	}

	passed := result <= threshold
	msg := fmt.Sprintf("query returned %d (threshold: %d)", result, threshold)
	return &CheckResult{Check: "custom_sql", Passed: passed, Message: msg, Value: result}, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

// stringSliceVal extracts a []string from a config map key that holds []any or []string.
func stringSliceVal(m map[string]any, key string) []string {
	v, ok := m[key]
	if !ok {
		return nil
	}
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// int64Val extracts an int64 from a config map key with a default fallback.
func int64Val(m map[string]any, key string, def int64) int64 {
	v, ok := m[key]
	if !ok {
		return def
	}
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	}
	return def
}
