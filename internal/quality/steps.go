package quality

import (
	"context"
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// ── step.quality_check ────────────────────────────────────────────────────────

type qualityCheckStep struct{ name string }

func NewQualityCheckStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &qualityCheckStep{name: name}, nil
}

func (s *qualityCheckStep) Execute(
	ctx context.Context, _ map[string]any, _ map[string]map[string]any, _ map[string]any, _ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	mod, exec, err := resolveModule(config)
	if err != nil {
		return nil, fmt.Errorf("step.quality_check %q: %w", s.name, err)
	}
	_ = mod

	table, _ := config["table"].(string)
	if table == "" {
		return nil, fmt.Errorf("step.quality_check %q: table is required", s.name)
	}

	rawChecks := checksFromConfig(config)
	if len(rawChecks) == 0 {
		return nil, fmt.Errorf("step.quality_check %q: checks is required", s.name)
	}

	passed := true
	var results []any
	for _, chk := range rawChecks {
		cr, err := RunCheck(ctx, exec, chk.Type, table, chk.Config)
		if err != nil {
			return nil, fmt.Errorf("step.quality_check %q: %w", s.name, err)
		}
		if !cr.Passed {
			passed = false
		}
		results = append(results, map[string]any{
			"check":   cr.Check,
			"passed":  cr.Passed,
			"message": cr.Message,
			"value":   cr.Value,
		})
	}
	return &sdk.StepResult{Output: map[string]any{"passed": passed, "results": results}}, nil
}

// ── step.quality_schema_validate ──────────────────────────────────────────────

type qualitySchemaValidateStep struct{ name string }

func NewQualitySchemaValidateStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &qualitySchemaValidateStep{name: name}, nil
}

func (s *qualitySchemaValidateStep) Execute(
	ctx context.Context, _ map[string]any, _ map[string]map[string]any, _ map[string]any, _ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	_, exec, err := resolveModule(config)
	if err != nil {
		return nil, fmt.Errorf("step.quality_schema_validate %q: %w", s.name, err)
	}

	contractPath, _ := config["contract"].(string)
	if contractPath == "" {
		return nil, fmt.Errorf("step.quality_schema_validate %q: contract path is required", s.name)
	}

	contract, err := ParseContract(contractPath)
	if err != nil {
		return nil, fmt.Errorf("step.quality_schema_validate %q: %w", s.name, err)
	}

	result, err := ValidateContract(ctx, exec, *contract)
	if err != nil {
		return nil, fmt.Errorf("step.quality_schema_validate %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"valid":          result.Passed,
		"schemaOk":       result.SchemaOK,
		"qualityOk":      result.QualityOK,
		"schemaErrors":   result.SchemaErrors,
		"qualityResults": result.QualityResults,
	}}, nil
}

// ── step.quality_profile ──────────────────────────────────────────────────────

type qualityProfileStep struct{ name string }

func NewQualityProfileStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &qualityProfileStep{name: name}, nil
}

func (s *qualityProfileStep) Execute(
	ctx context.Context, _ map[string]any, _ map[string]map[string]any, _ map[string]any, _ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	_, exec, err := resolveModule(config)
	if err != nil {
		return nil, fmt.Errorf("step.quality_profile %q: %w", s.name, err)
	}

	table, _ := config["table"].(string)
	if table == "" {
		return nil, fmt.Errorf("step.quality_profile %q: table is required", s.name)
	}

	columns := stringSliceVal(config, "columns")

	result, err := Profile(ctx, exec, table, columns)
	if err != nil {
		return nil, fmt.Errorf("step.quality_profile %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"table":    result.Table,
		"rowCount": result.RowCount,
		"columns":  result.Columns,
	}}, nil
}

// ── step.quality_compare ──────────────────────────────────────────────────────

type qualityCompareStep struct{ name string }

func NewQualityCompareStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &qualityCompareStep{name: name}, nil
}

func (s *qualityCompareStep) Execute(
	ctx context.Context, _ map[string]any, _ map[string]map[string]any, _ map[string]any, _ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	_, exec, err := resolveModule(config)
	if err != nil {
		return nil, fmt.Errorf("step.quality_compare %q: %w", s.name, err)
	}

	baseline, _ := config["baseline"].(string)
	current, _ := config["current"].(string)
	if baseline == "" || current == "" {
		return nil, fmt.Errorf("step.quality_compare %q: baseline and current are required", s.name)
	}
	if err := validateIdent(baseline); err != nil {
		return nil, fmt.Errorf("step.quality_compare %q: baseline: %w", s.name, err)
	}
	if err := validateIdent(current); err != nil {
		return nil, fmt.Errorf("step.quality_compare %q: current: %w", s.name, err)
	}

	tolerances := tolerancesFromConfig(config)

	// Compare row counts.
	var baselineCount, currentCount int64
	if err := exec.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", baseline)).Scan(&baselineCount); err != nil {
		return nil, fmt.Errorf("step.quality_compare %q: baseline count: %w", s.name, err)
	}
	if err := exec.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", current)).Scan(&currentCount); err != nil {
		return nil, fmt.Errorf("step.quality_compare %q: current count: %w", s.name, err)
	}

	passed := true
	var diffs []map[string]any

	if baselineCount > 0 {
		rowCountDrift := absDiff(float64(baselineCount), float64(currentCount)) / float64(baselineCount)
		rowCountTol := tolerances["row_count"]
		withinTolerance := rowCountDrift <= rowCountTol
		if !withinTolerance {
			passed = false
		}
		diffs = append(diffs, map[string]any{
			"column":            "",
			"metric":            "row_count",
			"baseline":          baselineCount,
			"current":           currentCount,
			"within_tolerance":  withinTolerance,
		})
	}

	// Compare null rates for specified columns.
	columns := stringSliceVal(config, "columns")
	nullRateTol := tolerances["null_rate"]
	for _, col := range columns {
		if err := validateIdent(col); err != nil {
			return nil, fmt.Errorf("step.quality_compare %q: column: %w", s.name, err)
		}

		var baseNulls, curNulls int64
		if err := exec.QueryRowContext(ctx,
			fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NULL", baseline, col),
		).Scan(&baseNulls); err != nil {
			return nil, fmt.Errorf("step.quality_compare %q: baseline nulls for %s: %w", s.name, col, err)
		}
		if err := exec.QueryRowContext(ctx,
			fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NULL", current, col),
		).Scan(&curNulls); err != nil {
			return nil, fmt.Errorf("step.quality_compare %q: current nulls for %s: %w", s.name, col, err)
		}

		var baseNullRate, curNullRate float64
		if baselineCount > 0 {
			baseNullRate = float64(baseNulls) / float64(baselineCount)
		}
		if currentCount > 0 {
			curNullRate = float64(curNulls) / float64(currentCount)
		}

		drift := absDiff(baseNullRate, curNullRate)
		withinTolerance := drift <= nullRateTol
		if !withinTolerance {
			passed = false
		}
		diffs = append(diffs, map[string]any{
			"column":           col,
			"metric":           "null_rate",
			"baseline":         baseNullRate,
			"current":          curNullRate,
			"within_tolerance": withinTolerance,
		})
	}

	return &sdk.StepResult{Output: map[string]any{"passed": passed, "diffs": diffs}}, nil
}

// ── step.quality_anomaly ──────────────────────────────────────────────────────

type qualityAnomalyStep struct{ name string }

func NewQualityAnomalyStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &qualityAnomalyStep{name: name}, nil
}

func (s *qualityAnomalyStep) Execute(
	ctx context.Context, _ map[string]any, _ map[string]map[string]any, _ map[string]any, _ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	_, exec, err := resolveModule(config)
	if err != nil {
		return nil, fmt.Errorf("step.quality_anomaly %q: %w", s.name, err)
	}

	table, _ := config["table"].(string)
	if table == "" {
		return nil, fmt.Errorf("step.quality_anomaly %q: table is required", s.name)
	}
	if err := validateIdent(table); err != nil {
		return nil, fmt.Errorf("step.quality_anomaly %q: table: %w", s.name, err)
	}

	columns := stringSliceVal(config, "columns")
	if len(columns) == 0 {
		return nil, fmt.Errorf("step.quality_anomaly %q: columns is required", s.name)
	}

	method, _ := config["method"].(string)
	if method == "" {
		method = "zscore"
	}
	threshold := float64Val(config, "threshold", 0)

	var results []*AnomalyResult
	for _, col := range columns {
		if err := validateIdent(col); err != nil {
			return nil, fmt.Errorf("step.quality_anomaly %q: column: %w", s.name, err)
		}

		values, err := fetchNumericValues(ctx, exec, table, col)
		if err != nil {
			return nil, fmt.Errorf("step.quality_anomaly %q: fetch column %q: %w", s.name, col, err)
		}

		ar := DetectAnomalies(values, method, threshold)
		ar.Column = col
		results = append(results, ar)
	}

	return &sdk.StepResult{Output: map[string]any{"results": results}}, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

// resolveModule looks up the quality checks module named by config["module"].
func resolveModule(config map[string]any) (*ChecksModule, DBQuerier, error) {
	modName, _ := config["module"].(string)
	if modName == "" {
		return nil, nil, fmt.Errorf("module is required")
	}
	mod, err := LookupChecksModule(modName)
	if err != nil {
		return nil, nil, err
	}
	exec := mod.Executor()
	if exec == nil {
		return nil, nil, fmt.Errorf("quality: module %q has no database executor configured", modName)
	}
	return mod, exec, nil
}

// checksFromConfig parses the "checks" field from step config into []QualityCheck.
func checksFromConfig(config map[string]any) []QualityCheck {
	raw, ok := config["checks"]
	if !ok {
		return nil
	}
	switch t := raw.(type) {
	case []QualityCheck:
		return t
	case []any:
		var out []QualityCheck
		for _, item := range t {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			qc := QualityCheck{}
			if typ, ok := m["type"].(string); ok {
				qc.Type = typ
			}
			if cfg, ok := m["config"].(map[string]any); ok {
				qc.Config = cfg
			} else {
				// Treat remaining keys as the config directly.
				cfg := map[string]any{}
				for k, v := range m {
					if k != "type" {
						cfg[k] = v
					}
				}
				qc.Config = cfg
			}
			out = append(out, qc)
		}
		return out
	}
	return nil
}

// tolerancesFromConfig parses the "tolerances" map from step config.
func tolerancesFromConfig(config map[string]any) map[string]float64 {
	out := map[string]float64{
		"row_count": 0.05,
		"null_rate": 0.01,
	}
	raw, ok := config["tolerances"].(map[string]any)
	if !ok {
		return out
	}
	for k, v := range raw {
		if f, ok := toFloat64(v); ok {
			out[k] = f
		}
	}
	return out
}

// float64Val extracts a float64 from config with a default.
func float64Val(m map[string]any, key string, def float64) float64 {
	v, ok := m[key]
	if !ok {
		return def
	}
	if f, ok := toFloat64(v); ok {
		return f
	}
	return def
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

func absDiff(a, b float64) float64 {
	if a > b {
		return a - b
	}
	return b - a
}

// fetchNumericValues queries a column and returns all non-null float64 values.
func fetchNumericValues(ctx context.Context, exec DBQuerier, table, col string) ([]float64, error) {
	rows, err := exec.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL", col, table, col),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var values []float64
	for rows.Next() {
		var v float64
		if err := rows.Scan(&v); err != nil {
			break
		}
		values = append(values, v)
	}
	return values, rows.Err()
}
