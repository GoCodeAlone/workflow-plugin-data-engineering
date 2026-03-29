package quality

import (
	"context"
	"fmt"
	"sort"

	"gonum.org/v1/gonum/stat"
)

// ProfileResult holds statistical profile of a table.
type ProfileResult struct {
	Table    string                    `json:"table"`
	RowCount int64                     `json:"rowCount"`
	Columns  map[string]*ColumnProfile `json:"columns"`
}

// ColumnProfile holds statistics for a single column.
type ColumnProfile struct {
	NullCount     int64        `json:"nullCount"`
	NullRate      float64      `json:"nullRate"`
	DistinctCount int64        `json:"distinctCount"`
	Min           any          `json:"min,omitempty"`
	Max           any          `json:"max,omitempty"`
	Mean          *float64     `json:"mean,omitempty"`
	StdDev        *float64     `json:"stdDev,omitempty"`
	Percentiles   *Percentiles `json:"percentiles,omitempty"`
}

// Percentiles holds common percentile values for numeric columns.
type Percentiles struct {
	P25 float64 `json:"p25"`
	P50 float64 `json:"p50"`
	P75 float64 `json:"p75"`
	P90 float64 `json:"p90"`
	P99 float64 `json:"p99"`
}

// Profile computes a statistical profile of the named table and columns.
// If columns is empty, callers should pass the desired column names; this function does not
// auto-discover columns from information_schema.
func Profile(ctx context.Context, exec DBQuerier, table string, columns []string) (*ProfileResult, error) {
	if err := validateIdent(table); err != nil {
		return nil, fmt.Errorf("profile: table: %w", err)
	}

	var rowCount int64
	if err := exec.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&rowCount); err != nil {
		return nil, fmt.Errorf("profile: row count: %w", err)
	}

	result := &ProfileResult{
		Table:    table,
		RowCount: rowCount,
		Columns:  make(map[string]*ColumnProfile),
	}

	for _, col := range columns {
		if err := validateIdent(col); err != nil {
			return nil, fmt.Errorf("profile: column: %w", err)
		}
		cp, err := profileColumn(ctx, exec, table, col, rowCount)
		if err != nil {
			return nil, fmt.Errorf("profile: column %q: %w", col, err)
		}
		result.Columns[col] = cp
	}

	return result, nil
}

func profileColumn(ctx context.Context, exec DBQuerier, table, col string, rowCount int64) (*ColumnProfile, error) {
	cp := &ColumnProfile{}

	// Null count.
	if err := exec.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NULL", table, col),
	).Scan(&cp.NullCount); err != nil {
		return nil, fmt.Errorf("null count: %w", err)
	}
	if rowCount > 0 {
		cp.NullRate = float64(cp.NullCount) / float64(rowCount)
	}

	// Distinct count.
	if err := exec.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(DISTINCT %s) FROM %s", col, table),
	).Scan(&cp.DistinctCount); err != nil {
		return nil, fmt.Errorf("distinct count: %w", err)
	}

	// Min / Max.
	var minVal, maxVal any
	if err := exec.QueryRowContext(ctx,
		fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", col, col, table),
	).Scan(&minVal, &maxVal); err != nil {
		return nil, fmt.Errorf("min/max: %w", err)
	}
	cp.Min = minVal
	cp.Max = maxVal

	// Fetch raw values to detect numeric columns and compute stats.
	rows, err := exec.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL", col, table, col),
	)
	if err != nil {
		// Query failure — treat as non-numeric, return profile without stats.
		return cp, nil
	}
	defer rows.Close()

	var values []float64
	numeric := true
	for rows.Next() {
		var v float64
		if err := rows.Scan(&v); err != nil {
			// Column is not numeric.
			numeric = false
			break
		}
		values = append(values, v)
	}
	// Drain remaining rows (required to release the mock expectation).
	for rows.Next() {
	}
	if rows.Err() != nil && numeric {
		return nil, fmt.Errorf("fetch values: %w", rows.Err())
	}

	if !numeric || len(values) == 0 {
		return cp, nil
	}

	// Compute statistics using gonum.
	mean := stat.Mean(values, nil)
	stddev := stat.StdDev(values, nil)
	cp.Mean = &mean
	cp.StdDev = &stddev

	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	p25 := stat.Quantile(0.25, stat.Empirical, sorted, nil)
	p50 := stat.Quantile(0.50, stat.Empirical, sorted, nil)
	p75 := stat.Quantile(0.75, stat.Empirical, sorted, nil)
	p90 := stat.Quantile(0.90, stat.Empirical, sorted, nil)
	p99 := stat.Quantile(0.99, stat.Empirical, sorted, nil)

	cp.Percentiles = &Percentiles{
		P25: p25,
		P50: p50,
		P75: p75,
		P90: p90,
		P99: p99,
	}

	return cp, nil
}
