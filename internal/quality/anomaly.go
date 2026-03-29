package quality

import (
	"math"
	"sort"

	"gonum.org/v1/gonum/stat"
)

// AnomalyResult holds the outcome of anomaly detection for one column.
type AnomalyResult struct {
	Column     string  `json:"column"`
	Method     string  `json:"method"` // zscore, iqr
	Anomalies  int     `json:"anomalies"`
	Threshold  float64 `json:"threshold"`
	SampleSize int     `json:"sampleSize"`
}

// DetectAnomalies detects anomalies in the given slice of float64 values.
// method: "zscore" (flag |z| > threshold) or "iqr" (flag below Q1-1.5*IQR or above Q3+1.5*IQR).
// If threshold <= 0, defaults to 3.0 for zscore and 1.5 for iqr.
func DetectAnomalies(values []float64, method string, threshold float64) *AnomalyResult {
	if threshold <= 0 {
		switch method {
		case "iqr":
			threshold = 1.5
		default:
			threshold = 3.0
		}
	}

	result := &AnomalyResult{
		Method:     method,
		Threshold:  threshold,
		SampleSize: len(values),
	}

	if len(values) == 0 {
		return result
	}

	switch method {
	case "iqr":
		result.Anomalies = detectIQR(values, threshold)
	default: // zscore
		result.Anomalies = detectZScore(values, threshold)
		result.Method = "zscore"
	}

	return result
}

func detectZScore(values []float64, threshold float64) int {
	mean := stat.Mean(values, nil)
	stddev := stat.StdDev(values, nil)
	if stddev == 0 {
		return 0
	}

	count := 0
	for _, v := range values {
		z := math.Abs((v - mean) / stddev)
		if z > threshold {
			count++
		}
	}
	return count
}

func detectIQR(values []float64, threshold float64) int {
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	q1 := stat.Quantile(0.25, stat.Empirical, sorted, nil)
	q3 := stat.Quantile(0.75, stat.Empirical, sorted, nil)
	iqr := q3 - q1

	lower := q1 - threshold*iqr
	upper := q3 + threshold*iqr

	count := 0
	for _, v := range values {
		if v < lower || v > upper {
			count++
		}
	}
	return count
}
