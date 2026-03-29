package quality

import (
	"testing"
)

func TestDetectAnomalies_ZScore(t *testing.T) {
	// Normal cluster [1..10] plus one clear outlier at 100.
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100}
	result := DetectAnomalies(values, "zscore", 2.0)
	if result.Method != "zscore" {
		t.Errorf("Method: got %q, want zscore", result.Method)
	}
	if result.SampleSize != len(values) {
		t.Errorf("SampleSize: got %d, want %d", result.SampleSize, len(values))
	}
	if result.Anomalies < 1 {
		t.Errorf("Anomalies: got %d, want >= 1 (outlier at 100)", result.Anomalies)
	}
	if result.Threshold != 2.0 {
		t.Errorf("Threshold: got %f, want 2.0", result.Threshold)
	}
}

func TestDetectAnomalies_ZScore_DefaultThreshold(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1000}
	result := DetectAnomalies(values, "zscore", 0)
	if result.Threshold != 3.0 {
		t.Errorf("default threshold: got %f, want 3.0", result.Threshold)
	}
	if result.Anomalies < 1 {
		t.Errorf("expected 1000 to be flagged as anomaly")
	}
}

func TestDetectAnomalies_IQR(t *testing.T) {
	// Tight cluster [1..10] plus outlier at 100.
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100}
	result := DetectAnomalies(values, "iqr", 1.5)
	if result.Method != "iqr" {
		t.Errorf("Method: got %q, want iqr", result.Method)
	}
	if result.Anomalies < 1 {
		t.Errorf("Anomalies: got %d, want >= 1 (outlier at 100)", result.Anomalies)
	}
}

func TestDetectAnomalies_IQR_DefaultThreshold(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1000}
	result := DetectAnomalies(values, "iqr", 0)
	if result.Threshold != 1.5 {
		t.Errorf("default IQR threshold: got %f, want 1.5", result.Threshold)
	}
}

func TestDetectAnomalies_NoAnomalies(t *testing.T) {
	// Uniform values — no outliers.
	values := []float64{5, 5, 5, 5, 5, 5, 5, 5}
	result := DetectAnomalies(values, "zscore", 3.0)
	if result.Anomalies != 0 {
		t.Errorf("Anomalies: got %d, want 0 for uniform data", result.Anomalies)
	}
}

func TestDetectAnomalies_AllAnomalies(t *testing.T) {
	// Extreme bimodal: half near 0, half near 1000 — with strict threshold, all deviate.
	values := []float64{0, 0, 0, 0, 1000, 1000, 1000, 1000}
	result := DetectAnomalies(values, "zscore", 0.1)
	if result.Anomalies == 0 {
		t.Error("expected anomalies with very strict threshold on bimodal distribution")
	}
}

func TestDetectAnomalies_EmptyInput(t *testing.T) {
	result := DetectAnomalies(nil, "zscore", 3.0)
	if result.Anomalies != 0 {
		t.Errorf("empty input: expected 0 anomalies, got %d", result.Anomalies)
	}
	if result.SampleSize != 0 {
		t.Errorf("empty input: expected SampleSize=0, got %d", result.SampleSize)
	}
}

func TestDetectAnomalies_SingleValue(t *testing.T) {
	result := DetectAnomalies([]float64{42}, "zscore", 3.0)
	if result.Anomalies != 0 {
		t.Errorf("single value: expected 0 anomalies (stddev=0), got %d", result.Anomalies)
	}
}

func TestDetectAnomalies_UnknownMethod_FallsBackToZScore(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5, 100}
	result := DetectAnomalies(values, "unknown_method", 2.0)
	if result.Method != "zscore" {
		t.Errorf("unknown method: expected fallback to zscore, got %q", result.Method)
	}
}
