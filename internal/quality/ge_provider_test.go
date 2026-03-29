package quality

import (
	"encoding/json"
	"testing"
)

func TestGEProvider_ParseOutput_Success(t *testing.T) {
	output, _ := json.Marshal(map[string]any{
		"success":    true,
		"evaluated":  10,
		"successful": 10,
		"failed":     0,
	})
	result, err := parseGEOutput(output)
	if err != nil {
		t.Fatalf("parseGEOutput: %v", err)
	}
	if !result.Success {
		t.Error("expected success=true")
	}
	if result.Evaluated != 10 {
		t.Errorf("Evaluated: got %d, want 10", result.Evaluated)
	}
	if result.Failed != 0 {
		t.Errorf("Failed: got %d, want 0", result.Failed)
	}
}

func TestGEProvider_ParseOutput_Failure(t *testing.T) {
	output, _ := json.Marshal(map[string]any{
		"success":    false,
		"evaluated":  5,
		"successful": 3,
		"failed":     2,
	})
	result, err := parseGEOutput(output)
	if err != nil {
		t.Fatalf("parseGEOutput: %v", err)
	}
	if result.Success {
		t.Error("expected success=false")
	}
	if result.Failed != 2 {
		t.Errorf("Failed: got %d, want 2", result.Failed)
	}
}

func TestGEProvider_ParseOutput_RunResults(t *testing.T) {
	// Structured format with run_results.
	output, _ := json.Marshal(map[string]any{
		"success": true,
		"run_results": map[string]any{
			"result_1": map[string]any{
				"validation_result": map[string]any{
					"statistics": map[string]any{
						"evaluated_expectations":  8,
						"successful_expectations": 7,
						"failed_expectations":     1,
					},
				},
			},
		},
	})
	result, err := parseGEOutput(output)
	if err != nil {
		t.Fatalf("parseGEOutput: %v", err)
	}
	if result.Evaluated != 8 {
		t.Errorf("Evaluated from run_results: got %d, want 8", result.Evaluated)
	}
	if result.Successful != 7 {
		t.Errorf("Successful from run_results: got %d, want 7", result.Successful)
	}
	if result.Failed != 1 {
		t.Errorf("Failed from run_results: got %d, want 1", result.Failed)
	}
}

func TestGEProvider_ParseOutput_Empty(t *testing.T) {
	result, err := parseGEOutput([]byte{})
	if err != nil {
		t.Fatalf("parseGEOutput empty: %v", err)
	}
	if result.Evaluated != 0 {
		t.Errorf("empty: expected Evaluated=0, got %d", result.Evaluated)
	}
}

func TestGEProvider_ParseOutput_InvalidJSON(t *testing.T) {
	_, err := parseGEOutput([]byte(`{invalid json`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}
