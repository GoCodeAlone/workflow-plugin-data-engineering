package quality

import (
	"testing"
)

func TestDBTProvider_ParseOutput_StructuredLogs(t *testing.T) {
	// newline-delimited JSON with TestPass and TestFail events.
	output := []byte(`{"info":{"name":"TestPass","msg":"test_not_null_orders_id PASS"}}
{"info":{"name":"TestPass","msg":"test_unique_users_email PASS"}}
{"info":{"name":"TestFail","msg":"test_freshness_events FAIL"}}
`)
	result, err := parseDBTOutput(output)
	if err != nil {
		t.Fatalf("parseDBTOutput: %v", err)
	}
	if result.Passed != 2 {
		t.Errorf("Passed: got %d, want 2", result.Passed)
	}
	if result.Failed != 1 {
		t.Errorf("Failed: got %d, want 1", result.Failed)
	}
	if result.Errors != 0 {
		t.Errorf("Errors: got %d, want 0", result.Errors)
	}
}

func TestDBTProvider_ParseOutput_SummaryJSON(t *testing.T) {
	// Single-object summary JSON (simplified format).
	output := []byte(`{"passed":5,"failed":0,"errors":0,"skipped":1}`)
	result, err := parseDBTOutput(output)
	if err != nil {
		t.Fatalf("parseDBTOutput: %v", err)
	}
	if result.Passed != 5 {
		t.Errorf("Passed: got %d, want 5", result.Passed)
	}
	if result.Skipped != 1 {
		t.Errorf("Skipped: got %d, want 1", result.Skipped)
	}
}

func TestDBTProvider_ParseOutput_Empty(t *testing.T) {
	result, err := parseDBTOutput([]byte{})
	if err != nil {
		t.Fatalf("parseDBTOutput empty: %v", err)
	}
	if result.Passed != 0 || result.Failed != 0 {
		t.Errorf("empty output: expected zeros, got passed=%d failed=%d", result.Passed, result.Failed)
	}
}

func TestDBTProvider_ParseOutput_WithErrors(t *testing.T) {
	output := []byte(`{"info":{"name":"TestPass","msg":"check_a PASS"}}
{"info":{"name":"TestError","msg":"check_b ERROR"}}
`)
	result, err := parseDBTOutput(output)
	if err != nil {
		t.Fatalf("parseDBTOutput: %v", err)
	}
	if result.Passed != 1 {
		t.Errorf("Passed: got %d, want 1", result.Passed)
	}
	if result.Errors != 1 {
		t.Errorf("Errors: got %d, want 1", result.Errors)
	}
}

func TestDBTProvider_ParseOutput_InvalidLines(t *testing.T) {
	// Mix of valid JSON and garbage — garbage should be skipped.
	output := []byte(`not json at all
{"info":{"name":"TestPass","msg":"check_a PASS"}}
also not json
`)
	result, err := parseDBTOutput(output)
	if err != nil {
		t.Fatalf("parseDBTOutput: %v", err)
	}
	if result.Passed != 1 {
		t.Errorf("Passed: got %d, want 1", result.Passed)
	}
}
