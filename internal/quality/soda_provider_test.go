package quality

import (
	"testing"
)

func TestSodaProvider_ParseOutput(t *testing.T) {
	output := []byte(`Soda Core 3.3.3
Sending anonymous usage statistics to Soda...
Scan summary:
3 checks passed.
1 check failed.
----
`)
	result, err := parseSodaOutput(output)
	if err != nil {
		t.Fatalf("parseSodaOutput: %v", err)
	}
	if result.Passed != 3 {
		t.Errorf("Passed: got %d, want 3", result.Passed)
	}
	if result.Failed != 1 {
		t.Errorf("Failed: got %d, want 1", result.Failed)
	}
}

func TestSodaProvider_ParseOutput_AllPass(t *testing.T) {
	output := []byte(`Soda Core 3.3.3
5 checks passed.
`)
	result, err := parseSodaOutput(output)
	if err != nil {
		t.Fatalf("parseSodaOutput: %v", err)
	}
	if result.Passed != 5 {
		t.Errorf("Passed: got %d, want 5", result.Passed)
	}
	if result.Failed != 0 {
		t.Errorf("Failed: got %d, want 0", result.Failed)
	}
}

func TestSodaProvider_ParseOutput_WithErrors(t *testing.T) {
	output := []byte(`Soda Core 3.3.3
2 checks passed.
1 check had error.
`)
	result, err := parseSodaOutput(output)
	if err != nil {
		t.Fatalf("parseSodaOutput: %v", err)
	}
	if result.Passed != 2 {
		t.Errorf("Passed: got %d, want 2", result.Passed)
	}
	if result.Errors != 1 {
		t.Errorf("Errors: got %d, want 1", result.Errors)
	}
}

func TestSodaProvider_ParseOutput_Empty(t *testing.T) {
	result, err := parseSodaOutput([]byte{})
	if err != nil {
		t.Fatalf("parseSodaOutput empty: %v", err)
	}
	if result.Passed != 0 || result.Failed != 0 {
		t.Errorf("empty output: expected zeros, got passed=%d failed=%d", result.Passed, result.Failed)
	}
}

func TestSodaProvider_ParseOutput_SingularCheck(t *testing.T) {
	// Test singular "check" (not "checks").
	output := []byte(`1 check passed.
1 check failed.
`)
	result, err := parseSodaOutput(output)
	if err != nil {
		t.Fatalf("parseSodaOutput: %v", err)
	}
	if result.Passed != 1 {
		t.Errorf("Passed: got %d, want 1", result.Passed)
	}
	if result.Failed != 1 {
		t.Errorf("Failed: got %d, want 1", result.Failed)
	}
}
