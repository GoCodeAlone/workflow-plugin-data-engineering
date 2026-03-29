package quality

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// GEResult holds the parsed output of `great_expectations checkpoint run`.
type GEResult struct {
	Success    bool   `json:"success"`
	Evaluated  int    `json:"evaluated"`
	Successful int    `json:"successful"`
	Failed     int    `json:"failed"`
}

// ── step.quality_ge_validate ──────────────────────────────────────────────────

type geValidateStep struct{ name string }

func NewGEValidateStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &geValidateStep{name: name}, nil
}

func (s *geValidateStep) Execute(
	ctx context.Context, _ map[string]any, _ map[string]map[string]any, _ map[string]any, _ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	checkpoint, _ := config["checkpoint"].(string)
	if checkpoint == "" {
		return nil, fmt.Errorf("step.quality_ge_validate %q: checkpoint is required", s.name)
	}

	result, err := runGE(ctx, checkpoint)
	if err != nil {
		return nil, fmt.Errorf("step.quality_ge_validate %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"success":    result.Success,
		"evaluated":  result.Evaluated,
		"successful": result.Successful,
		"failed":     result.Failed,
	}}, nil
}

// runGE shells out to `great_expectations checkpoint run`.
func runGE(ctx context.Context, checkpoint string) (*GEResult, error) {
	gePath, err := exec.LookPath("great_expectations")
	if err != nil {
		return nil, fmt.Errorf("great_expectations binary not found in PATH: install great-expectations")
	}

	cmd := exec.CommandContext(ctx, gePath, "--v3-api", "checkpoint", "run", checkpoint)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// GE exits non-zero on validation failure; parse output regardless.
	_ = cmd.Run()

	return parseGEOutput(stdout.Bytes())
}

// parseGEOutput parses JSON output from `great_expectations checkpoint run`.
// GE outputs a JSON summary to stdout when run in non-interactive mode.
func parseGEOutput(stdout []byte) (*GEResult, error) {
	stdout = bytes.TrimSpace(stdout)
	if len(stdout) == 0 {
		return &GEResult{}, nil
	}

	// Try to parse as the structured checkpoint result.
	var raw struct {
		Success bool `json:"success"`
		RunResults map[string]struct {
			ValidationResult struct {
				Statistics struct {
					EvaluatedExpectations  int `json:"evaluated_expectations"`
					SuccessfulExpectations int `json:"successful_expectations"`
					FailedExpectations     int `json:"failed_expectations"`
				} `json:"statistics"`
			} `json:"validation_result"`
		} `json:"run_results"`
		// Flat summary format (simplified CLI output).
		Evaluated  int `json:"evaluated"`
		Successful int `json:"successful"`
		Failed     int `json:"failed"`
	}

	if err := json.Unmarshal(stdout, &raw); err != nil {
		return nil, fmt.Errorf("parse great_expectations output: %w", err)
	}

	result := &GEResult{
		Success:    raw.Success,
		Evaluated:  raw.Evaluated,
		Successful: raw.Successful,
		Failed:     raw.Failed,
	}

	// Accumulate from run_results if present.
	for _, rr := range raw.RunResults {
		stats := rr.ValidationResult.Statistics
		result.Evaluated += stats.EvaluatedExpectations
		result.Successful += stats.SuccessfulExpectations
		result.Failed += stats.FailedExpectations
	}

	return result, nil
}
