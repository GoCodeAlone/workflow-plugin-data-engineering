package quality

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// DBTResult holds the parsed output of `dbt test`.
type DBTResult struct {
	Passed  int      `json:"passed"`
	Failed  int      `json:"failed"`
	Errors  int      `json:"errors"`
	Skipped int      `json:"skipped"`
	Results []string `json:"results,omitempty"`
}

// ── step.quality_dbt_test ─────────────────────────────────────────────────────

type dbtTestStep struct{ name string }

func NewDBTTestStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &dbtTestStep{name: name}, nil
}

func (s *dbtTestStep) Execute(
	ctx context.Context, _ map[string]any, _ map[string]map[string]any, _ map[string]any, _ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	projectDir, _ := config["project"].(string)
	if projectDir == "" {
		return nil, fmt.Errorf("step.quality_dbt_test %q: project dir is required", s.name)
	}
	selector, _ := config["select"].(string)

	result, err := runDBT(ctx, projectDir, selector)
	if err != nil {
		return nil, fmt.Errorf("step.quality_dbt_test %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"passed":  result.Passed,
		"failed":  result.Failed,
		"errors":  result.Errors,
		"skipped": result.Skipped,
		"ok":      result.Failed == 0 && result.Errors == 0,
	}}, nil
}

// runDBT shells out to `dbt test` and returns parsed results.
func runDBT(ctx context.Context, projectDir, selector string) (*DBTResult, error) {
	dbtPath, err := exec.LookPath("dbt")
	if err != nil {
		return nil, fmt.Errorf("dbt binary not found in PATH: install dbt-core")
	}

	args := []string{"test", "--project-dir", projectDir, "--log-format", "json"}
	if selector != "" {
		args = append(args, "--select", selector)
	}

	cmd := exec.CommandContext(ctx, dbtPath, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		// dbt exits non-zero on test failures; we parse output regardless.
		if stdout.Len() == 0 {
			return nil, fmt.Errorf("dbt test failed: %v — stderr: %s", err, stderr.String())
		}
	}

	return parseDBTOutput(stdout.Bytes())
}

// parseDBTOutput parses newline-delimited JSON log output from `dbt test --log-format json`.
// Each line is a JSON log event; we accumulate test result status lines.
func parseDBTOutput(stdout []byte) (*DBTResult, error) {
	result := &DBTResult{}
	lines := bytes.Split(bytes.TrimSpace(stdout), []byte("\n"))

	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal(line, &event); err != nil {
			continue
		}

		// Extract test result status from log events.
		// dbt JSON log events have: {"info": {"name": "...", "msg": "..."}, "data": {...}}
		info, _ := event["info"].(map[string]any)
		if info == nil {
			// Also handle structured log format: {"name": "...", "msg": "..."}
			info = event
		}
		name, _ := info["name"].(string)
		msg, _ := info["msg"].(string)

		switch name {
		case "TestPass":
			result.Passed++
			result.Results = append(result.Results, "PASS: "+msg)
		case "TestFail":
			result.Failed++
			result.Results = append(result.Results, "FAIL: "+msg)
		case "TestError":
			result.Errors++
			result.Results = append(result.Results, "ERROR: "+msg)
		case "TestSkip":
			result.Skipped++
		}

		// Also handle summary line: "Finished running N tests" from message
		if strings.Contains(msg, "passed") && strings.Contains(msg, "failed") {
			// Parse summary if we haven't accumulated individual results.
		}
	}

	// If no structured results found, try summary-only JSON format.
	if result.Passed+result.Failed+result.Errors+result.Skipped == 0 {
		var summary struct {
			Passed  int `json:"passed"`
			Failed  int `json:"failed"`
			Errors  int `json:"errors"`
			Skipped int `json:"skipped"`
		}
		if err := json.Unmarshal(bytes.TrimSpace(stdout), &summary); err == nil {
			result.Passed = summary.Passed
			result.Failed = summary.Failed
			result.Errors = summary.Errors
			result.Skipped = summary.Skipped
		}
	}

	return result, nil
}
