package quality

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// SodaResult holds the parsed output of `soda scan`.
type SodaResult struct {
	Passed int      `json:"passed"`
	Failed int      `json:"failed"`
	Errors int      `json:"errors"`
	Lines  []string `json:"lines,omitempty"`
}

// ── step.quality_soda_check ───────────────────────────────────────────────────

type sodaCheckStep struct{ name string }

func NewSodaCheckStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &sodaCheckStep{name: name}, nil
}

func (s *sodaCheckStep) Execute(
	ctx context.Context, _ map[string]any, _ map[string]map[string]any, _ map[string]any, _ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	datasource, _ := config["datasource"].(string)
	configFile, _ := config["config"].(string)
	checksFile, _ := config["checks_file"].(string)

	if datasource == "" || configFile == "" || checksFile == "" {
		return nil, fmt.Errorf("step.quality_soda_check %q: datasource, config, and checks_file are required", s.name)
	}

	result, err := runSoda(ctx, datasource, configFile, checksFile)
	if err != nil {
		return nil, fmt.Errorf("step.quality_soda_check %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"passed": result.Passed,
		"failed": result.Failed,
		"errors": result.Errors,
		"ok":     result.Failed == 0 && result.Errors == 0,
	}}, nil
}

// runSoda shells out to `soda scan`.
func runSoda(ctx context.Context, datasource, configFile, checksFile string) (*SodaResult, error) {
	sodaPath, err := exec.LookPath("soda")
	if err != nil {
		return nil, fmt.Errorf("soda binary not found in PATH: install soda-core")
	}

	cmd := exec.CommandContext(ctx, sodaPath, "scan",
		"-d", datasource,
		"-c", configFile,
		checksFile,
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// soda exits non-zero on check failures; parse output regardless.
	_ = cmd.Run()

	return parseSodaOutput(stdout.Bytes())
}

// sodaPassRe matches lines like "2 checks passed" or "1 check passed".
var sodaPassRe = regexp.MustCompile(`(?i)(\d+)\s+checks?\s+passed`)

// sodaFailRe matches lines like "1 check failed" or "3 checks failed".
var sodaFailRe = regexp.MustCompile(`(?i)(\d+)\s+checks?\s+failed`)

// sodaErrRe matches lines like "1 check error".
var sodaErrRe = regexp.MustCompile(`(?i)(\d+)\s+checks?\s+(?:had )?error`)

// parseSodaOutput parses text output from `soda scan`.
func parseSodaOutput(stdout []byte) (*SodaResult, error) {
	result := &SodaResult{}
	lines := bytes.Split(stdout, []byte("\n"))

	for _, line := range lines {
		s := string(bytes.TrimSpace(line))
		if s == "" {
			continue
		}
		result.Lines = append(result.Lines, s)

		if m := sodaPassRe.FindStringSubmatch(s); m != nil {
			if n, err := strconv.Atoi(m[1]); err == nil {
				result.Passed += n
			}
		}
		if m := sodaFailRe.FindStringSubmatch(s); m != nil {
			if n, err := strconv.Atoi(m[1]); err == nil {
				result.Failed += n
			}
		}
		if m := sodaErrRe.FindStringSubmatch(s); m != nil {
			if n, err := strconv.Atoi(m[1]); err == nil {
				result.Errors += n
			}
		}
	}

	return result, nil
}
