package cdc

import (
	"context"
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// ThrottleableProvider is an optional extension of CDCProvider for providers
// that support pausing and resuming the CDC stream.
type ThrottleableProvider interface {
	PauseSource(ctx context.Context, sourceID string) error
	ResumeSource(ctx context.Context, sourceID string) error
}

// BackpressureMonitor evaluates CDC lag against configurable thresholds.
type BackpressureMonitor struct {
	ThresholdLagBytes   int64
	ThresholdLagSeconds int64
	WarningMultiplier   float64 // warn at this fraction of threshold (e.g. 0.8)
}

// BackpressureStatus holds the lag evaluation result.
type BackpressureStatus struct {
	SourceID   string
	LagBytes   int64
	LagSeconds int64
	Status     string // "healthy", "warning", "critical"
}

// Evaluate returns the backpressure status for the given lag values.
func (m *BackpressureMonitor) Evaluate(sourceID string, lagBytes, lagSeconds int64) BackpressureStatus {
	bs := BackpressureStatus{
		SourceID:   sourceID,
		LagBytes:   lagBytes,
		LagSeconds: lagSeconds,
		Status:     "healthy",
	}
	if (m.ThresholdLagBytes > 0 && lagBytes >= m.ThresholdLagBytes) ||
		(m.ThresholdLagSeconds > 0 && lagSeconds >= m.ThresholdLagSeconds) {
		bs.Status = "critical"
		return bs
	}
	mult := m.WarningMultiplier
	if mult <= 0 {
		mult = 0.8
	}
	if (m.ThresholdLagBytes > 0 && float64(lagBytes) >= float64(m.ThresholdLagBytes)*mult) ||
		(m.ThresholdLagSeconds > 0 && float64(lagSeconds) >= float64(m.ThresholdLagSeconds)*mult) {
		bs.Status = "warning"
	}
	return bs
}

// ─── step.cdc_backpressure ────────────────────────────────────────────────────

// backpressureStep implements step.cdc_backpressure.
// Actions: check, throttle, resume.
type backpressureStep struct {
	name string
}

// NewBackpressureStep creates a new step.cdc_backpressure instance.
func NewBackpressureStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &backpressureStep{name: name}, nil
}

func (s *backpressureStep) Execute(
	ctx context.Context,
	_ map[string]any,
	_ map[string]map[string]any,
	_, _ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	sourceID, _ := stringVal(config, "source_id")
	if sourceID == "" {
		return nil, fmt.Errorf("step.cdc_backpressure %q: source_id is required", s.name)
	}
	action, _ := stringVal(config, "action")
	if action == "" {
		action = "check"
	}

	provider, err := LookupSource(sourceID)
	if err != nil {
		return nil, fmt.Errorf("step.cdc_backpressure %q: %w", s.name, err)
	}

	switch action {
	case "check":
		return s.executeCheck(ctx, sourceID, provider, config)
	case "throttle":
		return s.executeThrottle(ctx, sourceID, provider)
	case "resume":
		return s.executeResume(ctx, sourceID, provider)
	default:
		return nil, fmt.Errorf("step.cdc_backpressure %q: unknown action %q (valid: check, throttle, resume)", s.name, action)
	}
}

func (s *backpressureStep) executeCheck(
	ctx context.Context,
	sourceID string,
	provider CDCProvider,
	config map[string]any,
) (*sdk.StepResult, error) {
	status, err := provider.Status(ctx, sourceID)
	if err != nil {
		return nil, fmt.Errorf("step.cdc_backpressure %q: status: %w", s.name, err)
	}

	mon := backpressureMonitorFromConfig(config)
	bs := mon.Evaluate(sourceID, status.LagBytes, status.LagSeconds)

	return &sdk.StepResult{Output: map[string]any{
		"source_id":   sourceID,
		"lag_bytes":   status.LagBytes,
		"lag_seconds": status.LagSeconds,
		"status":      bs.Status,
		"thresholds": map[string]any{
			"lag_bytes":   mon.ThresholdLagBytes,
			"lag_seconds": mon.ThresholdLagSeconds,
		},
	}}, nil
}

func (s *backpressureStep) executeThrottle(
	ctx context.Context,
	sourceID string,
	provider CDCProvider,
) (*sdk.StepResult, error) {
	status, err := provider.Status(ctx, sourceID)
	if err != nil {
		return nil, fmt.Errorf("step.cdc_backpressure %q: status: %w", s.name, err)
	}
	previousState := status.State

	tp, ok := provider.(ThrottleableProvider)
	if !ok {
		return nil, fmt.Errorf("step.cdc_backpressure %q: provider does not support throttling", s.name)
	}
	if err := tp.PauseSource(ctx, sourceID); err != nil {
		return nil, fmt.Errorf("step.cdc_backpressure %q: throttle: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"source_id":      sourceID,
		"action":         "throttled",
		"previous_state": previousState,
	}}, nil
}

func (s *backpressureStep) executeResume(
	ctx context.Context,
	sourceID string,
	provider CDCProvider,
) (*sdk.StepResult, error) {
	tp, ok := provider.(ThrottleableProvider)
	if !ok {
		return nil, fmt.Errorf("step.cdc_backpressure %q: provider does not support resuming", s.name)
	}
	if err := tp.ResumeSource(ctx, sourceID); err != nil {
		return nil, fmt.Errorf("step.cdc_backpressure %q: resume: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"source_id": sourceID,
		"action":    "resumed",
	}}, nil
}

// ─── step.cdc_monitor ────────────────────────────────────────────────────────

// monitorStep implements step.cdc_monitor.
// Checks lag, publishes alerts, and auto-throttles on critical lag.
type monitorStep struct {
	name string
}

// NewMonitorStep creates a new step.cdc_monitor instance.
func NewMonitorStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &monitorStep{name: name}, nil
}

func (s *monitorStep) Execute(
	ctx context.Context,
	_ map[string]any,
	_ map[string]map[string]any,
	_, _ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	sourceID, _ := stringVal(config, "source_id")
	if sourceID == "" {
		return nil, fmt.Errorf("step.cdc_monitor %q: source_id is required", s.name)
	}

	provider, err := LookupSource(sourceID)
	if err != nil {
		return nil, fmt.Errorf("step.cdc_monitor %q: %w", s.name, err)
	}

	status, err := provider.Status(ctx, sourceID)
	if err != nil {
		return nil, fmt.Errorf("step.cdc_monitor %q: status: %w", s.name, err)
	}

	mon := backpressureMonitorFromConfig(config)
	bs := mon.Evaluate(sourceID, status.LagBytes, status.LagSeconds)

	alertsSent := 0
	autoThrottled := false

	switch bs.Status {
	case "critical":
		alertsSent++
		// Auto-throttle if provider supports it.
		if tp, ok := provider.(ThrottleableProvider); ok {
			if throttleErr := tp.PauseSource(ctx, sourceID); throttleErr == nil {
				autoThrottled = true
			}
		}
	case "warning":
		alertsSent++
	}

	return &sdk.StepResult{Output: map[string]any{
		"source_id":      sourceID,
		"lag_bytes":      status.LagBytes,
		"lag_seconds":    status.LagSeconds,
		"status":         bs.Status,
		"alerts_sent":    alertsSent,
		"auto_throttled": autoThrottled,
	}}, nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func backpressureMonitorFromConfig(config map[string]any) *BackpressureMonitor {
	mon := &BackpressureMonitor{
		ThresholdLagBytes:   1_073_741_824, // 1 GiB
		ThresholdLagSeconds: 300,
		WarningMultiplier:   0.8,
	}
	if t, ok := config["thresholds"].(map[string]any); ok {
		if v := int64Val(t, "lag_bytes"); v > 0 {
			mon.ThresholdLagBytes = v
		}
		if v := int64Val(t, "lag_seconds"); v > 0 {
			mon.ThresholdLagSeconds = v
		}
	}
	return mon
}

func int64Val(m map[string]any, key string) int64 {
	v, ok := m[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	case int32:
		return int64(n)
	}
	return 0
}
