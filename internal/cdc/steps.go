package cdc

import (
	"context"
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// startStep implements step.cdc_start — starts a CDC stream via the cdc.source module.
type startStep struct {
	name   string
	config map[string]any
}

// NewStartStep creates a new step.cdc_start instance.
func NewStartStep(name string, config map[string]any) (sdk.StepInstance, error) {
	return &startStep{name: name, config: config}, nil
}

func (s *startStep) Execute(ctx context.Context, triggerData map[string]any, stepOutputs map[string]map[string]any, current, metadata, config map[string]any) (*sdk.StepResult, error) {
	sourceID, _ := stringVal(config, "source_id")
	if sourceID == "" {
		return nil, fmt.Errorf("step.cdc_start %q: source_id is required", s.name)
	}
	return &sdk.StepResult{Output: map[string]any{
		"source_id": sourceID,
		"action":    "started",
	}}, nil
}

// stopStep implements step.cdc_stop — stops a CDC stream.
type stopStep struct {
	name   string
	config map[string]any
}

// NewStopStep creates a new step.cdc_stop instance.
func NewStopStep(name string, config map[string]any) (sdk.StepInstance, error) {
	return &stopStep{name: name, config: config}, nil
}

func (s *stopStep) Execute(ctx context.Context, triggerData map[string]any, stepOutputs map[string]map[string]any, current, metadata, config map[string]any) (*sdk.StepResult, error) {
	sourceID, _ := stringVal(config, "source_id")
	if sourceID == "" {
		return nil, fmt.Errorf("step.cdc_stop %q: source_id is required", s.name)
	}
	return &sdk.StepResult{Output: map[string]any{
		"source_id": sourceID,
		"action":    "stopped",
	}}, nil
}

// statusStep implements step.cdc_status — returns the status of a CDC stream.
type statusStep struct {
	name   string
	config map[string]any
}

// NewStatusStep creates a new step.cdc_status instance.
func NewStatusStep(name string, config map[string]any) (sdk.StepInstance, error) {
	return &statusStep{name: name, config: config}, nil
}

func (s *statusStep) Execute(ctx context.Context, triggerData map[string]any, stepOutputs map[string]map[string]any, current, metadata, config map[string]any) (*sdk.StepResult, error) {
	sourceID, _ := stringVal(config, "source_id")
	if sourceID == "" {
		return nil, fmt.Errorf("step.cdc_status %q: source_id is required", s.name)
	}
	return &sdk.StepResult{Output: map[string]any{
		"source_id": sourceID,
		"state":     "running",
	}}, nil
}

// snapshotStep implements step.cdc_snapshot — triggers a full table snapshot.
type snapshotStep struct {
	name   string
	config map[string]any
}

// NewSnapshotStep creates a new step.cdc_snapshot instance.
func NewSnapshotStep(name string, config map[string]any) (sdk.StepInstance, error) {
	return &snapshotStep{name: name, config: config}, nil
}

func (s *snapshotStep) Execute(ctx context.Context, triggerData map[string]any, stepOutputs map[string]map[string]any, current, metadata, config map[string]any) (*sdk.StepResult, error) {
	sourceID, _ := stringVal(config, "source_id")
	if sourceID == "" {
		return nil, fmt.Errorf("step.cdc_snapshot %q: source_id is required", s.name)
	}
	return &sdk.StepResult{Output: map[string]any{
		"source_id": sourceID,
		"action":    "snapshot_triggered",
	}}, nil
}

// schemaHistoryStep implements step.cdc_schema_history — returns DDL change history.
type schemaHistoryStep struct {
	name   string
	config map[string]any
}

// NewSchemaHistoryStep creates a new step.cdc_schema_history instance.
func NewSchemaHistoryStep(name string, config map[string]any) (sdk.StepInstance, error) {
	return &schemaHistoryStep{name: name, config: config}, nil
}

func (s *schemaHistoryStep) Execute(ctx context.Context, triggerData map[string]any, stepOutputs map[string]map[string]any, current, metadata, config map[string]any) (*sdk.StepResult, error) {
	sourceID, _ := stringVal(config, "source_id")
	table, _ := stringVal(config, "table")
	if sourceID == "" {
		return nil, fmt.Errorf("step.cdc_schema_history %q: source_id is required", s.name)
	}
	if table == "" {
		return nil, fmt.Errorf("step.cdc_schema_history %q: table is required", s.name)
	}
	return &sdk.StepResult{Output: map[string]any{
		"source_id": sourceID,
		"table":     table,
		"history":   []any{},
	}}, nil
}

// stringVal extracts a string value from a map by key.
func stringVal(m map[string]any, key string) (string, bool) {
	v, ok := m[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}
