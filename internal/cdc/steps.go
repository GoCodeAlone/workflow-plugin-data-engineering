package cdc

import (
	"context"
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// startStep implements step.cdc_start.
// Looks up the cdc.source module by source_id and calls Connect to start the stream.
// If the stream is already running, this is a no-op (returns current status).
type startStep struct {
	name string
}

// NewStartStep creates a new step.cdc_start instance.
func NewStartStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &startStep{name: name}, nil
}

func (s *startStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	sourceID, _ := stringVal(config, "source_id")
	if sourceID == "" {
		return nil, fmt.Errorf("step.cdc_start %q: source_id is required", s.name)
	}

	provider, err := LookupSource(sourceID)
	if err != nil {
		// Source not running: try to start it from step config.
		cfg, cfgErr := parseSourceConfig(config)
		if cfgErr != nil {
			return nil, fmt.Errorf("step.cdc_start %q: %w (and no running module: %v)", s.name, cfgErr, err)
		}
		provider, cfgErr = newProvider(cfg)
		if cfgErr != nil {
			return nil, fmt.Errorf("step.cdc_start %q: create provider: %w", s.name, cfgErr)
		}
		if cfgErr = provider.Connect(ctx, cfg); cfgErr != nil {
			return nil, fmt.Errorf("step.cdc_start %q: connect: %w", s.name, cfgErr)
		}
		if cfgErr = RegisterSource(sourceID, provider); cfgErr != nil {
			_ = provider.Disconnect(ctx, sourceID)
			return nil, fmt.Errorf("step.cdc_start %q: register: %w", s.name, cfgErr)
		}
	}

	status, err := provider.Status(ctx, sourceID)
	if err != nil {
		return nil, fmt.Errorf("step.cdc_start %q: status: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"source_id": sourceID,
		"action":    "started",
		"state":     status.State,
	}}, nil
}

// stopStep implements step.cdc_stop.
// Looks up the cdc.source module and calls Disconnect, then deregisters it.
type stopStep struct {
	name string
}

// NewStopStep creates a new step.cdc_stop instance.
func NewStopStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &stopStep{name: name}, nil
}

func (s *stopStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	sourceID, _ := stringVal(config, "source_id")
	if sourceID == "" {
		return nil, fmt.Errorf("step.cdc_stop %q: source_id is required", s.name)
	}

	provider, err := LookupSource(sourceID)
	if err != nil {
		return nil, fmt.Errorf("step.cdc_stop %q: %w", s.name, err)
	}

	UnregisterSource(sourceID)
	if err := provider.Disconnect(ctx, sourceID); err != nil {
		return nil, fmt.Errorf("step.cdc_stop %q: disconnect: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"source_id": sourceID,
		"action":    "stopped",
	}}, nil
}

// statusStep implements step.cdc_status.
// Returns the current status of a running CDC stream.
type statusStep struct {
	name string
}

// NewStatusStep creates a new step.cdc_status instance.
func NewStatusStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &statusStep{name: name}, nil
}

func (s *statusStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	sourceID, _ := stringVal(config, "source_id")
	if sourceID == "" {
		return nil, fmt.Errorf("step.cdc_status %q: source_id is required", s.name)
	}

	provider, err := LookupSource(sourceID)
	if err != nil {
		return nil, fmt.Errorf("step.cdc_status %q: %w", s.name, err)
	}

	status, err := provider.Status(ctx, sourceID)
	if err != nil {
		return nil, fmt.Errorf("step.cdc_status %q: %w", s.name, err)
	}

	output := map[string]any{
		"source_id": status.SourceID,
		"state":     status.State,
		"provider":  status.Provider,
	}
	if status.LastEvent != "" {
		output["last_event"] = status.LastEvent
	}
	if status.Error != "" {
		output["error"] = status.Error
	}

	return &sdk.StepResult{Output: output}, nil
}

// snapshotStep implements step.cdc_snapshot.
// Triggers a full table snapshot on the running CDC stream.
type snapshotStep struct {
	name string
}

// NewSnapshotStep creates a new step.cdc_snapshot instance.
func NewSnapshotStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &snapshotStep{name: name}, nil
}

func (s *snapshotStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	sourceID, _ := stringVal(config, "source_id")
	if sourceID == "" {
		return nil, fmt.Errorf("step.cdc_snapshot %q: source_id is required", s.name)
	}

	var tables []string
	switch t := config["tables"].(type) {
	case []string:
		tables = t
	case []any:
		for _, v := range t {
			if sv, ok := v.(string); ok {
				tables = append(tables, sv)
			}
		}
	}

	provider, err := LookupSource(sourceID)
	if err != nil {
		return nil, fmt.Errorf("step.cdc_snapshot %q: %w", s.name, err)
	}

	if err := provider.Snapshot(ctx, sourceID, tables); err != nil {
		return nil, fmt.Errorf("step.cdc_snapshot %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"source_id": sourceID,
		"action":    "snapshot_triggered",
		"tables":    tables,
	}}, nil
}

// schemaHistoryStep implements step.cdc_schema_history.
// Returns DDL change history for a specific table.
type schemaHistoryStep struct {
	name string
}

// NewSchemaHistoryStep creates a new step.cdc_schema_history instance.
func NewSchemaHistoryStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &schemaHistoryStep{name: name}, nil
}

func (s *schemaHistoryStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	sourceID, _ := stringVal(config, "source_id")
	if sourceID == "" {
		return nil, fmt.Errorf("step.cdc_schema_history %q: source_id is required", s.name)
	}
	table, _ := stringVal(config, "table")
	if table == "" {
		return nil, fmt.Errorf("step.cdc_schema_history %q: table is required", s.name)
	}

	provider, err := LookupSource(sourceID)
	if err != nil {
		return nil, fmt.Errorf("step.cdc_schema_history %q: %w", s.name, err)
	}

	history, err := provider.SchemaHistory(ctx, sourceID, table)
	if err != nil {
		return nil, fmt.Errorf("step.cdc_schema_history %q: %w", s.name, err)
	}

	// Convert []SchemaVersion to []map[string]any for the pipeline output.
	historyMaps := make([]map[string]any, len(history))
	for i, sv := range history {
		historyMaps[i] = map[string]any{
			"table":      sv.Table,
			"version":    sv.Version,
			"ddl":        sv.DDL,
			"applied_at": sv.AppliedAt,
		}
	}

	return &sdk.StepResult{Output: map[string]any{
		"source_id": sourceID,
		"table":     table,
		"history":   historyMaps,
		"count":     len(history),
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
