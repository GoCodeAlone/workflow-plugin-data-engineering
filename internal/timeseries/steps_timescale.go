package timeseries

import (
	"context"
	"fmt"
	"strings"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// tsContinuousQueryStep implements step.ts_continuous_query.
type tsContinuousQueryStep struct {
	name string
}

// NewTSContinuousQueryStep creates a new step.ts_continuous_query instance.
func NewTSContinuousQueryStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &tsContinuousQueryStep{name: name}, nil
}

func (s *tsContinuousQueryStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	module, _ := stringValTS(config, "module")
	if module == "" {
		return nil, fmt.Errorf("step.ts_continuous_query %q: module is required", s.name)
	}
	viewName, _ := stringValTS(config, "viewName")
	if viewName == "" {
		return nil, fmt.Errorf("step.ts_continuous_query %q: viewName is required", s.name)
	}

	action, _ := stringValTS(config, "action")
	if action == "" {
		action = "create"
	}

	writer, err := Lookup(module)
	if err != nil {
		return nil, fmt.Errorf("step.ts_continuous_query %q: %w", s.name, err)
	}
	tsmod, ok := writer.(*TimescaleModule)
	if !ok {
		return nil, fmt.Errorf("step.ts_continuous_query %q: module %q is not a timeseries.timescaledb module", s.name, module)
	}

	switch action {
	case "create":
		return s.create(ctx, tsmod, viewName, config)
	case "refresh":
		return s.refresh(ctx, tsmod, viewName)
	case "drop":
		return s.drop(ctx, tsmod, viewName)
	default:
		return nil, fmt.Errorf("step.ts_continuous_query %q: unknown action %q (create|refresh|drop)", s.name, action)
	}
}

func (s *tsContinuousQueryStep) create(ctx context.Context, tsmod *TimescaleModule, viewName string, config map[string]any) (*sdk.StepResult, error) {
	query, _ := stringValTS(config, "query")
	if query == "" {
		return nil, fmt.Errorf("step.ts_continuous_query %q: query is required for create action", s.name)
	}
	refreshInterval, _ := stringValTS(config, "refreshInterval")
	startOffset, _ := stringValTS(config, "startOffset")
	endOffset, _ := stringValTS(config, "endOffset")
	materialized := true
	if v, ok := config["materialized"].(bool); ok {
		materialized = v
	}

	db := tsmod.DB()
	if db == nil {
		return nil, fmt.Errorf("step.ts_continuous_query %q: module not started", s.name)
	}

	var createSQL strings.Builder
	if materialized {
		fmt.Fprintf(&createSQL, "CREATE MATERIALIZED VIEW IF NOT EXISTS %q\n", sanitizeIdentifier(viewName))
		fmt.Fprintf(&createSQL, "WITH (timescaledb.continuous) AS\n")
	} else {
		fmt.Fprintf(&createSQL, "CREATE VIEW IF NOT EXISTS %q AS\n", sanitizeIdentifier(viewName))
	}
	createSQL.WriteString(query)

	if _, err := db.ExecContext(ctx, createSQL.String()); err != nil {
		return nil, fmt.Errorf("step.ts_continuous_query %q: create view %q: %w", s.name, viewName, err)
	}

	if materialized && refreshInterval != "" {
		policy := buildRefreshPolicy(viewName, startOffset, endOffset, refreshInterval)
		if _, err := db.ExecContext(ctx, policy); err != nil {
			return nil, fmt.Errorf("step.ts_continuous_query %q: add refresh policy for %q: %w", s.name, viewName, err)
		}
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":          "created",
		"viewName":        viewName,
		"refreshInterval": refreshInterval,
	}}, nil
}

func (s *tsContinuousQueryStep) refresh(ctx context.Context, tsmod *TimescaleModule, viewName string) (*sdk.StepResult, error) {
	db := tsmod.DB()
	if db == nil {
		return nil, fmt.Errorf("step.ts_continuous_query %q: module not started", s.name)
	}
	_, err := db.ExecContext(ctx,
		fmt.Sprintf(`CALL refresh_continuous_aggregate('%s', NULL, NULL)`, sanitizeIdentifier(viewName)))
	if err != nil {
		return nil, fmt.Errorf("step.ts_continuous_query %q: refresh %q: %w", s.name, viewName, err)
	}
	return &sdk.StepResult{Output: map[string]any{
		"status":   "refreshed",
		"viewName": viewName,
	}}, nil
}

func (s *tsContinuousQueryStep) drop(ctx context.Context, tsmod *TimescaleModule, viewName string) (*sdk.StepResult, error) {
	db := tsmod.DB()
	if db == nil {
		return nil, fmt.Errorf("step.ts_continuous_query %q: module not started", s.name)
	}
	_, err := db.ExecContext(ctx,
		fmt.Sprintf(`DROP MATERIALIZED VIEW IF EXISTS %q`, sanitizeIdentifier(viewName)))
	if err != nil {
		return nil, fmt.Errorf("step.ts_continuous_query %q: drop %q: %w", s.name, viewName, err)
	}
	return &sdk.StepResult{Output: map[string]any{
		"status":   "dropped",
		"viewName": viewName,
	}}, nil
}

func buildRefreshPolicy(viewName, startOffset, endOffset, refreshInterval string) string {
	start := toPostgresInterval(startOffset)
	if start == "" {
		start = "NULL"
	} else {
		start = fmt.Sprintf("INTERVAL '%s'", start)
	}
	end := toPostgresInterval(endOffset)
	if end == "" {
		end = "NULL"
	} else {
		end = fmt.Sprintf("INTERVAL '%s'", end)
	}
	ri := toPostgresInterval(refreshInterval)
	if ri == "" {
		ri = "1 hour"
	}
	return fmt.Sprintf(
		`SELECT add_continuous_aggregate_policy('%s', start_offset => %s, end_offset => %s, schedule_interval => INTERVAL '%s')`,
		sanitizeIdentifier(viewName), start, end, ri,
	)
}
