package timeseries

import (
	"context"
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// druidIngestStep implements step.ts_druid_ingest — submits a Kafka supervisor spec.
type druidIngestStep struct {
	name string
}

// NewDruidIngestStep creates a new step.ts_druid_ingest instance.
func NewDruidIngestStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &druidIngestStep{name: name}, nil
}

func (s *druidIngestStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	moduleName, _ := stringValTS(config, "module")
	if moduleName == "" {
		return nil, fmt.Errorf("step.ts_druid_ingest %q: module is required", s.name)
	}

	spec, ok := config["spec"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("step.ts_druid_ingest %q: spec is required", s.name)
	}

	writer, err := Lookup(moduleName)
	if err != nil {
		return nil, fmt.Errorf("step.ts_druid_ingest %q: %w", s.name, err)
	}
	dm, ok := writer.(*DruidModule)
	if !ok {
		return nil, fmt.Errorf("step.ts_druid_ingest %q: module %q is not a timeseries.druid module", s.name, moduleName)
	}

	status, err := dm.Client().SubmitSupervisor(ctx, spec)
	if err != nil {
		return nil, fmt.Errorf("step.ts_druid_ingest %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":       "submitted",
		"supervisorId": status.ID,
		"state":        status.State,
	}}, nil
}

// druidQueryStep implements step.ts_druid_query — executes SQL or native JSON queries.
type druidQueryStep struct {
	name string
}

// NewDruidQueryStep creates a new step.ts_druid_query instance.
func NewDruidQueryStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &druidQueryStep{name: name}, nil
}

func (s *druidQueryStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	moduleName, _ := stringValTS(config, "module")
	if moduleName == "" {
		return nil, fmt.Errorf("step.ts_druid_query %q: module is required", s.name)
	}
	query, _ := stringValTS(config, "query")
	queryType, _ := stringValTS(config, "queryType")
	if queryType == "" {
		queryType = "sql"
	}

	writer, err := Lookup(moduleName)
	if err != nil {
		return nil, fmt.Errorf("step.ts_druid_query %q: %w", s.name, err)
	}
	dm, ok := writer.(*DruidModule)
	if !ok {
		return nil, fmt.Errorf("step.ts_druid_query %q: module %q is not a timeseries.druid module", s.name, moduleName)
	}

	var result *QueryResult
	switch queryType {
	case "sql":
		if query == "" {
			return nil, fmt.Errorf("step.ts_druid_query %q: query is required for sql queryType", s.name)
		}
		var params []any
		if raw, ok := config["params"].([]any); ok {
			params = raw
		}
		result, err = dm.Client().SQLQuery(ctx, query, params)
	case "native":
		nativeQuery, ok := config["query"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("step.ts_druid_query %q: query must be an object for native queryType", s.name)
		}
		result, err = dm.Client().NativeQuery(ctx, nativeQuery)
	default:
		return nil, fmt.Errorf("step.ts_druid_query %q: unknown queryType %q (use sql or native)", s.name, queryType)
	}
	if err != nil {
		return nil, fmt.Errorf("step.ts_druid_query %q: %w", s.name, err)
	}

	rows := result.Rows
	if rows == nil {
		rows = []map[string]any{}
	}
	return &sdk.StepResult{Output: map[string]any{
		"rows":  rows,
		"count": len(rows),
	}}, nil
}

// druidDatasourceStep implements step.ts_druid_datasource — manages datasources.
type druidDatasourceStep struct {
	name string
}

// NewDruidDatasourceStep creates a new step.ts_druid_datasource instance.
func NewDruidDatasourceStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &druidDatasourceStep{name: name}, nil
}

func (s *druidDatasourceStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	moduleName, _ := stringValTS(config, "module")
	if moduleName == "" {
		return nil, fmt.Errorf("step.ts_druid_datasource %q: module is required", s.name)
	}
	action, _ := stringValTS(config, "action")
	if action == "" {
		action = "list"
	}

	writer, err := Lookup(moduleName)
	if err != nil {
		return nil, fmt.Errorf("step.ts_druid_datasource %q: %w", s.name, err)
	}
	dm, ok := writer.(*DruidModule)
	if !ok {
		return nil, fmt.Errorf("step.ts_druid_datasource %q: module %q is not a timeseries.druid module", s.name, moduleName)
	}

	switch action {
	case "list":
		sources, err := dm.Client().ListDatasources(ctx)
		if err != nil {
			return nil, fmt.Errorf("step.ts_druid_datasource %q: %w", s.name, err)
		}
		return &sdk.StepResult{Output: map[string]any{
			"datasources": sources,
			"count":       len(sources),
		}}, nil

	case "get":
		datasource, _ := stringValTS(config, "datasource")
		if datasource == "" {
			return nil, fmt.Errorf("step.ts_druid_datasource %q: datasource is required for action=get", s.name)
		}
		info, err := dm.Client().GetDatasource(ctx, datasource)
		if err != nil {
			return nil, fmt.Errorf("step.ts_druid_datasource %q: %w", s.name, err)
		}
		return &sdk.StepResult{Output: map[string]any{
			"datasource": info.Name,
			"properties": info.Properties,
		}}, nil

	case "disable":
		datasource, _ := stringValTS(config, "datasource")
		if datasource == "" {
			return nil, fmt.Errorf("step.ts_druid_datasource %q: datasource is required for action=disable", s.name)
		}
		if err := dm.Client().DisableDatasource(ctx, datasource); err != nil {
			return nil, fmt.Errorf("step.ts_druid_datasource %q: %w", s.name, err)
		}
		return &sdk.StepResult{Output: map[string]any{
			"status":     "disabled",
			"datasource": datasource,
		}}, nil

	default:
		return nil, fmt.Errorf("step.ts_druid_datasource %q: unknown action %q (use list, get, or disable)", s.name, action)
	}
}

// druidCompactStep implements step.ts_druid_compact — triggers manual compaction.
type druidCompactStep struct {
	name string
}

// NewDruidCompactStep creates a new step.ts_druid_compact instance.
func NewDruidCompactStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &druidCompactStep{name: name}, nil
}

func (s *druidCompactStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	moduleName, _ := stringValTS(config, "module")
	if moduleName == "" {
		return nil, fmt.Errorf("step.ts_druid_compact %q: module is required", s.name)
	}
	datasource, _ := stringValTS(config, "datasource")
	if datasource == "" {
		return nil, fmt.Errorf("step.ts_druid_compact %q: datasource is required", s.name)
	}

	writer, err := Lookup(moduleName)
	if err != nil {
		return nil, fmt.Errorf("step.ts_druid_compact %q: %w", s.name, err)
	}
	dm, ok := writer.(*DruidModule)
	if !ok {
		return nil, fmt.Errorf("step.ts_druid_compact %q: module %q is not a timeseries.druid module", s.name, moduleName)
	}

	var compCfg CompactionConfig
	if v, ok := config["targetCompactionSizeBytes"].(int64); ok {
		compCfg.TargetCompactionSizeBytes = v
	} else if v, ok := config["targetCompactionSizeBytes"].(float64); ok {
		compCfg.TargetCompactionSizeBytes = int64(v)
	}
	if v, ok := config["skipOffsetFromLatest"].(string); ok {
		compCfg.SkipOffsetFromLatest = v
	}

	if err := dm.Client().SubmitCompaction(ctx, datasource, compCfg); err != nil {
		return nil, fmt.Errorf("step.ts_druid_compact %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":     "compaction_submitted",
		"datasource": datasource,
	}}, nil
}
