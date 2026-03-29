package timeseries

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/ident"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// tsClickHouseViewStep implements step.ts_clickhouse_view — manages ClickHouse materialized views.
type tsClickHouseViewStep struct {
	name string
}

// NewTSClickHouseViewStep creates a new step.ts_clickhouse_view instance.
func NewTSClickHouseViewStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &tsClickHouseViewStep{name: name}, nil
}

func (s *tsClickHouseViewStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	module, _ := stringValTS(config, "module")
	if module == "" {
		return nil, fmt.Errorf("step.ts_clickhouse_view %q: module is required", s.name)
	}
	viewName, _ := stringValTS(config, "viewName")
	if viewName == "" {
		return nil, fmt.Errorf("step.ts_clickhouse_view %q: viewName is required", s.name)
	}
	action, _ := stringValTS(config, "action")
	if action == "" {
		action = "create"
	}

	if err := ident.Validate(viewName); err != nil {
		return nil, fmt.Errorf("step.ts_clickhouse_view %q: invalid viewName: %w", s.name, err)
	}

	writer, err := Lookup(module)
	if err != nil {
		return nil, fmt.Errorf("step.ts_clickhouse_view %q: %w", s.name, err)
	}
	chmod, ok := writer.(*ClickHouseModule)
	if !ok {
		return nil, fmt.Errorf("step.ts_clickhouse_view %q: module %q is not a timeseries.clickhouse module", s.name, module)
	}

	switch action {
	case "create":
		return s.create(ctx, chmod, viewName, config)
	case "drop":
		return s.drop(ctx, chmod, viewName)
	case "status":
		return s.status(ctx, chmod, viewName)
	default:
		return nil, fmt.Errorf("step.ts_clickhouse_view %q: unknown action %q (create|drop|status)", s.name, action)
	}
}

func (s *tsClickHouseViewStep) create(ctx context.Context, m *ClickHouseModule, viewName string, config map[string]any) (*sdk.StepResult, error) {
	query, _ := stringValTS(config, "query")
	if query == "" {
		return nil, fmt.Errorf("step.ts_clickhouse_view %q: query is required for create action", s.name)
	}
	engine, _ := stringValTS(config, "engine")
	if engine == "" {
		engine = "MergeTree()"
	}
	orderBy, _ := stringValTS(config, "orderBy")
	partitionBy, _ := stringValTS(config, "partitionBy")

	m.mu.RLock()
	conn := m.conn
	m.mu.RUnlock()
	if conn == nil {
		return nil, fmt.Errorf("step.ts_clickhouse_view %q: module not started", s.name)
	}

	ddl, err := buildClickHouseViewDDL(viewName, engine, orderBy, partitionBy, query)
	if err != nil {
		return nil, fmt.Errorf("step.ts_clickhouse_view %q: %w", s.name, err)
	}
	if err := conn.Exec(ctx, ddl); err != nil {
		return nil, fmt.Errorf("step.ts_clickhouse_view %q: create view %q: %w", s.name, viewName, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":   "created",
		"viewName": viewName,
		"engine":   engine,
	}}, nil
}

func (s *tsClickHouseViewStep) drop(ctx context.Context, m *ClickHouseModule, viewName string) (*sdk.StepResult, error) {
	m.mu.RLock()
	conn := m.conn
	m.mu.RUnlock()
	if conn == nil {
		return nil, fmt.Errorf("step.ts_clickhouse_view %q: module not started", s.name)
	}

	ddl := fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName)
	if err := conn.Exec(ctx, ddl); err != nil {
		return nil, fmt.Errorf("step.ts_clickhouse_view %q: drop view %q: %w", s.name, viewName, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":   "dropped",
		"viewName": viewName,
	}}, nil
}

func (s *tsClickHouseViewStep) status(ctx context.Context, m *ClickHouseModule, viewName string) (*sdk.StepResult, error) {
	m.mu.RLock()
	conn := m.conn
	m.mu.RUnlock()
	if conn == nil {
		return nil, fmt.Errorf("step.ts_clickhouse_view %q: module not started", s.name)
	}

	query := fmt.Sprintf(
		`SELECT name, engine, total_rows, total_bytes FROM system.tables WHERE name = '%s' AND database = currentDatabase()`,
		viewName,
	)
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("step.ts_clickhouse_view %q: status query: %w", s.name, err)
	}
	defer rows.Close()

	out := map[string]any{
		"viewName": viewName,
		"isActive": false,
		"rows":     0,
		"bytes":    0,
		"parts":    0,
		"engine":   "",
	}

	if rows.Next() {
		var name, engine any
		var totalRows, totalBytes any
		if scanErr := rows.Scan(&name, &engine, &totalRows, &totalBytes); scanErr == nil {
			out["isActive"] = true
			if v, ok := engine.(string); ok {
				out["engine"] = v
			}
			switch v := totalRows.(type) {
			case uint64:
				out["rows"] = v
			case int64:
				out["rows"] = v
			}
			switch v := totalBytes.(type) {
			case uint64:
				out["bytes"] = v
			case int64:
				out["bytes"] = v
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("step.ts_clickhouse_view %q: status rows: %w", s.name, err)
	}

	return &sdk.StepResult{Output: out}, nil
}

// validCHEngineRe matches safe ClickHouse engine names like AggregatingMergeTree(), SummingMergeTree(amount).
var validCHEngineRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*\([a-zA-Z0-9_, ]*\)$`)

// validCHExprRe matches safe ClickHouse expressions for ORDER BY / PARTITION BY.
// Allows identifiers, function calls, commas, spaces, parens — rejects semicolons and SQL keywords.
var validCHExprRe = regexp.MustCompile(`^[a-zA-Z0-9_(),. ]+$`)
var unsafeCHKeywords = regexp.MustCompile(`(?i)\b(DROP|DELETE|INSERT|UPDATE|CREATE|ALTER|TRUNCATE)\b`)

// validateCHExpr validates a ClickHouse ORDER BY / PARTITION BY expression.
func validateCHExpr(s, field string) error {
	if !validCHExprRe.MatchString(s) {
		return fmt.Errorf("%s %q contains unsafe characters", field, s)
	}
	if unsafeCHKeywords.MatchString(s) {
		return fmt.Errorf("%s %q contains unsafe keyword", field, s)
	}
	return nil
}

// buildClickHouseViewDDL builds the CREATE MATERIALIZED VIEW DDL.
func buildClickHouseViewDDL(viewName, engine, orderBy, partitionBy, query string) (string, error) {
	if engine != "" && !validCHEngineRe.MatchString(engine) {
		return "", fmt.Errorf("step.ts_clickhouse_view: unsafe engine %q", engine)
	}
	if orderBy != "" {
		if err := validateCHExpr(orderBy, "orderBy"); err != nil {
			return "", fmt.Errorf("step.ts_clickhouse_view: %w", err)
		}
	}
	if partitionBy != "" {
		if err := validateCHExpr(partitionBy, "partitionBy"); err != nil {
			return "", fmt.Errorf("step.ts_clickhouse_view: %w", err)
		}
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s\n", viewName))
	sb.WriteString(fmt.Sprintf("ENGINE = %s\n", engine))
	if orderBy != "" {
		sb.WriteString(fmt.Sprintf("ORDER BY (%s)\n", orderBy))
	}
	if partitionBy != "" {
		sb.WriteString(fmt.Sprintf("PARTITION BY (%s)\n", partitionBy))
	}
	sb.WriteString("AS ")
	sb.WriteString(query)
	return sb.String(), nil
}
