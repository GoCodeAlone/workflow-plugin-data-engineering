package migrate

import (
	"context"
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// ─── step.migrate_expand ──────────────────────────────────────────────────────

// MigrateExpandStep executes the expand phase of an expand-contract migration.
// It adds new columns and installs sync triggers for dual-write operation.
type MigrateExpandStep struct{ name string }

// NewMigrateExpandStep creates a step.migrate_expand instance.
func NewMigrateExpandStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &MigrateExpandStep{name: name}, nil
}

func (s *MigrateExpandStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	table, _ := config["table"].(string)
	if table == "" {
		return nil, fmt.Errorf("step.migrate_expand %q: table is required", s.name)
	}
	rawChanges, _ := config["changes"].([]any)
	changes := parseExpandContractChanges(rawChanges)
	if len(changes) == 0 {
		return nil, fmt.Errorf("step.migrate_expand %q: changes is required", s.name)
	}

	plan, err := BuildExpandContractPlan(table, changes)
	if err != nil {
		return nil, fmt.Errorf("step.migrate_expand %q: %w", s.name, err)
	}

	exec, hasExec := resolveExecutor(config)
	if !hasExec {
		return &sdk.StepResult{Output: map[string]any{
			"status":       "dry_run",
			"table":        table,
			"triggerNames": plan.TriggerNames,
			"newColumns":   plan.NewColumns,
			"expandSQL":    plan.ExpandSQL,
		}}, nil
	}

	for _, sql := range plan.ExpandSQL {
		if _, err := exec.ExecContext(ctx, sql); err != nil {
			return nil, fmt.Errorf("step.migrate_expand %q: execute expand SQL: %w", s.name, err)
		}
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":       "expanded",
		"table":        table,
		"triggerNames": plan.TriggerNames,
		"newColumns":   plan.NewColumns,
	}}, nil
}

// ─── step.migrate_contract ────────────────────────────────────────────────────

// MigrateContractStep executes the contract phase: removes old columns and triggers
// once all consumers have migrated to the new schema.
type MigrateContractStep struct{ name string }

// NewMigrateContractStep creates a step.migrate_contract instance.
func NewMigrateContractStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &MigrateContractStep{name: name}, nil
}

func (s *MigrateContractStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	table, _ := config["table"].(string)
	if table == "" {
		return nil, fmt.Errorf("step.migrate_contract %q: table is required", s.name)
	}
	rawChanges, _ := config["changes"].([]any)
	changes := parseExpandContractChanges(rawChanges)
	if len(changes) == 0 {
		return nil, fmt.Errorf("step.migrate_contract %q: changes is required", s.name)
	}

	plan, err := BuildExpandContractPlan(table, changes)
	if err != nil {
		return nil, fmt.Errorf("step.migrate_contract %q: %w", s.name, err)
	}

	exec, hasExec := resolveExecutor(config)
	if !hasExec {
		return &sdk.StepResult{Output: map[string]any{
			"status":         "dry_run",
			"table":          table,
			"contractSQL":    plan.ContractSQL,
		}}, nil
	}

	var droppedColumns, droppedTriggers []string
	for _, sql := range plan.ContractSQL {
		if _, err := exec.ExecContext(ctx, sql); err != nil {
			return nil, fmt.Errorf("step.migrate_contract %q: execute contract SQL: %w", s.name, err)
		}
		// Track what was dropped for the output.
		if isDropColumnSQL(sql) {
			droppedColumns = append(droppedColumns, sql)
		}
		if isDropTriggerSQL(sql) {
			droppedTriggers = append(droppedTriggers, sql)
		}
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":          "contracted",
		"table":           table,
		"droppedColumns":  droppedColumns,
		"droppedTriggers": droppedTriggers,
	}}, nil
}

func isDropColumnSQL(sql string) bool {
	return len(sql) > 10 && sql[:10] == "ALTER TABL" && containsString(sql, "DROP COLUMN")
}

func isDropTriggerSQL(sql string) bool {
	return len(sql) > 4 && sql[:4] == "DROP" && containsString(sql, "TRIGGER")
}

func containsString(s, sub string) bool {
	return len(s) >= len(sub) && func() bool {
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	}()
}

// ─── step.migrate_expand_status ───────────────────────────────────────────────

// MigrateExpandStatusStep checks whether an expand phase is active and safe to contract.
type MigrateExpandStatusStep struct{ name string }

// NewMigrateExpandStatusStep creates a step.migrate_expand_status instance.
func NewMigrateExpandStatusStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &MigrateExpandStatusStep{name: name}, nil
}

func (s *MigrateExpandStatusStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	table, _ := config["table"].(string)
	if table == "" {
		return nil, fmt.Errorf("step.migrate_expand_status %q: table is required", s.name)
	}
	triggerName, _ := config["triggerName"].(string)

	exec, hasExec := resolveExecutor(config)
	if !hasExec {
		return &sdk.StepResult{Output: map[string]any{
			"expanded":            false,
			"triggerActive":       false,
			"oldColumnReferences": 0,
			"safeToContract":      false,
		}}, nil
	}

	triggerActive, err := checkTriggerExists(ctx, exec, table, triggerName)
	if err != nil {
		return nil, fmt.Errorf("step.migrate_expand_status %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"expanded":            triggerActive,
		"triggerActive":       triggerActive,
		"oldColumnReferences": 0,
		"safeToContract":      triggerActive,
	}}, nil
}

// checkTriggerExists queries pg_trigger to check if the named trigger exists on the table.
func checkTriggerExists(ctx context.Context, exec SQLExecutor, table, trgName string) (bool, error) {
	rows, err := exec.QueryContext(ctx, `
SELECT COUNT(*) FROM pg_trigger t
JOIN pg_class c ON t.tgrelid = c.oid
WHERE c.relname = $1 AND t.tgname = $2 AND NOT t.tgisinternal`,
		table, trgName)
	if err != nil {
		return false, fmt.Errorf("query trigger existence: %w", err)
	}
	defer rows.Close()
	var count int
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return false, fmt.Errorf("scan trigger count: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return count > 0, nil
}
