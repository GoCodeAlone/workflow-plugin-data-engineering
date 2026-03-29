package migrate

import (
	"context"
	"fmt"
	"os"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// stepBase holds the step name.
type stepBase struct {
	name string
}

// resolveModule looks up the migration module specified by config["module"].
func (s *stepBase) resolveModule(config map[string]any) (*SchemaModule, error) {
	moduleName, _ := config["module"].(string)
	if moduleName == "" {
		return nil, fmt.Errorf("module is required")
	}
	return LookupModule(moduleName)
}

// resolveExecutor extracts a SQLExecutor from the step execution context.
// The executor is passed via config["_executor"] by the plugin host.
func resolveExecutor(config map[string]any) (SQLExecutor, bool) {
	exec, ok := config["_executor"].(SQLExecutor)
	return exec, ok
}

// ─── step.migrate_plan ────────────────────────────────────────────────────────

// MigratePlanStep diffs the desired schema against the live database.
type MigratePlanStep struct{ stepBase }

// NewMigratePlanStep creates a step.migrate_plan instance.
func NewMigratePlanStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &MigratePlanStep{stepBase{name}}, nil
}

func (s *MigratePlanStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	mod, err := s.resolveModule(config)
	if err != nil {
		return nil, fmt.Errorf("step.migrate_plan %q: %w", s.name, err)
	}

	// Determine desired schemas.
	desired := mod.Schemas()
	if desiredPath, _ := config["desired"].(string); desiredPath != "" {
		def, err := ParseSchemaFile(desiredPath)
		if err != nil {
			return nil, fmt.Errorf("step.migrate_plan %q: parse desired: %w", s.name, err)
		}
		desired = []SchemaDefinition{def}
	}

	// Introspect live schema if executor available.
	var allChanges []map[string]any
	safe := true
	changeCount := 0

	exec, hasExec := resolveExecutor(config)
	for _, d := range desired {
		var live SchemaDefinition
		if hasExec {
			liveDef, err := IntrospectSchema(ctx, exec, d.Table)
			if err != nil {
				return nil, fmt.Errorf("step.migrate_plan %q: introspect %q: %w", s.name, d.Table, err)
			}
			if liveDef != nil {
				live = *liveDef
			}
		}
		plan, err := DiffSchema(d, live)
		if err != nil {
			return nil, fmt.Errorf("step.migrate_plan %q: diff %q: %w", s.name, d.Table, err)
		}
		if !plan.Safe {
			safe = false
		}
		for _, c := range plan.Changes {
			allChanges = append(allChanges, map[string]any{
				"type":        c.Type,
				"description": c.Description,
				"sql":         c.SQL,
				"breaking":    c.Breaking,
			})
		}
		changeCount += len(plan.Changes)
	}

	return &sdk.StepResult{Output: map[string]any{
		"plan":        allChanges,
		"safe":        safe,
		"changeCount": changeCount,
	}}, nil
}

// ─── step.migrate_apply ───────────────────────────────────────────────────────

// MigrateApplyStep executes a previously computed migration plan.
type MigrateApplyStep struct{ stepBase }

// NewMigrateApplyStep creates a step.migrate_apply instance.
func NewMigrateApplyStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &MigrateApplyStep{stepBase{name}}, nil
}

func (s *MigrateApplyStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	mod, err := s.resolveModule(config)
	if err != nil {
		return nil, fmt.Errorf("step.migrate_apply %q: %w", s.name, err)
	}

	mode, _ := config["mode"].(string)
	if mode == "" {
		mode = "online"
	}
	if mode != "online" && mode != "blue_green" {
		return nil, fmt.Errorf("step.migrate_apply %q: unknown mode %q (online, blue_green)", s.name, mode)
	}

	// Extract plan from config (pre-computed or inline).
	planRaw, _ := config["plan"].([]any)
	var changes []SchemaChange
	for _, raw := range planRaw {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		c := SchemaChange{}
		c.Type, _ = m["type"].(string)
		c.Description, _ = m["description"].(string)
		c.SQL, _ = m["sql"].(string)
		c.Breaking, _ = m["breaking"].(bool)
		changes = append(changes, c)
	}

	// Check breaking changes.
	policy := mod.OnBreakingChange()
	for _, c := range changes {
		if c.Breaking && policy == "block" {
			return nil, fmt.Errorf("step.migrate_apply %q: breaking change blocked: %s", s.name, c.Description)
		}
	}

	exec, hasExec := resolveExecutor(config)
	if !hasExec {
		return &sdk.StepResult{Output: map[string]any{
			"status":         "dry_run",
			"changesApplied": len(changes),
		}}, nil
	}

	switch mode {
	case "online":
		for _, c := range changes {
			if c.SQL == "" {
				continue
			}
			if _, err := exec.ExecContext(ctx, c.SQL); err != nil {
				return nil, fmt.Errorf("step.migrate_apply %q: apply %q: %w", s.name, c.Type, err)
			}
		}
	case "blue_green":
		// Blue-green: apply changes on a shadow table, then swap.
		// For each change, wrap in a transaction and apply to a shadow.
		for _, c := range changes {
			if c.SQL == "" {
				continue
			}
			shadowSQL := blueGreenWrap(c.SQL)
			if _, err := exec.ExecContext(ctx, shadowSQL); err != nil {
				return nil, fmt.Errorf("step.migrate_apply %q: blue_green apply %q: %w", s.name, c.Type, err)
			}
		}
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":         "applied",
		"changesApplied": len(changes),
		"mode":           mode,
	}}, nil
}

// blueGreenWrap wraps a DDL statement in a BEGIN/COMMIT block.
// A real blue-green would swap tables, but this is the DDL execution path.
func blueGreenWrap(sql string) string {
	return "BEGIN;\n" + sql + "\nCOMMIT;"
}

// ─── step.migrate_run ─────────────────────────────────────────────────────────

// MigrateRunStep runs a specific numbered migration script.
type MigrateRunStep struct{ stepBase }

// NewMigrateRunStep creates a step.migrate_run instance.
func NewMigrateRunStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &MigrateRunStep{stepBase{name}}, nil
}

func (s *MigrateRunStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	mod, err := s.resolveModule(config)
	if err != nil {
		return nil, fmt.Errorf("step.migrate_run %q: %w", s.name, err)
	}

	exec, hasExec := resolveExecutor(config)

	// Resolve the specific script.
	var script *MigrationScript

	if scriptPath, _ := config["script"].(string); scriptPath != "" {
		data, err := os.ReadFile(scriptPath)
		if err != nil {
			return nil, fmt.Errorf("step.migrate_run %q: read script %q: %w", s.name, scriptPath, err)
		}
		script = &MigrationScript{Version: 0, Description: scriptPath, UpSQL: string(data)}
	} else {
		version := intVal(config["version"])
		if version == 0 {
			return nil, fmt.Errorf("step.migrate_run %q: script or version is required", s.name)
		}
		for i, sc := range mod.Scripts() {
			if sc.Version == version {
				script = &mod.Scripts()[i]
				break
			}
		}
		if script == nil {
			return nil, fmt.Errorf("step.migrate_run %q: version %d not found", s.name, version)
		}
	}

	if !hasExec {
		return &sdk.StepResult{Output: map[string]any{
			"status":  "dry_run",
			"version": script.Version,
		}}, nil
	}

	runner, err := NewMigrationRunner(exec, mod.LockTable())
	if err != nil {
		return nil, fmt.Errorf("step %q: %w", s.name, err)
	}
	if err := runner.Apply(ctx, []MigrationScript{*script}); err != nil {
		return nil, fmt.Errorf("step.migrate_run %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":  "migrated",
		"version": script.Version,
	}}, nil
}

// ─── step.migrate_rollback ────────────────────────────────────────────────────

// MigrateRollbackStep rolls back N migration scripts.
type MigrateRollbackStep struct{ stepBase }

// NewMigrateRollbackStep creates a step.migrate_rollback instance.
func NewMigrateRollbackStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &MigrateRollbackStep{stepBase{name}}, nil
}

func (s *MigrateRollbackStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	mod, err := s.resolveModule(config)
	if err != nil {
		return nil, fmt.Errorf("step.migrate_rollback %q: %w", s.name, err)
	}

	steps := intVal(config["steps"])
	if steps <= 0 {
		steps = 1
	}

	exec, hasExec := resolveExecutor(config)
	if !hasExec {
		return &sdk.StepResult{Output: map[string]any{
			"status": "dry_run",
			"steps":  steps,
		}}, nil
	}

	runner, err := NewMigrationRunner(exec, mod.LockTable())
	if err != nil {
		return nil, fmt.Errorf("step %q: %w", s.name, err)
	}

	// Get state before rollback.
	scripts := mod.Scripts()
	stateBefore, err := runner.Status(ctx, scripts)
	if err != nil {
		return nil, fmt.Errorf("step.migrate_rollback %q: status: %w", s.name, err)
	}
	fromVersion := stateBefore.Version

	if err := runner.Rollback(ctx, scripts, steps); err != nil {
		return nil, fmt.Errorf("step.migrate_rollback %q: %w", s.name, err)
	}

	stateAfter, err := runner.Status(ctx, scripts)
	if err != nil {
		return nil, fmt.Errorf("step.migrate_rollback %q: status after: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":      "rolled_back",
		"fromVersion": fromVersion,
		"toVersion":   stateAfter.Version,
	}}, nil
}

// ─── step.migrate_status ──────────────────────────────────────────────────────

// MigrateStatusStep reports the current migration state.
type MigrateStatusStep struct{ stepBase }

// NewMigrateStatusStep creates a step.migrate_status instance.
func NewMigrateStatusStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &MigrateStatusStep{stepBase{name}}, nil
}

func (s *MigrateStatusStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	mod, err := s.resolveModule(config)
	if err != nil {
		return nil, fmt.Errorf("step.migrate_status %q: %w", s.name, err)
	}

	exec, hasExec := resolveExecutor(config)
	if !hasExec {
		return &sdk.StepResult{Output: map[string]any{
			"status":  "no_executor",
			"version": 0,
			"pending": len(mod.Scripts()),
		}}, nil
	}

	runner, err := NewMigrationRunner(exec, mod.LockTable())
	if err != nil {
		return nil, fmt.Errorf("step %q: %w", s.name, err)
	}
	scripts := mod.Scripts()
	state, err := runner.Status(ctx, scripts)
	if err != nil {
		return nil, fmt.Errorf("step.migrate_status %q: %w", s.name, err)
	}

	applied := make([]map[string]any, 0, len(state.Applied))
	for _, a := range state.Applied {
		applied = append(applied, map[string]any{
			"version":   a.Version,
			"appliedAt": a.AppliedAt,
			"checksum":  a.Checksum,
		})
	}

	return &sdk.StepResult{Output: map[string]any{
		"version": state.Version,
		"pending": len(state.Pending),
		"applied": applied,
	}}, nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func intVal(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	}
	return 0
}
