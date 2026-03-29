package migrate

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/ident"
)

// validSQLExprRe matches safe SQL expressions for trigger transforms:
// column references (NEW.col), casts (col::type), simple functions (LOWER(col), COALESCE(col, 'x')),
// literals, and arithmetic. Rejects semicolons, statement keywords (DELETE, DROP, INSERT, UPDATE, CREATE).
var validSQLExprRe = regexp.MustCompile(`^[a-zA-Z0-9_.(),:'"\s+\-*/|]+$`)
var unsafeSQLKeywords = regexp.MustCompile(`(?i)\b(DELETE|DROP|INSERT|UPDATE|CREATE|ALTER|TRUNCATE|EXECUTE|GRANT|REVOKE)\b`)

func validateTransformExpr(expr string) error {
	if expr == "" {
		return nil
	}
	if !validSQLExprRe.MatchString(expr) {
		return fmt.Errorf("transform expression %q contains unsafe characters", expr)
	}
	if unsafeSQLKeywords.MatchString(expr) {
		return fmt.Errorf("transform expression %q contains unsafe SQL keyword", expr)
	}
	if strings.Contains(expr, ";") {
		return fmt.Errorf("transform expression %q contains semicolon", expr)
	}
	return nil
}

// ExpandContractChange describes one change in an expand-contract migration.
type ExpandContractChange struct {
	Type      string `json:"type"`      // add_column, rename_column, change_type, drop_column
	OldColumn string `json:"oldColumn"`
	NewColumn string `json:"newColumn"`
	NewType   string `json:"newType"`
	Transform string `json:"transform"` // SQL expr (references NEW.<col>); defaults set per type
}

// ExpandContractPlan holds all SQL generated for the expand and contract phases.
type ExpandContractPlan struct {
	Table        string
	Changes      []ExpandContractChange
	ExpandSQL    []string // DDL + triggers to apply during expand phase
	ContractSQL  []string // DDL to apply during contract phase
	VerifySQL    string   // SQL to verify all rows have been migrated
	TriggerNames []string // generated trigger names (for status checking)
	NewColumns   []string // new column names added during expand
}

// BuildExpandContractPlan generates SQL statements for the expand and contract phases.
//
// Expand phase creates new columns and BEFORE INSERT OR UPDATE triggers that
// keep old and new columns in sync, enabling dual-version read/write.
//
// Contract phase removes old structures once all consumers have migrated.
func BuildExpandContractPlan(table string, changes []ExpandContractChange) (*ExpandContractPlan, error) {
	if err := ident.Validate(table); err != nil {
		return nil, fmt.Errorf("expand-contract: table %w", err)
	}

	plan := &ExpandContractPlan{
		Table:   table,
		Changes: changes,
	}

	for _, ch := range changes {
		if err := applyChange(plan, table, ch); err != nil {
			return nil, err
		}
	}

	// VerifySQL: count rows where any new column is still NULL.
	if len(plan.NewColumns) > 0 {
		conditions := make([]string, 0, len(plan.NewColumns))
		for _, col := range plan.NewColumns {
			conditions = append(conditions, col+" IS NULL")
		}
		plan.VerifySQL = fmt.Sprintf(
			"SELECT COUNT(*) FROM %s WHERE %s;",
			table, strings.Join(conditions, " OR "),
		)
	}

	return plan, nil
}

func applyChange(plan *ExpandContractPlan, table string, ch ExpandContractChange) error {
	switch ch.Type {
	case "add_column":
		if err := ident.Validate(ch.NewColumn); err != nil {
			return fmt.Errorf("expand-contract: newColumn %w", err)
		}
		if err := validateSQLType(ch.NewType); err != nil {
			return fmt.Errorf("expand-contract: newType for %q: %w", ch.NewColumn, err)
		}
		transform := ch.Transform
		if transform == "" {
			transform = "NULL"
		}
		addSQL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", table, ch.NewColumn, ch.NewType)
		fnName := triggerFnName(table, ch.NewColumn)
		trgName := triggerName(table, ch.NewColumn)
		fnSQL, trgSQL, err := buildTriggerSQL(ch.NewColumn, transform, fnName, trgName, table)
		if err != nil {
			return err
		}
		plan.ExpandSQL = append(plan.ExpandSQL, addSQL, fnSQL, trgSQL)
		// Contract: drop trigger + function only (new column stays)
		plan.ContractSQL = append(plan.ContractSQL,
			fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s;", trgName, table),
			fmt.Sprintf("DROP FUNCTION IF EXISTS %s();", fnName),
		)
		plan.TriggerNames = append(plan.TriggerNames, trgName)
		plan.NewColumns = append(plan.NewColumns, ch.NewColumn)

	case "rename_column":
		if err := ident.Validate(ch.OldColumn); err != nil {
			return fmt.Errorf("expand-contract: oldColumn %w", err)
		}
		if err := ident.Validate(ch.NewColumn); err != nil {
			return fmt.Errorf("expand-contract: newColumn %w", err)
		}
		if err := validateSQLType(ch.NewType); err != nil {
			return fmt.Errorf("expand-contract: newType for %q: %w", ch.NewColumn, err)
		}
		transform := ch.Transform
		if transform == "" {
			transform = "NEW." + ch.OldColumn
		}
		addSQL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", table, ch.NewColumn, ch.NewType)
		fnName := triggerFnName(table, ch.NewColumn)
		trgName := triggerName(table, ch.NewColumn)
		fnSQL, trgSQL, err := buildTriggerSQL(ch.NewColumn, transform, fnName, trgName, table)
		if err != nil {
			return err
		}
		plan.ExpandSQL = append(plan.ExpandSQL, addSQL, fnSQL, trgSQL)
		// Contract: drop trigger + function + old column
		plan.ContractSQL = append(plan.ContractSQL,
			fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s;", trgName, table),
			fmt.Sprintf("DROP FUNCTION IF EXISTS %s();", fnName),
			fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", table, ch.OldColumn),
		)
		plan.TriggerNames = append(plan.TriggerNames, trgName)
		plan.NewColumns = append(plan.NewColumns, ch.NewColumn)

	case "change_type":
		if err := ident.Validate(ch.OldColumn); err != nil {
			return fmt.Errorf("expand-contract: oldColumn %w", err)
		}
		if err := ident.Validate(ch.NewColumn); err != nil {
			return fmt.Errorf("expand-contract: newColumn %w", err)
		}
		if err := validateSQLType(ch.NewType); err != nil {
			return fmt.Errorf("expand-contract: newType for %q: %w", ch.NewColumn, err)
		}
		transform := ch.Transform
		if transform == "" {
			transform = "NEW." + ch.OldColumn + "::" + ch.NewType
		}
		addSQL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", table, ch.NewColumn, ch.NewType)
		fnName := triggerFnName(table, ch.NewColumn)
		trgName := triggerName(table, ch.NewColumn)
		fnSQL, trgSQL, err := buildTriggerSQL(ch.NewColumn, transform, fnName, trgName, table)
		if err != nil {
			return err
		}
		plan.ExpandSQL = append(plan.ExpandSQL, addSQL, fnSQL, trgSQL)
		// Contract: drop trigger + function + old column
		plan.ContractSQL = append(plan.ContractSQL,
			fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s;", trgName, table),
			fmt.Sprintf("DROP FUNCTION IF EXISTS %s();", fnName),
			fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", table, ch.OldColumn),
		)
		plan.TriggerNames = append(plan.TriggerNames, trgName)
		plan.NewColumns = append(plan.NewColumns, ch.NewColumn)

	case "drop_column":
		if err := ident.Validate(ch.OldColumn); err != nil {
			return fmt.Errorf("expand-contract: oldColumn %w", err)
		}
		// Expand phase is a no-op for drop; just add to contract.
		plan.ContractSQL = append(plan.ContractSQL,
			fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", table, ch.OldColumn),
		)

	default:
		return fmt.Errorf("expand-contract: unknown change type %q (add_column, rename_column, change_type, drop_column)", ch.Type)
	}

	return nil
}

// buildTriggerSQL generates a PostgreSQL trigger function and trigger statement
// that sets newCol := transform on every BEFORE INSERT OR UPDATE.
func buildTriggerSQL(newCol, transform, fnName, trgName, table string) (fnSQL, trgSQL string, err error) {
	if err := validateTransformExpr(transform); err != nil {
		return "", "", fmt.Errorf("expand-contract trigger: %w", err)
	}
	fnSQL = fmt.Sprintf(
		"CREATE OR REPLACE FUNCTION %s() RETURNS TRIGGER AS $body$\nBEGIN\n  NEW.%s := %s;\n  RETURN NEW;\nEND;\n$body$ LANGUAGE plpgsql;",
		fnName, newCol, transform,
	)
	trgSQL = fmt.Sprintf(
		"CREATE TRIGGER %s\n  BEFORE INSERT OR UPDATE ON %s\n  FOR EACH ROW EXECUTE FUNCTION %s();",
		trgName, table, fnName,
	)
	return fnSQL, trgSQL, nil
}

// triggerFnName returns the name for the trigger function syncing table.col.
func triggerFnName(table, col string) string {
	return fmt.Sprintf("trg_fn_%s_%s_sync", table, col)
}

// triggerName returns the name for the trigger syncing table.col.
func triggerName(table, col string) string {
	return fmt.Sprintf("trg_%s_%s_sync", table, col)
}

// parseExpandContractChanges converts []any from config into []ExpandContractChange.
func parseExpandContractChanges(raw []any) []ExpandContractChange {
	changes := make([]ExpandContractChange, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		ch := ExpandContractChange{}
		ch.Type, _ = m["type"].(string)
		ch.OldColumn, _ = m["oldColumn"].(string)
		ch.NewColumn, _ = m["newColumn"].(string)
		ch.NewType, _ = m["newType"].(string)
		ch.Transform, _ = m["transform"].(string)
		changes = append(changes, ch)
	}
	return changes
}
