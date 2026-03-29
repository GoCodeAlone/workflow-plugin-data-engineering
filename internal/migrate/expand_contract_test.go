package migrate

import (
	"context"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestExpandContract_AddColumn(t *testing.T) {
	plan, err := BuildExpandContractPlan("users", []ExpandContractChange{
		{Type: "add_column", NewColumn: "email_lower", NewType: "text", Transform: "LOWER(email)"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// ADD COLUMN + CREATE FUNCTION + CREATE TRIGGER
	if len(plan.ExpandSQL) != 3 {
		t.Fatalf("expected 3 expand SQL statements, got %d: %v", len(plan.ExpandSQL), plan.ExpandSQL)
	}
	if !strings.Contains(plan.ExpandSQL[0], "ADD COLUMN") || !strings.Contains(plan.ExpandSQL[0], "email_lower") {
		t.Errorf("expand[0] should ADD COLUMN email_lower, got: %s", plan.ExpandSQL[0])
	}
	if !strings.Contains(strings.ToUpper(plan.ExpandSQL[1]), "CREATE OR REPLACE FUNCTION") {
		t.Errorf("expand[1] should CREATE FUNCTION, got: %s", plan.ExpandSQL[1])
	}
	if !strings.Contains(strings.ToUpper(plan.ExpandSQL[2]), "CREATE TRIGGER") {
		t.Errorf("expand[2] should CREATE TRIGGER, got: %s", plan.ExpandSQL[2])
	}
	// ContractSQL for add_column: DROP TRIGGER + DROP FUNCTION only (new column stays)
	if len(plan.ContractSQL) != 2 {
		t.Fatalf("expected 2 contract SQL for add_column, got %d: %v", len(plan.ContractSQL), plan.ContractSQL)
	}
	if len(plan.TriggerNames) != 1 {
		t.Errorf("expected 1 trigger name, got %d", len(plan.TriggerNames))
	}
	if len(plan.NewColumns) != 1 || plan.NewColumns[0] != "email_lower" {
		t.Errorf("expected NewColumns=[email_lower], got %v", plan.NewColumns)
	}
}

func TestExpandContract_RenameColumn(t *testing.T) {
	plan, err := BuildExpandContractPlan("users", []ExpandContractChange{
		{Type: "rename_column", OldColumn: "name", NewColumn: "full_name", NewType: "text"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.ExpandSQL) != 3 {
		t.Fatalf("expected 3 expand SQL, got %d", len(plan.ExpandSQL))
	}
	if !strings.Contains(plan.ExpandSQL[0], "full_name") {
		t.Errorf("expand[0] should mention full_name, got: %s", plan.ExpandSQL[0])
	}
	// ContractSQL: DROP TRIGGER + DROP FUNCTION + DROP COLUMN old
	if len(plan.ContractSQL) != 3 {
		t.Fatalf("expected 3 contract SQL for rename_column, got %d: %v", len(plan.ContractSQL), plan.ContractSQL)
	}
	foundDrop := false
	for _, s := range plan.ContractSQL {
		if strings.Contains(s, "DROP COLUMN") && strings.Contains(s, "name") {
			foundDrop = true
		}
	}
	if !foundDrop {
		t.Errorf("expected DROP COLUMN name in contract SQL, got %v", plan.ContractSQL)
	}
	// Default transform: NEW.<oldColumn>
	if !strings.Contains(plan.ExpandSQL[1], "NEW.name") {
		t.Errorf("trigger function should set full_name := NEW.name, got: %s", plan.ExpandSQL[1])
	}
}

func TestExpandContract_ChangeType(t *testing.T) {
	plan, err := BuildExpandContractPlan("orders", []ExpandContractChange{
		{Type: "change_type", OldColumn: "amount_str", NewColumn: "amount", NewType: "numeric", Transform: "amount_str::numeric"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.ExpandSQL) != 3 {
		t.Fatalf("expected 3 expand SQL, got %d", len(plan.ExpandSQL))
	}
	if !strings.Contains(plan.ExpandSQL[0], "amount") || !strings.Contains(plan.ExpandSQL[0], "numeric") {
		t.Errorf("expand[0] should ADD COLUMN amount numeric, got: %s", plan.ExpandSQL[0])
	}
	// ContractSQL should drop old column
	foundDrop := false
	for _, s := range plan.ContractSQL {
		if strings.Contains(s, "DROP COLUMN") && strings.Contains(s, "amount_str") {
			foundDrop = true
		}
	}
	if !foundDrop {
		t.Errorf("expected DROP COLUMN amount_str in contract SQL, got %v", plan.ContractSQL)
	}
}

func TestExpandContract_DropColumn(t *testing.T) {
	plan, err := BuildExpandContractPlan("products", []ExpandContractChange{
		{Type: "drop_column", OldColumn: "legacy_field"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// No expand SQL for drop_column
	for _, s := range plan.ExpandSQL {
		if strings.Contains(s, "legacy_field") {
			t.Errorf("drop_column should not generate expand SQL for old column, got: %s", s)
		}
	}
	// ContractSQL: just DROP COLUMN
	if len(plan.ContractSQL) != 1 {
		t.Fatalf("expected 1 contract SQL for drop_column, got %d: %v", len(plan.ContractSQL), plan.ContractSQL)
	}
	if !strings.Contains(plan.ContractSQL[0], "DROP COLUMN") || !strings.Contains(plan.ContractSQL[0], "legacy_field") {
		t.Errorf("contract[0] should DROP COLUMN legacy_field, got: %s", plan.ContractSQL[0])
	}
}

func TestExpandContract_TriggerGeneration(t *testing.T) {
	plan, err := BuildExpandContractPlan("events", []ExpandContractChange{
		{Type: "rename_column", OldColumn: "ts", NewColumn: "event_time", NewType: "timestamptz"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.TriggerNames) != 1 {
		t.Fatalf("expected 1 trigger name, got %d", len(plan.TriggerNames))
	}
	trgName := plan.TriggerNames[0]
	if !strings.Contains(trgName, "events") || !strings.Contains(trgName, "event_time") {
		t.Errorf("trigger name %q should contain table and column names", trgName)
	}
	// Trigger SQL references the trigger name
	found := false
	for _, s := range plan.ExpandSQL {
		if strings.Contains(s, trgName) {
			found = true
		}
	}
	if !found {
		t.Errorf("trigger name %q not found in expand SQL: %v", trgName, plan.ExpandSQL)
	}
	// Default transform: NEW.ts (the old column)
	if !strings.Contains(plan.ExpandSQL[1], "NEW.ts") {
		t.Errorf("expected trigger function to reference NEW.ts, got: %s", plan.ExpandSQL[1])
	}
}

func TestExpandContract_InvalidTable(t *testing.T) {
	_, err := BuildExpandContractPlan("bad-table!", []ExpandContractChange{
		{Type: "add_column", NewColumn: "col", NewType: "text"},
	})
	if err == nil {
		t.Error("expected error for invalid table identifier")
	}
}

func TestExpandContract_InvalidColumn(t *testing.T) {
	_, err := BuildExpandContractPlan("users", []ExpandContractChange{
		{Type: "rename_column", OldColumn: "bad-name!", NewColumn: "new_name", NewType: "text"},
	})
	if err == nil {
		t.Error("expected error for invalid column identifier")
	}
}

func TestExpandContract_UnknownType(t *testing.T) {
	_, err := BuildExpandContractPlan("users", []ExpandContractChange{
		{Type: "unknown_op", NewColumn: "col", NewType: "text"},
	})
	if err == nil {
		t.Error("expected error for unknown change type")
	}
}

func TestExpandContract_VerifySQL(t *testing.T) {
	plan, err := BuildExpandContractPlan("users", []ExpandContractChange{
		{Type: "add_column", NewColumn: "email_normalized", NewType: "text", Transform: "LOWER(email)"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.VerifySQL == "" {
		t.Fatal("expected VerifySQL to be populated")
	}
	if !strings.Contains(plan.VerifySQL, "email_normalized") {
		t.Errorf("VerifySQL should reference new column, got: %s", plan.VerifySQL)
	}
	if !strings.Contains(plan.VerifySQL, "IS NULL") {
		t.Errorf("VerifySQL should check for NULLs, got: %s", plan.VerifySQL)
	}
}

func TestExpandContract_DropColumn_NoVerifySQL(t *testing.T) {
	plan, err := BuildExpandContractPlan("products", []ExpandContractChange{
		{Type: "drop_column", OldColumn: "legacy"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Drop column adds nothing new, so VerifySQL should be empty.
	if plan.VerifySQL != "" {
		t.Errorf("expected empty VerifySQL for drop_column, got: %s", plan.VerifySQL)
	}
}

func TestMigrateExpand_Step(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectExec("ALTER TABLE users ADD COLUMN email_lower").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE OR REPLACE FUNCTION trg_fn_users_email_lower_sync").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TRIGGER trg_users_email_lower_sync").WillReturnResult(sqlmock.NewResult(0, 0))

	step, _ := NewMigrateExpandStep("expand", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"table": "users",
		"changes": []any{
			map[string]any{
				"type":      "add_column",
				"newColumn": "email_lower",
				"newType":   "text",
				"transform": "LOWER(email)",
			},
		},
		"_executor": db,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["status"] != "expanded" {
		t.Errorf("expected status=expanded, got %v", result.Output["status"])
	}
	if result.Output["table"] != "users" {
		t.Errorf("expected table=users, got %v", result.Output["table"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestMigrateExpand_DryRun(t *testing.T) {
	step, _ := NewMigrateExpandStep("expand", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"table": "users",
		"changes": []any{
			map[string]any{
				"type":      "add_column",
				"newColumn": "status",
				"newType":   "text",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["status"] != "dry_run" {
		t.Errorf("expected dry_run, got %v", result.Output["status"])
	}
}

func TestMigrateContract_Step(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectExec("DROP TRIGGER IF EXISTS trg_users_full_name_sync").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("DROP FUNCTION IF EXISTS trg_fn_users_full_name_sync").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("ALTER TABLE users DROP COLUMN name").WillReturnResult(sqlmock.NewResult(0, 0))

	step, _ := NewMigrateContractStep("contract", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"table": "users",
		"changes": []any{
			map[string]any{
				"type":      "rename_column",
				"oldColumn": "name",
				"newColumn": "full_name",
				"newType":   "text",
			},
		},
		"_executor": db,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["status"] != "contracted" {
		t.Errorf("expected status=contracted, got %v", result.Output["status"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestMigrateContract_DryRun(t *testing.T) {
	step, _ := NewMigrateContractStep("contract", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"table": "users",
		"changes": []any{
			map[string]any{
				"type":      "drop_column",
				"oldColumn": "legacy",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["status"] != "dry_run" {
		t.Errorf("expected dry_run, got %v", result.Output["status"])
	}
}

func TestExpandStatus(t *testing.T) {
	db, mock := newMockDB(t)
	// Query trigger existence in pg_trigger
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(1),
	)

	step, _ := NewMigrateExpandStatusStep("status", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"table":       "users",
		"triggerName": "trg_users_full_name_sync",
		"_executor":   db,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["triggerActive"] != true {
		t.Error("expected triggerActive=true")
	}
	if result.Output["expanded"] != true {
		t.Error("expected expanded=true")
	}
	if result.Output["safeToContract"] != true {
		t.Error("expected safeToContract=true")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestExpandStatus_NoTrigger(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(0),
	)

	step, _ := NewMigrateExpandStatusStep("status", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"table":       "users",
		"triggerName": "trg_users_col_sync",
		"_executor":   db,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["triggerActive"] != false {
		t.Error("expected triggerActive=false")
	}
	if result.Output["safeToContract"] != false {
		t.Error("expected safeToContract=false when no trigger")
	}
}

func TestExpandStatus_NoExecutor(t *testing.T) {
	step, _ := NewMigrateExpandStatusStep("status", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"table":       "users",
		"triggerName": "trg_users_col_sync",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Output["triggerActive"] != false {
		t.Error("expected triggerActive=false with no executor")
	}
}
