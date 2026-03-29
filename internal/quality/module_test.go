package quality

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestQualityModule_Init(t *testing.T) {
	m, err := NewChecksModule("test-mod", map[string]any{
		"provider": "builtin",
	})
	if err != nil {
		t.Fatalf("NewChecksModule: %v", err)
	}
	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
}

func TestQualityModule_Init_DefaultProvider(t *testing.T) {
	// provider defaults to "builtin" — Init should not fail.
	m, err := NewChecksModule("test-default", map[string]any{})
	if err != nil {
		t.Fatalf("NewChecksModule: %v", err)
	}
	if err := m.Init(); err != nil {
		t.Errorf("Init with default provider: %v", err)
	}
}

func TestQualityModule_StartStop(t *testing.T) {
	mod := NewChecksModuleWithExecutor("qmod-startstop", nil)

	if err := mod.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Should be registered.
	found, err := LookupChecksModule("qmod-startstop")
	if err != nil {
		t.Fatalf("LookupChecksModule after Start: %v", err)
	}
	if found != mod {
		t.Error("LookupChecksModule returned wrong module")
	}

	if err := mod.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := LookupChecksModule("qmod-startstop"); err == nil {
		t.Error("expected lookup to fail after Stop")
	}
}

func TestQualityModule_Builtin(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mod := NewChecksModuleWithExecutor("qmod-builtin", db)
	if err := mod.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer mod.Stop(context.Background())

	// Verify the executor is accessible.
	exec := mod.Executor()
	if exec == nil {
		t.Fatal("Executor should not be nil after injection")
	}

	// Run a simple check through the module executor.
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM orders").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(100))

	result, err := RunCheck(context.Background(), exec, "row_count", "orders", map[string]any{
		"min": int64(10),
	})
	if err != nil {
		t.Fatalf("RunCheck: %v", err)
	}
	if !result.Passed {
		t.Errorf("expected pass, got: %s", result.Message)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestQualityModule_DuplicateRegistration(t *testing.T) {
	mod := NewChecksModuleWithExecutor("qmod-dup", nil)
	if err := mod.Start(context.Background()); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	defer mod.Stop(context.Background())

	mod2 := NewChecksModuleWithExecutor("qmod-dup", nil)
	if err := mod2.Start(context.Background()); err == nil {
		t.Error("expected error for duplicate registration")
		mod2.Stop(context.Background())
	}
}

func TestQualityModule_SetExecutor(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mod := NewChecksModuleWithExecutor("qmod-setexec", nil)
	if mod.Executor() != nil {
		t.Error("initial executor should be nil")
	}
	mod.SetExecutor(db)
	if mod.Executor() == nil {
		t.Error("executor should not be nil after SetExecutor")
	}
}
