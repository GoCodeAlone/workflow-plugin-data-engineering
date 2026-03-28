package tenancy

import (
	"context"
	"testing"
)

func TestTenancyModule_AllStrategies(t *testing.T) {
	tests := []struct {
		name     string
		config   map[string]any
		wantType string
	}{
		{
			name: "schema_per_tenant",
			config: map[string]any{
				"strategy":      "schema_per_tenant",
				"tenant_key":    "ctx.tenant_id",
				"schema_prefix": "t_",
			},
			wantType: "schema_per_tenant",
		},
		{
			name: "db_per_tenant",
			config: map[string]any{
				"strategy":            "db_per_tenant",
				"tenant_key":          "ctx.org",
				"connection_template": "postgres://localhost/{{tenant}}",
			},
			wantType: "db_per_tenant",
		},
		{
			name: "row_level",
			config: map[string]any{
				"strategy":      "row_level",
				"tenant_key":    "ctx.tenant_id",
				"tenant_column": "org_id",
				"tables":        []any{"users", "orders"},
			},
			wantType: "row_level",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mod, err := NewTenancyModule(tc.name, tc.config)
			if err != nil {
				t.Fatalf("NewTenancyModule: %v", err)
			}
			tm, ok := mod.(*TenancyModule)
			if !ok {
				t.Fatal("expected *TenancyModule")
			}
			if tm.Strategy() == nil {
				t.Fatal("expected non-nil strategy")
			}
			if tm.config.Strategy != tc.wantType {
				t.Errorf("strategy = %q, want %q", tm.config.Strategy, tc.wantType)
			}
		})
	}
}

func TestTenancyModule_InvalidStrategy(t *testing.T) {
	_, err := NewTenancyModule("bad", map[string]any{"strategy": "unknown_strategy"})
	if err == nil {
		t.Fatal("expected error for unknown strategy")
	}
}

func TestTenancyModule_MissingStrategy(t *testing.T) {
	_, err := NewTenancyModule("empty", map[string]any{})
	if err == nil {
		t.Fatal("expected error for missing strategy")
	}
}

func TestTenancyModule_TenantKey(t *testing.T) {
	mod, err := NewTenancyModule("m", map[string]any{
		"strategy":   "row_level",
		"tenant_key": "ctx.tenant_id",
	})
	if err != nil {
		t.Fatal(err)
	}
	tm := mod.(*TenancyModule)
	if got := tm.TenantKey(); got != "ctx.tenant_id" {
		t.Errorf("TenantKey() = %q, want %q", got, "ctx.tenant_id")
	}
}

func TestTenancyModule_Lifecycle(t *testing.T) {
	mod, err := NewTenancyModule("m", map[string]any{
		"strategy":      "schema_per_tenant",
		"schema_prefix": "t_",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := mod.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := mod.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestTenancyModule_Strategy_SchemaPerTenant_ResolveTable(t *testing.T) {
	mod, err := NewTenancyModule("m", map[string]any{
		"strategy":      "schema_per_tenant",
		"schema_prefix": "tenant_",
	})
	if err != nil {
		t.Fatal(err)
	}
	tm := mod.(*TenancyModule)
	got := tm.Strategy().ResolveTable("corp", "users")
	want := "tenant_corp.users"
	if got != want {
		t.Errorf("ResolveTable = %q, want %q", got, want)
	}
}

func TestTenancyModule_Strategy_RowLevel_TenantFilter(t *testing.T) {
	mod, err := NewTenancyModule("m", map[string]any{
		"strategy":      "row_level",
		"tenant_column": "org_id",
	})
	if err != nil {
		t.Fatal(err)
	}
	tm := mod.(*TenancyModule)
	col, val := tm.Strategy().TenantFilter("acme")
	if col != "org_id" || val != "acme" {
		t.Errorf("TenantFilter = (%q,%q), want (org_id,acme)", col, val)
	}
}

func TestTenancyModule_Strategy_DBPerTenant_ResolveConnection(t *testing.T) {
	mod, err := NewTenancyModule("m", map[string]any{
		"strategy":            "db_per_tenant",
		"connection_template": "postgres://localhost/db_{{tenant}}",
	})
	if err != nil {
		t.Fatal(err)
	}
	tm := mod.(*TenancyModule)
	got := tm.Strategy().ResolveConnection("acme", "postgres://localhost/db_{{tenant}}")
	want := "postgres://localhost/db_acme"
	if got != want {
		t.Errorf("ResolveConnection = %q, want %q", got, want)
	}
}
