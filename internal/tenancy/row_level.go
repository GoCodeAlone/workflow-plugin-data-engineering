package tenancy

import (
	"context"
	"errors"
	"fmt"
)

// RowLevel isolates tenants via a tenant ID column in shared tables.
// All tables are shared; queries must include a WHERE clause filtering by tenant.
type RowLevel struct {
	tenantColumn string
	tables       []string
	executor     SQLExecutor
}

// NewRowLevel creates a RowLevel strategy.
// tenantColumn is the column used for tenant filtering.
// tables is the list of tenant-scoped tables (used only in DeprovisionTenant delete mode).
func NewRowLevel(tenantColumn string, tables []string, exec SQLExecutor) *RowLevel {
	return &RowLevel{tenantColumn: tenantColumn, tables: tables, executor: exec}
}

// ResolveTable returns the table unchanged — shared tables, no schema prefix.
func (r *RowLevel) ResolveTable(tenantID, table string) string {
	return table
}

// ResolveConnection returns the base connection unchanged — shared database.
func (r *RowLevel) ResolveConnection(tenantID, baseConnection string) string {
	return baseConnection
}

// TenantFilter returns the tenant column and tenant ID for WHERE clause injection.
func (r *RowLevel) TenantFilter(tenantID string) (string, string) {
	return r.tenantColumn, tenantID
}

// ProvisionTenant is a no-op for row-level isolation (shared tables need no setup).
func (r *RowLevel) ProvisionTenant(_ context.Context, _ string) error {
	return nil
}

// DeprovisionTenant deletes all rows belonging to the tenant (mode "delete").
// mode "archive" is a no-op for row-level isolation — caller must handle archiving.
func (r *RowLevel) DeprovisionTenant(ctx context.Context, tenantID string, mode string) error {
	if mode != "delete" {
		return nil
	}
	var errs []error
	for _, table := range r.tables {
		// tenantID is passed as a query parameter ($1) to prevent SQL injection.
		// Table and column names are not user-controlled (set at strategy construction time).
		sql := fmt.Sprintf("DELETE FROM %s WHERE %s = $1", table, r.tenantColumn)
		if err := r.executor(ctx, sql, tenantID); err != nil {
			errs = append(errs, fmt.Errorf("row_level: delete from %s: %w", table, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("row_level: deprovision %q: %w", tenantID, errors.Join(errs...))
	}
	return nil
}
