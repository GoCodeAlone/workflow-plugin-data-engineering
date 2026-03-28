package tenancy

import (
	"context"
	"fmt"
	"strings"
)

// DBPerTenant isolates each tenant in its own database.
// A connection template uses "{{tenant}}" as a placeholder for the tenant ID.
type DBPerTenant struct {
	executor SQLExecutor
}

// NewDBPerTenant creates a DBPerTenant strategy.
func NewDBPerTenant(exec SQLExecutor) *DBPerTenant {
	return &DBPerTenant{executor: exec}
}

// ResolveTable returns the table unchanged — tenant is isolated at the DB level.
func (d *DBPerTenant) ResolveTable(tenantID, table string) string {
	return table
}

// ResolveConnection replaces "{{tenant}}" in the connection template with the tenant ID.
func (d *DBPerTenant) ResolveConnection(tenantID, baseConnection string) string {
	return strings.ReplaceAll(baseConnection, "{{tenant}}", tenantID)
}

// TenantFilter returns empty strings — isolation is via database, not row filtering.
func (d *DBPerTenant) TenantFilter(tenantID string) (string, string) {
	return "", ""
}

// ProvisionTenant executes CREATE DATABASE for the tenant.
func (d *DBPerTenant) ProvisionTenant(ctx context.Context, tenantID string) error {
	if err := validateIdentifier(tenantID); err != nil {
		return fmt.Errorf("db_per_tenant: provision: invalid tenant ID: %w", err)
	}
	sql := fmt.Sprintf("CREATE DATABASE %s", tenantID)
	return d.executor(ctx, sql)
}

// DeprovisionTenant archives or drops the tenant's database.
// mode "archive" renames the database; mode "delete" drops it.
func (d *DBPerTenant) DeprovisionTenant(ctx context.Context, tenantID string, mode string) error {
	if err := validateIdentifier(tenantID); err != nil {
		return fmt.Errorf("db_per_tenant: deprovision: invalid tenant ID: %w", err)
	}
	switch mode {
	case "archive":
		sql := fmt.Sprintf("ALTER DATABASE %s RENAME TO %s_archived", tenantID, tenantID)
		return d.executor(ctx, sql)
	case "delete":
		sql := fmt.Sprintf("DROP DATABASE %s", tenantID)
		return d.executor(ctx, sql)
	default:
		return fmt.Errorf("db_per_tenant: unknown deprovision mode %q (valid: archive, delete)", mode)
	}
}
