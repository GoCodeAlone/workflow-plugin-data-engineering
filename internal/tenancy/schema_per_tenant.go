package tenancy

import (
	"context"
	"fmt"
)

// SchemaPerTenant isolates each tenant in its own database schema.
// Tables are prefixed with "{prefix}{tenantID}." and the shared admin
// connection is used for all tenants.
type SchemaPerTenant struct {
	prefix   string
	executor SQLExecutor
}

// NewSchemaPerTenant creates a SchemaPerTenant strategy.
// prefix is prepended to the tenant ID to form the schema name (e.g. "t_" → "t_acme").
func NewSchemaPerTenant(prefix string, exec SQLExecutor) *SchemaPerTenant {
	return &SchemaPerTenant{prefix: prefix, executor: exec}
}

// ResolveTable returns "{prefix}{tenantID}.{table}".
func (s *SchemaPerTenant) ResolveTable(tenantID, table string) string {
	return fmt.Sprintf("%s%s.%s", s.prefix, tenantID, table)
}

// ResolveConnection returns the base connection unchanged (shared DB, schema-based isolation).
func (s *SchemaPerTenant) ResolveConnection(tenantID, baseConnection string) string {
	return baseConnection
}

// TenantFilter returns empty strings — isolation is via schema, not row filtering.
func (s *SchemaPerTenant) TenantFilter(tenantID string) (string, string) {
	return "", ""
}

// ProvisionTenant executes CREATE SCHEMA IF NOT EXISTS for the tenant.
func (s *SchemaPerTenant) ProvisionTenant(ctx context.Context, tenantID string) error {
	if err := validateIdentifier(tenantID); err != nil {
		return fmt.Errorf("schema_per_tenant: provision: invalid tenant ID: %w", err)
	}
	sql := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s%s", s.prefix, tenantID)
	return s.executor(ctx, sql)
}

// DeprovisionTenant archives or drops the tenant's schema.
// mode "archive" renames the schema; mode "delete" drops it with CASCADE.
func (s *SchemaPerTenant) DeprovisionTenant(ctx context.Context, tenantID string, mode string) error {
	if err := validateIdentifier(tenantID); err != nil {
		return fmt.Errorf("schema_per_tenant: deprovision: invalid tenant ID: %w", err)
	}
	switch mode {
	case "archive":
		sql := fmt.Sprintf("ALTER SCHEMA %s%s RENAME TO %s%s_archived",
			s.prefix, tenantID, s.prefix, tenantID)
		return s.executor(ctx, sql)
	case "delete":
		sql := fmt.Sprintf("DROP SCHEMA %s%s CASCADE", s.prefix, tenantID)
		return s.executor(ctx, sql)
	default:
		return fmt.Errorf("schema_per_tenant: unknown deprovision mode %q (valid: archive, delete)", mode)
	}
}
