// Package tenancy implements multi-tenant data isolation modules and steps.
package tenancy

import "context"

// SQLExecutor runs a SQL statement against an admin database connection.
type SQLExecutor func(ctx context.Context, sql string) error

// noopSQLExecutor discards all SQL (used when no DB connection is configured).
var noopSQLExecutor SQLExecutor = func(_ context.Context, _ string) error { return nil }

// TenancyStrategy defines how multi-tenant isolation is implemented.
// Each strategy controls table resolution, connection routing, tenant filtering,
// and tenant lifecycle SQL for a specific isolation model.
type TenancyStrategy interface {
	// ResolveTable returns the fully-qualified table reference for a given tenant.
	ResolveTable(tenantID, table string) string

	// ResolveConnection returns the connection string for a given tenant,
	// potentially replacing a template placeholder with the tenant ID.
	ResolveConnection(tenantID, baseConnection string) string

	// TenantFilter returns the column and value to filter by for row-level isolation.
	// Returns empty strings when the strategy uses schema or DB isolation.
	TenantFilter(tenantID string) (column, value string)

	// ProvisionTenant creates the tenant's namespace (schema, database, etc.).
	ProvisionTenant(ctx context.Context, tenantID string) error

	// DeprovisionTenant removes or archives the tenant's namespace.
	// mode is "archive" (rename/retain data) or "delete" (destroy data).
	DeprovisionTenant(ctx context.Context, tenantID string, mode string) error
}
