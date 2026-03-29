package tenancy

import (
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/ident"
)

// validateIdentifier returns an error if id contains characters that could enable
// SQL injection when interpolated into DDL statements (CREATE/DROP/ALTER).
func validateIdentifier(id string) error {
	return ident.Validate(id)
}
