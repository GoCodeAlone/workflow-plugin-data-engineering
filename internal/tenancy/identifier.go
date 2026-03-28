package tenancy

import (
	"fmt"
	"regexp"
)

// validIdentifierRe matches safe SQL identifiers: must start with a letter or underscore,
// followed by letters, digits, or underscores. This mirrors standard SQL identifier rules
// and prevents injection payloads from being interpolated into DDL statements.
var validIdentifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// validateIdentifier returns an error if id contains characters that could enable
// SQL injection when interpolated into DDL statements (CREATE/DROP/ALTER).
// DDL statements cannot use parameterized placeholders, so only identifier
// characters ([a-zA-Z0-9_]) are permitted.
func validateIdentifier(id string) error {
	if id == "" {
		return fmt.Errorf("identifier must not be empty")
	}
	if !validIdentifierRe.MatchString(id) {
		return fmt.Errorf("identifier %q is invalid (must start with a letter or underscore, followed by [a-zA-Z0-9_])", id)
	}
	return nil
}
