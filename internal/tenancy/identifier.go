package tenancy

import (
	"fmt"
	"regexp"
)

// validIdentifierRe matches safe SQL identifiers: letters, digits, underscores only.
var validIdentifierRe = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// validateIdentifier returns an error if id contains characters that could enable
// SQL injection when interpolated into DDL statements (CREATE/DROP/ALTER).
// DDL statements cannot use parameterized placeholders, so only identifier
// characters ([a-zA-Z0-9_]) are permitted.
func validateIdentifier(id string) error {
	if id == "" {
		return fmt.Errorf("identifier must not be empty")
	}
	if !validIdentifierRe.MatchString(id) {
		return fmt.Errorf("identifier %q contains invalid characters (only [a-zA-Z0-9_] allowed)", id)
	}
	return nil
}
