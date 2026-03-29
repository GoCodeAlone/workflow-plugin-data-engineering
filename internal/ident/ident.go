// Package ident provides SQL and Cypher identifier validation.
package ident

import (
	"fmt"
	"regexp"
)

// ValidIdentifierRe matches safe SQL/Cypher identifiers: must start with a letter or
// underscore, followed by letters, digits, or underscores. This prevents injection
// payloads from being interpolated into DDL or Cypher statements.
var ValidIdentifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// Validate returns an error if id is not a valid SQL identifier.
func Validate(id string) error {
	if id == "" {
		return fmt.Errorf("identifier must not be empty")
	}
	if !ValidIdentifierRe.MatchString(id) {
		return fmt.Errorf("identifier %q is invalid (must start with a letter or underscore, followed by [a-zA-Z0-9_])", id)
	}
	return nil
}

// ValidateCypher returns an error if id is not a valid Cypher label, relationship
// type, or property key. kind names what is being validated (used in error messages).
func ValidateCypher(id, kind string) error {
	if !ValidIdentifierRe.MatchString(id) {
		return fmt.Errorf("invalid Cypher %s %q: must match [a-zA-Z_][a-zA-Z0-9_]*", kind, id)
	}
	return nil
}
