// Package quality implements Go-native data quality checks, profiling, and anomaly detection.
package quality

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
)

// DBQuerier is the subset of *sql.DB used by quality checks and the profiler.
// *sql.DB satisfies this interface natively; go-sqlmock returns *sql.DB, so tests work without adapters.
type DBQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// validIdentRe matches safe SQL identifiers: letter/underscore prefix, then alphanum/underscore/dot.
// Dots are allowed for schema-qualified names (e.g. raw.users).
var validIdentRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)

// validateIdent returns an error if id contains characters unsafe for SQL identifier interpolation.
func validateIdent(id string) error {
	if id == "" {
		return fmt.Errorf("identifier must not be empty")
	}
	if !validIdentRe.MatchString(id) {
		return fmt.Errorf("identifier %q is invalid (must match [a-zA-Z_][a-zA-Z0-9_.]*)", id)
	}
	return nil
}
