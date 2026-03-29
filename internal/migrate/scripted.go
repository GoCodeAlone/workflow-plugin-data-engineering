package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SQLExecutor executes SQL statements against a database connection.
type SQLExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// MigrationRunner executes ordered migration scripts with advisory locking.
type MigrationRunner struct {
	mu        sync.RWMutex
	executor  SQLExecutor
	lockTable string
}

// MigrationScript is a versioned up/down SQL pair parsed from disk.
type MigrationScript struct {
	Version     int
	Description string
	UpSQL       string
	DownSQL     string
}

// MigrationState describes the current migration state of the database.
type MigrationState struct {
	Version int
	Applied []AppliedMigration
	Pending []MigrationScript
}

// AppliedMigration is a migration that has already been run.
type AppliedMigration struct {
	Version   int
	AppliedAt time.Time
	Checksum  string
}

// scriptFileRe matches NNN_description.up.sql or NNN_description.down.sql.
var scriptFileRe = regexp.MustCompile(`^(\d+)_([^.]+)\.(up|down)\.sql$`)

// NewMigrationRunner creates a runner backed by executor storing state in lockTable.
// Returns an error if lockTable is not a valid SQL identifier.
func NewMigrationRunner(executor SQLExecutor, lockTable string) (*MigrationRunner, error) {
	if err := validateIdentifier(lockTable); err != nil {
		return nil, fmt.Errorf("lock table: %w", err)
	}
	return &MigrationRunner{executor: executor, lockTable: lockTable}, nil
}

// EnsureLockTable creates the migration state table if it does not exist.
func (r *MigrationRunner) EnsureLockTable(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := validateIdentifier(r.lockTable); err != nil {
		return fmt.Errorf("lock table %w", err)
	}
	_, err := r.executor.ExecContext(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  version    BIGINT      NOT NULL PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  checksum   TEXT        NOT NULL,
  description TEXT       NOT NULL DEFAULT ''
);`, r.lockTable))
	if err != nil {
		return fmt.Errorf("ensure lock table %q: %w", r.lockTable, err)
	}
	return nil
}

// LoadScripts parses NNN_description.up.sql + NNN_description.down.sql pairs from dir.
// Pairs are sorted by version ascending. A missing .down.sql file is not an error.
func LoadScripts(dir string) ([]MigrationScript, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations dir %q: %w", dir, err)
	}

	type halfScript struct {
		version     int
		description string
		direction   string
		sql         string
	}

	byVersion := make(map[int]*MigrationScript)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		m := scriptFileRe.FindStringSubmatch(entry.Name())
		if m == nil {
			continue
		}
		version, _ := strconv.Atoi(m[1])
		description := m[2]
		direction := m[3]

		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read migration file %q: %w", entry.Name(), err)
		}

		if _, ok := byVersion[version]; !ok {
			byVersion[version] = &MigrationScript{Version: version, Description: description}
		}
		switch direction {
		case "up":
			byVersion[version].UpSQL = string(data)
		case "down":
			byVersion[version].DownSQL = string(data)
		}
	}

	scripts := make([]MigrationScript, 0, len(byVersion))
	for _, s := range byVersion {
		if s.UpSQL == "" {
			return nil, fmt.Errorf("migration version %d has no up script", s.Version)
		}
		scripts = append(scripts, *s)
	}
	sort.Slice(scripts, func(i, j int) bool { return scripts[i].Version < scripts[j].Version })
	return scripts, nil
}

// Status returns the current version and which scripts are still pending.
func (r *MigrationRunner) Status(ctx context.Context, scripts []MigrationScript) (*MigrationState, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	applied, err := r.loadApplied(ctx)
	if err != nil {
		return nil, err
	}

	appliedSet := make(map[int]bool, len(applied))
	maxVersion := 0
	for _, a := range applied {
		appliedSet[a.Version] = true
		if a.Version > maxVersion {
			maxVersion = a.Version
		}
	}

	var pending []MigrationScript
	for _, s := range scripts {
		if !appliedSet[s.Version] {
			pending = append(pending, s)
		}
	}

	return &MigrationState{
		Version: maxVersion,
		Applied: applied,
		Pending: pending,
	}, nil
}

// Apply runs all pending migrations from scripts in version order.
// It acquires a Postgres advisory lock for the duration.
func (r *MigrationRunner) Apply(ctx context.Context, scripts []MigrationScript) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.advisoryLock(ctx); err != nil {
		return err
	}
	defer r.advisoryUnlock(ctx) //nolint:errcheck

	applied, err := r.loadApplied(ctx)
	if err != nil {
		return err
	}
	appliedSet := make(map[int]bool, len(applied))
	for _, a := range applied {
		appliedSet[a.Version] = true
	}

	for _, s := range scripts {
		if appliedSet[s.Version] {
			continue
		}
		if strings.TrimSpace(s.UpSQL) == "" {
			return fmt.Errorf("migration %d: up SQL is empty", s.Version)
		}
		if _, err := r.executor.ExecContext(ctx, s.UpSQL); err != nil {
			return fmt.Errorf("migration %d (%s): execute up: %w", s.Version, s.Description, err)
		}
		checksum := sha256sum(s.UpSQL)
		_, err := r.executor.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO %s (version, checksum, description) VALUES ($1, $2, $3)`, r.lockTable),
			s.Version, checksum, s.Description,
		)
		if err != nil {
			return fmt.Errorf("migration %d: record applied: %w", s.Version, err)
		}
	}
	return nil
}

// Rollback rolls back the N most recent applied migrations using their down scripts.
func (r *MigrationRunner) Rollback(ctx context.Context, scripts []MigrationScript, steps int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.advisoryLock(ctx); err != nil {
		return err
	}
	defer r.advisoryUnlock(ctx) //nolint:errcheck

	applied, err := r.loadApplied(ctx)
	if err != nil {
		return err
	}
	if len(applied) == 0 {
		return nil
	}

	// Sort applied descending.
	sort.Slice(applied, func(i, j int) bool { return applied[i].Version > applied[j].Version })

	scriptByVersion := make(map[int]MigrationScript, len(scripts))
	for _, s := range scripts {
		scriptByVersion[s.Version] = s
	}

	if steps > len(applied) {
		steps = len(applied)
	}

	for i := range steps {
		a := applied[i]
		s, ok := scriptByVersion[a.Version]
		if !ok {
			return fmt.Errorf("rollback: no script found for version %d", a.Version)
		}
		if strings.TrimSpace(s.DownSQL) == "" {
			return fmt.Errorf("rollback: migration %d has no down script", a.Version)
		}
		if _, err := r.executor.ExecContext(ctx, s.DownSQL); err != nil {
			return fmt.Errorf("rollback migration %d: execute down: %w", a.Version, err)
		}
		_, err := r.executor.ExecContext(ctx,
			fmt.Sprintf(`DELETE FROM %s WHERE version = $1`, r.lockTable),
			a.Version,
		)
		if err != nil {
			return fmt.Errorf("rollback migration %d: remove record: %w", a.Version, err)
		}
	}
	return nil
}

// loadApplied reads all applied migrations from the lock table.
func (r *MigrationRunner) loadApplied(ctx context.Context) ([]AppliedMigration, error) {
	rows, err := r.executor.QueryContext(ctx,
		fmt.Sprintf(`SELECT version, applied_at, checksum FROM %s ORDER BY version ASC`, r.lockTable),
	)
	if err != nil {
		return nil, fmt.Errorf("query applied migrations: %w", err)
	}
	defer rows.Close()

	var applied []AppliedMigration
	for rows.Next() {
		var a AppliedMigration
		if err := rows.Scan(&a.Version, &a.AppliedAt, &a.Checksum); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}
		applied = append(applied, a)
	}
	return applied, rows.Err()
}

// advisoryLock acquires a Postgres session-level advisory lock with a fixed key.
func (r *MigrationRunner) advisoryLock(ctx context.Context) error {
	_, err := r.executor.ExecContext(ctx, `SELECT pg_advisory_lock(8765432198765432)`)
	if err != nil {
		return fmt.Errorf("acquire advisory lock: %w", err)
	}
	return nil
}

// advisoryUnlock releases the advisory lock.
func (r *MigrationRunner) advisoryUnlock(ctx context.Context) error {
	_, err := r.executor.ExecContext(ctx, `SELECT pg_advisory_unlock(8765432198765432)`)
	return err
}

// sha256sum returns the hex SHA-256 of the given string.
func sha256sum(s string) string {
	h := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", h)
}
