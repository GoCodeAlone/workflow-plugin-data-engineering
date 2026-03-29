// Package migrate provides declarative schema diffing and scripted migration running.
package migrate

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// validIdentifierRe matches safe SQL identifiers.
var validIdentifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// validSQLTypeRe matches safe SQL column types (e.g., bigint, varchar(255), timestamptz, numeric(10,2)).
var validSQLTypeRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_ ]*(\(\d+(,\s*\d+)?\))?(\[\])?$`)

// validSQLDefaultRe matches safe SQL default values: numeric literals, quoted strings, function calls like now().
var validSQLDefaultRe = regexp.MustCompile(`^(\d+(\.\d+)?|'[^']*'|[a-zA-Z_][a-zA-Z0-9_]*\(\)|true|false|NULL)$`)

func validateSQLType(t string) error {
	if t == "" {
		return fmt.Errorf("column type must not be empty")
	}
	if !validSQLTypeRe.MatchString(t) {
		return fmt.Errorf("column type %q is invalid", t)
	}
	return nil
}

func validateSQLDefault(d string) error {
	if d == "" {
		return nil
	}
	if !validSQLDefaultRe.MatchString(d) {
		return fmt.Errorf("column default %q is invalid (must be a literal, quoted string, or simple function call)", d)
	}
	return nil
}

func validateIdentifier(id string) error {
	if id == "" {
		return fmt.Errorf("identifier must not be empty")
	}
	if !validIdentifierRe.MatchString(id) {
		return fmt.Errorf("identifier %q is invalid (must start with a letter or underscore, followed by [a-zA-Z0-9_])", id)
	}
	return nil
}

// SchemaDefinition describes the desired state of a database table.
type SchemaDefinition struct {
	Table   string      `json:"table"             yaml:"table"`
	Columns []ColumnDef `json:"columns"           yaml:"columns"`
	Indexes []IndexDef  `json:"indexes,omitempty" yaml:"indexes,omitempty"`
}

// ColumnDef describes a single column within a table.
type ColumnDef struct {
	Name       string `json:"name"               yaml:"name"`
	Type       string `json:"type"               yaml:"type"`
	Nullable   bool   `json:"nullable,omitempty" yaml:"nullable,omitempty"`
	PrimaryKey bool   `json:"primaryKey,omitempty" yaml:"primaryKey,omitempty"`
	Unique     bool   `json:"unique,omitempty"   yaml:"unique,omitempty"`
	Default    string `json:"default,omitempty"  yaml:"default,omitempty"`
}

// IndexDef describes an index on a table.
type IndexDef struct {
	Name    string   `json:"name,omitempty"   yaml:"name,omitempty"`
	Columns []string `json:"columns"          yaml:"columns"`
	Unique  bool     `json:"unique,omitempty" yaml:"unique,omitempty"`
}

// MigrationPlan is the result of diffing two schema definitions.
type MigrationPlan struct {
	Changes []SchemaChange
	Safe    bool // true if all changes are non-breaking
}

// SchemaChange represents a single DDL change.
type SchemaChange struct {
	Type        string // add_column, add_table, add_index, widen_type, drop_column, narrow_type, drop_index
	Description string
	SQL         string
	Breaking    bool
}

// ParseSchemaFile parses a YAML schema definition file.
func ParseSchemaFile(path string) (SchemaDefinition, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return SchemaDefinition{}, fmt.Errorf("read schema file %q: %w", path, err)
	}
	var def SchemaDefinition
	if err := yaml.Unmarshal(data, &def); err != nil {
		return SchemaDefinition{}, fmt.Errorf("parse schema file %q: %w", path, err)
	}
	if err := validateIdentifier(def.Table); err != nil {
		return SchemaDefinition{}, fmt.Errorf("schema file %q: table %w", path, err)
	}
	for _, col := range def.Columns {
		if err := validateIdentifier(col.Name); err != nil {
			return SchemaDefinition{}, fmt.Errorf("schema file %q: column %w", path, err)
		}
	}
	return def, nil
}

// ParseSchemaDir parses all .yaml files in a directory as schema definitions.
func ParseSchemaDir(dir string) ([]SchemaDefinition, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read schema dir %q: %w", dir, err)
	}
	var defs []SchemaDefinition
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
			continue
		}
		def, err := ParseSchemaFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		defs = append(defs, def)
	}
	return defs, nil
}

// DiffSchema computes the migration plan to move from live to desired schema.
// live may be an empty SchemaDefinition (zero value) if the table does not yet exist.
func DiffSchema(desired, live SchemaDefinition) (MigrationPlan, error) {
	if err := validateIdentifier(desired.Table); err != nil {
		return MigrationPlan{}, fmt.Errorf("desired schema: table %w", err)
	}
	for _, col := range desired.Columns {
		if err := validateIdentifier(col.Name); err != nil {
			return MigrationPlan{}, fmt.Errorf("desired schema: column %w", err)
		}
	}

	var plan MigrationPlan
	plan.Safe = true

	// Table doesn't exist yet — emit CREATE TABLE.
	if live.Table == "" {
		sql, err := buildCreateTable(desired)
		if err != nil {
			return MigrationPlan{}, err
		}
		plan.Changes = append(plan.Changes, SchemaChange{
			Type:        "add_table",
			Description: fmt.Sprintf("create table %s", desired.Table),
			SQL:         sql,
			Breaking:    false,
		})
		// Add indexes on new table.
		for _, idx := range desired.Indexes {
			idxSQL, err := buildCreateIndex(desired.Table, idx)
			if err != nil {
				return MigrationPlan{}, err
			}
			plan.Changes = append(plan.Changes, SchemaChange{
				Type:        "add_index",
				Description: fmt.Sprintf("add index on %s(%s)", desired.Table, strings.Join(idx.Columns, ",")),
				SQL:         idxSQL,
				Breaking:    false,
			})
		}
		return plan, nil
	}

	// Build column lookup for live schema.
	liveByName := make(map[string]ColumnDef, len(live.Columns))
	for _, c := range live.Columns {
		liveByName[c.Name] = c
	}
	desiredByName := make(map[string]ColumnDef, len(desired.Columns))
	for _, c := range desired.Columns {
		desiredByName[c.Name] = c
	}

	// Additions and type changes.
	for _, dc := range desired.Columns {
		lc, exists := liveByName[dc.Name]
		if !exists {
			colSQL, err := buildColumnDef(dc)
			if err != nil {
				return MigrationPlan{}, err
			}
			plan.Changes = append(plan.Changes, SchemaChange{
				Type:        "add_column",
				Description: fmt.Sprintf("add column %s.%s %s", desired.Table, dc.Name, dc.Type),
				SQL:         fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", desired.Table, colSQL),
				Breaking:    false,
			})
			continue
		}
		// Compare types.
		if !strings.EqualFold(lc.Type, dc.Type) {
			breaking := isNarrowingChange(lc.Type, dc.Type)
			changeType := "widen_type"
			if breaking {
				changeType = "narrow_type"
				plan.Safe = false
			}
			plan.Changes = append(plan.Changes, SchemaChange{
				Type:        changeType,
				Description: fmt.Sprintf("change %s.%s type from %s to %s", desired.Table, dc.Name, lc.Type, dc.Type),
				SQL: func() string {
					if err := validateSQLType(dc.Type); err != nil {
						return fmt.Sprintf("-- INVALID TYPE: %s", dc.Type)
					}
					return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s;", desired.Table, dc.Name, dc.Type)
				}(),
				Breaking:    breaking,
			})
		}
	}

	// Drops — breaking.
	for _, lc := range live.Columns {
		if _, ok := desiredByName[lc.Name]; !ok {
			plan.Safe = false
			plan.Changes = append(plan.Changes, SchemaChange{
				Type:        "drop_column",
				Description: fmt.Sprintf("drop column %s.%s", desired.Table, lc.Name),
				SQL:         fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", desired.Table, lc.Name),
				Breaking:    true,
			})
		}
	}

	// Index diffs.
	liveIndexKeys := make(map[string]bool, len(live.Indexes))
	for _, idx := range live.Indexes {
		liveIndexKeys[indexKey(idx)] = true
	}
	desiredIndexKeys := make(map[string]bool, len(desired.Indexes))
	for _, idx := range desired.Indexes {
		desiredIndexKeys[indexKey(idx)] = true
	}

	for _, idx := range desired.Indexes {
		if !liveIndexKeys[indexKey(idx)] {
			idxSQL, err := buildCreateIndex(desired.Table, idx)
			if err != nil {
				return MigrationPlan{}, err
			}
			plan.Changes = append(plan.Changes, SchemaChange{
				Type:        "add_index",
				Description: fmt.Sprintf("add index on %s(%s)", desired.Table, strings.Join(idx.Columns, ",")),
				SQL:         idxSQL,
				Breaking:    false,
			})
		}
	}
	for _, idx := range live.Indexes {
		if !desiredIndexKeys[indexKey(idx)] {
			idxName := idx.Name
			if idxName == "" {
				idxName = autoIndexName(desired.Table, idx.Columns)
			}
			plan.Safe = false
			plan.Changes = append(plan.Changes, SchemaChange{
				Type:        "drop_index",
				Description: fmt.Sprintf("drop index %s", idxName),
				SQL:         fmt.Sprintf("DROP INDEX IF EXISTS %s;", idxName),
				Breaking:    true,
			})
		}
	}

	return plan, nil
}

// buildCreateTable generates a CREATE TABLE statement for the given schema.
func buildCreateTable(def SchemaDefinition) (string, error) {
	var cols []string
	var pks []string
	for _, c := range def.Columns {
		colSQL, err := buildColumnDef(c)
		if err != nil {
			return "", err
		}
		cols = append(cols, "  "+colSQL)
		if c.PrimaryKey {
			pks = append(pks, c.Name)
		}
	}
	if len(pks) > 0 {
		cols = append(cols, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(pks, ", ")))
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n);", def.Table, strings.Join(cols, ",\n")), nil
}

// buildColumnDef generates the column definition fragment (without trailing comma).
func buildColumnDef(c ColumnDef) (string, error) {
	if err := validateIdentifier(c.Name); err != nil {
		return "", err
	}
	if err := validateSQLType(c.Type); err != nil {
		return "", fmt.Errorf("column %q: %w", c.Name, err)
	}
	if err := validateSQLDefault(c.Default); err != nil {
		return "", fmt.Errorf("column %q: %w", c.Name, err)
	}
	parts := []string{c.Name, c.Type}
	if !c.Nullable && !c.PrimaryKey {
		parts = append(parts, "NOT NULL")
	}
	if c.Unique {
		parts = append(parts, "UNIQUE")
	}
	if c.Default != "" {
		parts = append(parts, "DEFAULT "+c.Default)
	}
	return strings.Join(parts, " "), nil
}

// buildCreateIndex generates a CREATE INDEX statement.
func buildCreateIndex(table string, idx IndexDef) (string, error) {
	for _, col := range idx.Columns {
		if err := validateIdentifier(col); err != nil {
			return "", fmt.Errorf("index column %w", err)
		}
	}
	idxName := idx.Name
	if idxName == "" {
		idxName = autoIndexName(table, idx.Columns)
	}
	unique := ""
	if idx.Unique {
		unique = "UNIQUE "
	}
	return fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s);",
		unique, idxName, table, strings.Join(idx.Columns, ", ")), nil
}

// autoIndexName generates a deterministic index name from table and columns.
func autoIndexName(table string, columns []string) string {
	return fmt.Sprintf("idx_%s_%s", table, strings.Join(columns, "_"))
}

// indexKey returns a canonical key for deduplicating indexes.
func indexKey(idx IndexDef) string {
	return fmt.Sprintf("%v|%v", strings.Join(idx.Columns, ","), idx.Unique)
}

// isNarrowingChange returns true if changing from fromType to toType is a breaking (narrowing) change.
// Widening examples: varchar(100)→varchar(255), int→bigint, float→double precision.
func isNarrowingChange(fromType, toType string) bool {
	from := strings.ToLower(strings.TrimSpace(fromType))
	to := strings.ToLower(strings.TrimSpace(toType))

	// Extract varchar widths.
	fromWidth, fromIsVarchar := varcharWidth(from)
	toWidth, toIsVarchar := varcharWidth(to)
	if fromIsVarchar && toIsVarchar {
		return toWidth < fromWidth
	}

	// Numeric widening order.
	numericRank := map[string]int{
		"smallint": 1, "int2": 1,
		"int": 2, "integer": 2, "int4": 2,
		"bigint": 3, "int8": 3,
		"real": 4, "float4": 4,
		"float": 5, "double precision": 5, "float8": 5,
		"numeric": 6, "decimal": 6,
	}
	fromRank, fromOK := numericRank[from]
	toRank, toOK := numericRank[to]
	if fromOK && toOK {
		return toRank < fromRank
	}

	// Any other type change is treated as breaking.
	return true
}

// varcharWidth extracts width from varchar(N) / character varying(N).
func varcharWidth(t string) (int, bool) {
	t = strings.TrimSpace(t)
	for _, prefix := range []string{"varchar(", "character varying("} {
		if strings.HasPrefix(t, prefix) && strings.HasSuffix(t, ")") {
			inner := t[len(prefix) : len(t)-1]
			var n int
			_, err := fmt.Sscanf(inner, "%d", &n)
			if err == nil {
				return n, true
			}
		}
	}
	return 0, false
}
