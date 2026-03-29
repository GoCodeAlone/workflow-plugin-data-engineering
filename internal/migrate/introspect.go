package migrate

import (
	"context"
	"fmt"
	"strings"
)

// IntrospectSchema queries the live database for the current schema of tableName.
// Uses information_schema.columns and pg_indexes to build a SchemaDefinition.
// Returns nil (not an error) if the table does not exist.
func IntrospectSchema(ctx context.Context, exec SQLExecutor, tableName string) (*SchemaDefinition, error) {
	if err := validateIdentifier(tableName); err != nil {
		return nil, fmt.Errorf("introspect: table %w", err)
	}

	// Check if the table exists.
	existsRows, err := exec.QueryContext(ctx, `
SELECT COUNT(*) FROM information_schema.tables
WHERE table_name = $1 AND table_schema NOT IN ('pg_catalog', 'information_schema')`,
		tableName)
	if err != nil {
		return nil, fmt.Errorf("introspect: check table existence: %w", err)
	}
	defer existsRows.Close()
	var tableCount int
	if existsRows.Next() {
		if err := existsRows.Scan(&tableCount); err != nil {
			return nil, fmt.Errorf("introspect: scan table count: %w", err)
		}
	}
	if err := existsRows.Err(); err != nil {
		return nil, err
	}
	if tableCount == 0 {
		return nil, nil
	}

	// Fetch columns.
	colRows, err := exec.QueryContext(ctx, `
SELECT
  column_name,
  data_type,
  is_nullable,
  column_default
FROM information_schema.columns
WHERE table_name = $1 AND table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY ordinal_position`,
		tableName)
	if err != nil {
		return nil, fmt.Errorf("introspect: query columns: %w", err)
	}
	defer colRows.Close()

	def := SchemaDefinition{Table: tableName}
	for colRows.Next() {
		var colName, dataType, isNullable string
		var colDefault *string
		if err := colRows.Scan(&colName, &dataType, &isNullable, &colDefault); err != nil {
			return nil, fmt.Errorf("introspect: scan column: %w", err)
		}
		col := ColumnDef{
			Name:     colName,
			Type:     normalizeType(dataType),
			Nullable: strings.EqualFold(isNullable, "YES"),
		}
		if colDefault != nil {
			col.Default = *colDefault
		}
		def.Columns = append(def.Columns, col)
	}
	if err := colRows.Err(); err != nil {
		return nil, err
	}

	// Fetch indexes from pg_indexes.
	idxRows, err := exec.QueryContext(ctx, `
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = $1`,
		tableName)
	if err != nil {
		return nil, fmt.Errorf("introspect: query indexes: %w", err)
	}
	defer idxRows.Close()

	for idxRows.Next() {
		var idxName, idxDef string
		if err := idxRows.Scan(&idxName, &idxDef); err != nil {
			return nil, fmt.Errorf("introspect: scan index: %w", err)
		}
		idx := parseIndexDef(idxName, idxDef)
		def.Indexes = append(def.Indexes, idx)
	}
	if err := idxRows.Err(); err != nil {
		return nil, err
	}

	return &def, nil
}

// normalizeType maps Postgres information_schema type names to canonical forms.
func normalizeType(t string) string {
	switch strings.ToLower(t) {
	case "character varying":
		return "varchar"
	case "integer":
		return "int"
	case "bigint":
		return "bigint"
	case "boolean":
		return "boolean"
	case "double precision":
		return "double precision"
	case "timestamp with time zone":
		return "timestamptz"
	case "timestamp without time zone":
		return "timestamp"
	default:
		return t
	}
}

// parseIndexDef extracts a rough IndexDef from a Postgres CREATE INDEX definition string.
// Example: "CREATE UNIQUE INDEX idx_users_email ON users USING btree (email)"
func parseIndexDef(name, def string) IndexDef {
	idx := IndexDef{Name: name}
	upper := strings.ToUpper(def)
	if strings.Contains(upper, "UNIQUE") {
		idx.Unique = true
	}
	// Extract column list from the last parentheses.
	open := strings.LastIndex(def, "(")
	close := strings.LastIndex(def, ")")
	if open >= 0 && close > open {
		colStr := def[open+1 : close]
		for _, col := range strings.Split(colStr, ",") {
			col = strings.TrimSpace(col)
			// Strip any direction or operator class (e.g. "email ASC" → "email").
			parts := strings.Fields(col)
			if len(parts) > 0 {
				idx.Columns = append(idx.Columns, parts[0])
			}
		}
	}
	return idx
}
