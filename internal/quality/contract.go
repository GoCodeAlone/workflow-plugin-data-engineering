package quality

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// DataContract describes a dataset's expected schema and quality assertions.
type DataContract struct {
	Dataset string         `json:"dataset" yaml:"dataset"`
	Owner   string         `json:"owner"   yaml:"owner"`
	Schema  ContractSchema `json:"schema"  yaml:"schema"`
	Quality []QualityCheck `json:"quality" yaml:"quality"`
}

// ContractSchema lists the columns expected in a dataset.
type ContractSchema struct {
	Columns []ContractColumn `json:"columns" yaml:"columns"`
}

// ContractColumn describes a single expected column.
type ContractColumn struct {
	Name     string `json:"name"               yaml:"name"`
	Type     string `json:"type"               yaml:"type"`
	Nullable bool   `json:"nullable,omitempty" yaml:"nullable,omitempty"`
	Pattern  string `json:"pattern,omitempty"  yaml:"pattern,omitempty"`
}

// ContractResult is the outcome of validating a dataset against a contract.
type ContractResult struct {
	Dataset        string        `json:"dataset"`
	Passed         bool          `json:"passed"`
	SchemaOK       bool          `json:"schemaOk"`
	QualityOK      bool          `json:"qualityOk"`
	SchemaErrors   []string      `json:"schemaErrors,omitempty"`
	QualityResults []CheckResult `json:"qualityResults"`
}

// ParseContract reads and parses a YAML data contract file.
func ParseContract(path string) (*DataContract, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("parse contract %q: %w", path, err)
	}
	var contract DataContract
	if err := yaml.Unmarshal(data, &contract); err != nil {
		return nil, fmt.Errorf("parse contract %q: yaml: %w", path, err)
	}
	if contract.Dataset == "" {
		return nil, fmt.Errorf("parse contract %q: dataset is required", path)
	}
	return &contract, nil
}

// ValidateContract runs schema and quality checks against a live dataset.
func ValidateContract(ctx context.Context, exec DBQuerier, contract DataContract) (*ContractResult, error) {
	result := &ContractResult{
		Dataset:  contract.Dataset,
		SchemaOK: true,
	}

	// Resolve schema and table names from dataset (e.g. "raw.users" → schema=raw, table=users).
	schema, table := splitDataset(contract.Dataset)
	qualTable := contract.Dataset // fully-qualified for SQL queries
	if schema == "" {
		qualTable = table
	}

	// Schema validation: compare contract columns against information_schema.
	if len(contract.Schema.Columns) > 0 {
		schemaErrors, err := validateContractSchema(ctx, exec, schema, table, contract.Schema.Columns)
		if err != nil {
			return nil, fmt.Errorf("validate contract %q schema: %w", contract.Dataset, err)
		}
		result.SchemaErrors = schemaErrors
		if len(schemaErrors) > 0 {
			result.SchemaOK = false
		}
	}

	// Quality checks.
	qualityOK := true
	for _, check := range contract.Quality {
		cr, err := RunCheck(ctx, exec, check.Type, qualTable, check.Config)
		if err != nil {
			return nil, fmt.Errorf("validate contract %q quality check %q: %w", contract.Dataset, check.Type, err)
		}
		result.QualityResults = append(result.QualityResults, *cr)
		if !cr.Passed {
			qualityOK = false
		}
	}
	result.QualityOK = qualityOK

	// Pattern checks on string columns.
	for _, col := range contract.Schema.Columns {
		if col.Pattern == "" {
			continue
		}
		re, err := regexp.Compile(col.Pattern)
		if err != nil {
			return nil, fmt.Errorf("validate contract %q column %q: invalid pattern: %w", contract.Dataset, col.Name, err)
		}
		if err := validateIdent(col.Name); err != nil {
			return nil, fmt.Errorf("validate contract %q column %q: %w", contract.Dataset, col.Name, err)
		}
		query := fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL", col.Name, qualTable, col.Name)
		rows, err := exec.QueryContext(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("validate contract %q pattern check column %q: %w", contract.Dataset, col.Name, err)
		}
		var violations int
		for rows.Next() {
			var val string
			if err := rows.Scan(&val); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("validate contract %q pattern check column %q: scan: %w", contract.Dataset, col.Name, err)
			}
			if !re.MatchString(val) {
				violations++
			}
		}
		_ = rows.Close()
		if rows.Err() != nil {
			return nil, fmt.Errorf("validate contract %q pattern check column %q: %w", contract.Dataset, col.Name, rows.Err())
		}
		cr := CheckResult{
			Check:   fmt.Sprintf("pattern(%s)", col.Name),
			Passed:  violations == 0,
			Message: fmt.Sprintf("pattern check on %s: %d violation(s)", col.Name, violations),
			Value:   violations,
		}
		result.QualityResults = append(result.QualityResults, cr)
		if !cr.Passed {
			result.QualityOK = false
		}
	}

	result.Passed = result.SchemaOK && result.QualityOK
	return result, nil
}

// validateContractSchema queries information_schema.columns and compares against contract columns.
func validateContractSchema(ctx context.Context, exec DBQuerier, schema, table string, expected []ContractColumn) ([]string, error) {
	rows, err := exec.QueryContext(ctx,
		"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
		schema, table,
	)
	if err != nil {
		return nil, fmt.Errorf("query information_schema: %w", err)
	}
	defer rows.Close()

	type liveCol struct {
		name     string
		dataType string
		nullable bool
	}
	live := map[string]liveCol{}
	for rows.Next() {
		var c liveCol
		var isNullable string
		if err := rows.Scan(&c.name, &c.dataType, &isNullable); err != nil {
			return nil, fmt.Errorf("scan information_schema row: %w", err)
		}
		c.nullable = strings.EqualFold(isNullable, "YES")
		live[c.name] = c
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read information_schema: %w", err)
	}

	var errs []string
	for _, col := range expected {
		lc, exists := live[col.Name]
		if !exists {
			errs = append(errs, fmt.Sprintf("column %q not found in %s.%s", col.Name, schema, table))
			continue
		}
		// Type check: normalize common SQL type aliases before comparing.
		if col.Type != "" {
			liveNorm := normalizeSQLType(lc.dataType)
			contractNorm := normalizeSQLType(col.Type)
			if !strings.Contains(liveNorm, contractNorm) && !strings.Contains(contractNorm, liveNorm) {
				errs = append(errs, fmt.Sprintf("column %q: expected type %q, got %q", col.Name, col.Type, lc.dataType))
			}
		}
		// Nullable check: if contract says not nullable but DB allows nulls.
		if !col.Nullable && lc.nullable {
			errs = append(errs, fmt.Sprintf("column %q: contract requires NOT NULL but column allows NULL", col.Name))
		}
	}
	return errs, nil
}

// sqlTypeAliases maps common short SQL type names to their canonical forms.
var sqlTypeAliases = map[string]string{
	"varchar":  "character varying",
	"int":      "integer",
	"int2":     "smallint",
	"int4":     "integer",
	"int8":     "bigint",
	"float4":   "real",
	"float8":   "double precision",
	"float":    "double precision",
	"bool":     "boolean",
	"timestamptz": "timestamp with time zone",
}

// normalizeSQLType lowercases and expands common SQL type aliases.
func normalizeSQLType(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	if canonical, ok := sqlTypeAliases[t]; ok {
		return canonical
	}
	return t
}

// splitDataset splits "schema.table" into (schema, table).
// Returns ("", table) if there is no dot.
func splitDataset(dataset string) (schema, table string) {
	parts := strings.SplitN(dataset, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", dataset
}
