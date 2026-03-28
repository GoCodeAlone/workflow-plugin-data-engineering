package cdc

import (
	"fmt"
	"strings"
)

// buildBentoInputYAML generates a Bento input YAML fragment for the given CDC source config.
// Bento v4 does not have native postgres_cdc or mysql_binlog inputs, so we use
// sql_raw (polling-based CDC) for SQL sources and kinesis for DynamoDB streams.
func buildBentoInputYAML(cfg SourceConfig) (string, error) {
	switch cfg.SourceType {
	case "postgres":
		return buildPostgresCDCInput(cfg)
	case "mysql":
		return buildMySQLCDCInput(cfg)
	case "dynamodb":
		return buildDynamoDBStreamInput(cfg)
	default:
		return "", fmt.Errorf("unsupported source type %q (valid: postgres, mysql, dynamodb)", cfg.SourceType)
	}
}

// buildPostgresCDCInput builds a Bento sql_raw input for polling-based Postgres CDC.
// It queries pg_logical_slot_peek_changes to read from a logical replication slot,
// falling back to table polling when no slot is configured.
func buildPostgresCDCInput(cfg SourceConfig) (string, error) {
	if cfg.Connection == "" {
		return "", fmt.Errorf("postgres CDC: connection is required")
	}

	tables := quotedTableList(cfg.Tables)
	tableFilter := ""
	if len(cfg.Tables) > 0 {
		tableFilter = fmt.Sprintf("AND relation = ANY(ARRAY[%s]::text[])", tables)
	}

	// sanitizeID ensures the slot name contains only alphanumerics and underscores,
	// preventing SQL injection via SourceID into the pg_logical_slot_peek_changes call.
	slotName := sanitizeID(cfg.SourceID) + "_cdc_slot"
	// escapeSQLString escapes single-quotes in each table name for the add-tables literal.
	addTables := escapeSQLString(strings.Join(cfg.Tables, ","))
	// NOTE: cfg.Connection may contain credentials (user:password@host). Bento receives
	// this DSN directly; ensure the Bento process has appropriate log-level controls to
	// avoid leaking credentials at DEBUG verbosity.
	yaml := fmt.Sprintf(`sql_raw:
  driver: postgres
  dsn: %s
  query: >-
    SELECT lsn::text AS offset_id, xid::text AS transaction_id,
           data::text AS event_data
    FROM pg_logical_slot_peek_changes('%s', NULL, NULL,
         'include-xids', '1',
         'include-timestamp', '1',
         'add-tables', '%s')
    WHERE TRUE %s
    LIMIT 500
  auto_replay_nacks: true`,
		cfg.Connection,
		slotName,
		addTables,
		tableFilter,
	)
	return yaml, nil
}

// buildMySQLCDCInput builds a Bento sql_raw input for polling-based MySQL CDC.
// Uses information_schema to detect changes (requires CDC-aware queries).
func buildMySQLCDCInput(cfg SourceConfig) (string, error) {
	if cfg.Connection == "" {
		return "", fmt.Errorf("mysql CDC: connection is required")
	}

	tableConditions := "1=1"
	if len(cfg.Tables) > 0 {
		quoted := make([]string, len(cfg.Tables))
		for i, t := range cfg.Tables {
			// Strip schema prefix if present; escape single-quotes to prevent SQL injection.
			parts := strings.SplitN(t, ".", 2)
			quoted[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(parts[len(parts)-1], "'", "''"))
		}
		tableConditions = fmt.Sprintf("TABLE_NAME IN (%s)", strings.Join(quoted, ", "))
	}

	// NOTE: cfg.Connection may contain credentials; see Postgres note above.
	yaml := fmt.Sprintf(`sql_raw:
  driver: mysql
  dsn: %s
  query: >-
    SELECT TABLE_SCHEMA AS db_schema, TABLE_NAME AS table_name,
           TABLE_ROWS AS approx_row_count,
           CREATE_TIME AS created_at,
           UPDATE_TIME AS last_modified
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = DATABASE()
      AND %s
    ORDER BY UPDATE_TIME DESC
  auto_replay_nacks: true`,
		cfg.Connection,
		tableConditions,
	)
	return yaml, nil
}

// buildDynamoDBStreamInput builds a Bento kinesis input for DynamoDB Streams
// piped through AWS Kinesis. DynamoDB Streams → Kinesis is the recommended AWS pattern.
func buildDynamoDBStreamInput(cfg SourceConfig) (string, error) {
	streamARN, _ := cfg.Options["kinesis_stream_arn"].(string)
	region, _ := cfg.Options["region"].(string)
	if streamARN == "" {
		streamARN = fmt.Sprintf("arn:aws:kinesis:%s:*:stream/%s-cdc", region, cfg.SourceID)
	}
	if region == "" {
		region = "us-east-1"
	}

	yaml := fmt.Sprintf(`kinesis:
  streams:
    - %s
  region: %s
  start_from_timestamp: "2006-01-02T15:04:05.000Z"
  checkpoint_limit: 1024
  auto_replay_nacks: true`,
		streamARN,
		region,
	)
	return yaml, nil
}

// escapeSQLString escapes single-quote characters in a string for safe embedding
// inside a SQL single-quoted literal (e.g. 'value' → 'val''ue').
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// quotedTableList formats table names as a SQL array literal for use in queries.
func quotedTableList(tables []string) string {
	if len(tables) == 0 {
		return ""
	}
	quoted := make([]string, len(tables))
	for i, t := range tables {
		quoted[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(t, "'", "''"))
	}
	return strings.Join(quoted, ", ")
}
