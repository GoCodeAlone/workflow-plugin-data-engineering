package timeseries

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver for database/sql
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// TimescaleConfig holds configuration for the timeseries.timescaledb module.
type TimescaleConfig struct {
	Connection    string              `json:"connection"    yaml:"connection"`
	MaxOpenConns  int                 `json:"maxOpenConns"  yaml:"maxOpenConns"`
	MaxIdleConns  int                 `json:"maxIdleConns"  yaml:"maxIdleConns"`
	Hypertables   []HypertableConfig  `json:"hypertables"   yaml:"hypertables"`
}

// HypertableConfig describes a TimescaleDB hypertable to create on Start.
type HypertableConfig struct {
	Table         string `json:"table"         yaml:"table"`
	TimeColumn    string `json:"timeColumn"    yaml:"timeColumn"`
	ChunkInterval string `json:"chunkInterval" yaml:"chunkInterval"`
}

// TimescaleModule is the timeseries.timescaledb module.
type TimescaleModule struct {
	mu     sync.RWMutex
	name   string
	config TimescaleConfig
	db     *sql.DB
}

// NewTimescaleModule creates a new timeseries.timescaledb module instance.
func NewTimescaleModule(name string, config map[string]any) (sdk.ModuleInstance, error) {
	cfg, err := parseTimescaleConfig(config)
	if err != nil {
		return nil, fmt.Errorf("timeseries.timescaledb %q: %w", name, err)
	}
	return &TimescaleModule{name: name, config: cfg}, nil
}

// Init validates the module configuration.
func (m *TimescaleModule) Init() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.config.Connection == "" {
		return fmt.Errorf("timeseries.timescaledb %q: connection is required", m.name)
	}
	return nil
}

// Start opens the database connection, verifies the TimescaleDB extension, and creates configured hypertables.
func (m *TimescaleModule) Start(ctx context.Context) error {
	db, err := sql.Open("pgx", m.config.Connection)
	if err != nil {
		return fmt.Errorf("timeseries.timescaledb %q: open db: %w", m.name, err)
	}
	if m.config.MaxOpenConns > 0 {
		db.SetMaxOpenConns(m.config.MaxOpenConns)
	}
	if m.config.MaxIdleConns > 0 {
		db.SetMaxIdleConns(m.config.MaxIdleConns)
	}

	m.mu.Lock()
	m.db = db
	m.mu.Unlock()

	if err := m.checkExtension(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("timeseries.timescaledb %q: %w", m.name, err)
	}

	for _, ht := range m.config.Hypertables {
		if err := m.CreateHypertable(ctx, ht.Table, ht.TimeColumn, ht.ChunkInterval); err != nil {
			_ = db.Close()
			return fmt.Errorf("timeseries.timescaledb %q: create hypertable %q: %w", m.name, ht.Table, err)
		}
	}

	if err := Register(m.name, m); err != nil {
		_ = db.Close()
		return fmt.Errorf("timeseries.timescaledb %q: register: %w", m.name, err)
	}
	return nil
}

// Stop closes the database connection and deregisters.
func (m *TimescaleModule) Stop(_ context.Context) error {
	Unregister(m.name)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

// DB returns the underlying *sql.DB.
func (m *TimescaleModule) DB() *sql.DB {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.db
}

// CreateHypertable creates a TimescaleDB hypertable if it doesn't already exist.
func (m *TimescaleModule) CreateHypertable(ctx context.Context, table, timeColumn, chunkInterval string) error {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()

	interval := chunkInterval
	if interval == "" {
		interval = "1d"
	}
	// Convert short notation (1d, 7d) to interval string PostgreSQL understands.
	pgInterval := toPostgresInterval(interval)

	_, err := db.ExecContext(ctx,
		`SELECT create_hypertable($1, $2, chunk_time_interval => $3::interval, if_not_exists => TRUE)`,
		table, timeColumn, pgInterval,
	)
	if err != nil {
		return fmt.Errorf("create_hypertable(%q): %w", table, err)
	}
	return nil
}

// WritePoint implements TimeSeriesWriter.
// Inserts a single row into the measurement table using a JSONB schema.
func (m *TimescaleModule) WritePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]any, timestamp time.Time) error {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return fmt.Errorf("timeseries.timescaledb %q: not started", m.name)
	}

	tagsJSON, err := json.Marshal(tags)
	if err != nil {
		return fmt.Errorf("timeseries.timescaledb %q: marshal tags: %w", m.name, err)
	}
	fieldsJSON, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("timeseries.timescaledb %q: marshal fields: %w", m.name, err)
	}

	query := fmt.Sprintf(`INSERT INTO %q ("time","tags","fields") VALUES ($1,$2::jsonb,$3::jsonb)`,
		sanitizeIdentifier(measurement))
	_, err = db.ExecContext(ctx, query, timestamp, string(tagsJSON), string(fieldsJSON))
	if err != nil {
		return fmt.Errorf("timeseries.timescaledb %q: write to %q: %w", m.name, measurement, err)
	}
	return nil
}

// WriteBatch implements TimeSeriesWriter.
func (m *TimescaleModule) WriteBatch(ctx context.Context, points []Point) error {
	for _, p := range points {
		if err := m.WritePoint(ctx, p.Measurement, p.Tags, p.Fields, p.Timestamp); err != nil {
			return err
		}
	}
	return nil
}

// Query implements TimeSeriesWriter using standard SQL.
func (m *TimescaleModule) Query(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("timeseries.timescaledb %q: not started", m.name)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("timeseries.timescaledb %q: query: %w", m.name, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("timeseries.timescaledb %q: columns: %w", m.name, err)
	}

	var result []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("timeseries.timescaledb %q: scan: %w", m.name, err)
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = vals[i]
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("timeseries.timescaledb %q: rows: %w", m.name, err)
	}
	if result == nil {
		result = []map[string]any{}
	}
	return result, nil
}

func (m *TimescaleModule) checkExtension(ctx context.Context) error {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()

	var extVersion string
	err := db.QueryRowContext(ctx,
		`SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'`).Scan(&extVersion)
	if err == sql.ErrNoRows {
		return fmt.Errorf("timescaledb extension not installed")
	}
	if err != nil {
		return fmt.Errorf("check timescaledb extension: %w", err)
	}
	return nil
}

// sanitizeIdentifier strips any characters that aren't alphanumeric or underscore.
// The identifier is then safely quoted in the query.
func sanitizeIdentifier(s string) string {
	var sb strings.Builder
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			sb.WriteRune(c)
		}
	}
	return sb.String()
}

func toPostgresInterval(s string) string {
	// Convert "1d" -> "1 day", "7d" -> "7 days", "1h" -> "1 hour", etc.
	if strings.HasSuffix(s, "d") {
		n := strings.TrimSuffix(s, "d")
		if n == "1" {
			return "1 day"
		}
		return n + " days"
	}
	if strings.HasSuffix(s, "h") {
		n := strings.TrimSuffix(s, "h")
		return n + " hours"
	}
	return s
}

func parseTimescaleConfig(config map[string]any) (TimescaleConfig, error) {
	var cfg TimescaleConfig
	if v, ok := config["connection"].(string); ok {
		cfg.Connection = v
	}
	if v, ok := config["maxOpenConns"]; ok {
		cfg.MaxOpenConns = toInt(v)
	}
	if v, ok := config["maxIdleConns"]; ok {
		cfg.MaxIdleConns = toInt(v)
	}
	if raw, ok := config["hypertables"].([]any); ok {
		for _, item := range raw {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			var ht HypertableConfig
			if v, ok := m["table"].(string); ok {
				ht.Table = v
			}
			if v, ok := m["timeColumn"].(string); ok {
				ht.TimeColumn = v
			}
			if v, ok := m["chunkInterval"].(string); ok {
				ht.ChunkInterval = v
			}
			if ht.Table != "" {
				cfg.Hypertables = append(cfg.Hypertables, ht)
			}
		}
	}
	if cfg.Connection == "" {
		return cfg, fmt.Errorf("connection is required")
	}
	return cfg, nil
}

func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	case int64:
		return int(n)
	}
	return 0
}
