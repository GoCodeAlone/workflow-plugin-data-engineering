package timeseries

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// ClickHouseConfig holds configuration for the timeseries.clickhouse module.
type ClickHouseConfig struct {
	Endpoints    []string      `json:"endpoints"    yaml:"endpoints"`
	Database     string        `json:"database"     yaml:"database"`
	Username     string        `json:"username"     yaml:"username"`
	Password     string        `json:"password"     yaml:"password"`
	MaxOpenConns int           `json:"maxOpenConns" yaml:"maxOpenConns"`
	MaxIdleConns int           `json:"maxIdleConns" yaml:"maxIdleConns"`
	Compression  string        `json:"compression"  yaml:"compression"`
	Secure       bool          `json:"secure"       yaml:"secure"`
	DialTimeout  time.Duration `json:"dialTimeout"  yaml:"dialTimeout"`
}

// clickhouseOpener is a function type for creating clickhouse connections.
// Injected for testing.
type clickhouseOpener func(opt *clickhouse.Options) (driver.Conn, error)

// ClickHouseModule is the timeseries.clickhouse module.
type ClickHouseModule struct {
	mu     sync.RWMutex
	name   string
	config ClickHouseConfig
	conn   driver.Conn
	opener clickhouseOpener
}

// NewClickHouseModule creates a new timeseries.clickhouse module instance.
func NewClickHouseModule(name string, config map[string]any) (sdk.ModuleInstance, error) {
	cfg, err := parseClickHouseConfig(config)
	if err != nil {
		return nil, fmt.Errorf("timeseries.clickhouse %q: %w", name, err)
	}
	return &ClickHouseModule{
		name:   name,
		config: cfg,
		opener: clickhouse.Open,
	}, nil
}

// Init validates the module configuration.
func (m *ClickHouseModule) Init() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.config.Endpoints) == 0 {
		return fmt.Errorf("timeseries.clickhouse %q: endpoints is required", m.name)
	}
	return nil
}

// Start opens the ClickHouse connection, pings the server, and registers.
func (m *ClickHouseModule) Start(ctx context.Context) error {
	opts := m.buildOptions()
	conn, err := m.opener(opts)
	if err != nil {
		return fmt.Errorf("timeseries.clickhouse %q: open: %w", m.name, err)
	}

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return fmt.Errorf("timeseries.clickhouse %q: ping: %w", m.name, err)
	}

	m.mu.Lock()
	m.conn = conn
	m.mu.Unlock()

	if err := Register(m.name, m); err != nil {
		_ = conn.Close()
		return fmt.Errorf("timeseries.clickhouse %q: register: %w", m.name, err)
	}
	return nil
}

// Stop closes the connection and deregisters.
func (m *ClickHouseModule) Stop(_ context.Context) error {
	Unregister(m.name)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn != nil {
		return m.conn.Close()
	}
	return nil
}

// Conn returns the underlying ClickHouse connection.
func (m *ClickHouseModule) Conn() driver.Conn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.conn
}

// WritePoint implements TimeSeriesWriter with a single INSERT.
func (m *ClickHouseModule) WritePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]any, timestamp time.Time) error {
	m.mu.RLock()
	conn := m.conn
	m.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("timeseries.clickhouse %q: not started", m.name)
	}

	tagKeys, tagVals := mapToSortedSlices(tags)
	fieldKeys, fieldVals := anyMapToSortedSlices(fields)

	table := sanitizeIdentifier(measurement)
	query := fmt.Sprintf(
		`INSERT INTO %s (ts, tag_keys, tag_values, field_keys, field_values) VALUES (?, ?, ?, ?, ?)`,
		table,
	)
	if err := conn.Exec(ctx, query, timestamp, tagKeys, tagVals, fieldKeys, fieldVals); err != nil {
		return fmt.Errorf("timeseries.clickhouse %q: write to %q: %w", m.name, measurement, err)
	}
	return nil
}

// WriteBatch implements TimeSeriesWriter using ClickHouse's native batch protocol.
func (m *ClickHouseModule) WriteBatch(ctx context.Context, points []Point) error {
	if len(points) == 0 {
		return nil
	}
	m.mu.RLock()
	conn := m.conn
	m.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("timeseries.clickhouse %q: not started", m.name)
	}

	// Group points by measurement for efficient batching.
	byMeasurement := make(map[string][]Point)
	for _, p := range points {
		byMeasurement[p.Measurement] = append(byMeasurement[p.Measurement], p)
	}

	for measurement, pts := range byMeasurement {
		table := sanitizeIdentifier(measurement)
		query := fmt.Sprintf(
			`INSERT INTO %s (ts, tag_keys, tag_values, field_keys, field_values)`,
			table,
		)
		batch, err := conn.PrepareBatch(ctx, query)
		if err != nil {
			return fmt.Errorf("timeseries.clickhouse %q: prepare batch for %q: %w", m.name, measurement, err)
		}
		for _, pt := range pts {
			tagKeys, tagVals := mapToSortedSlices(pt.Tags)
			fieldKeys, fieldVals := anyMapToSortedSlices(pt.Fields)
			if err := batch.Append(pt.Timestamp, tagKeys, tagVals, fieldKeys, fieldVals); err != nil {
				return fmt.Errorf("timeseries.clickhouse %q: batch append for %q: %w", m.name, measurement, err)
			}
		}
		if err := batch.Send(); err != nil {
			return fmt.Errorf("timeseries.clickhouse %q: batch send for %q: %w", m.name, measurement, err)
		}
	}
	return nil
}

// Query implements TimeSeriesWriter by executing a SQL query and scanning results.
func (m *ClickHouseModule) Query(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	m.mu.RLock()
	conn := m.conn
	m.mu.RUnlock()
	if conn == nil {
		return nil, fmt.Errorf("timeseries.clickhouse %q: not started", m.name)
	}

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("timeseries.clickhouse %q: query: %w", m.name, err)
	}
	defer rows.Close()

	cols := rows.ColumnTypes()
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.Name()
	}

	var result []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("timeseries.clickhouse %q: scan: %w", m.name, err)
		}
		row := make(map[string]any, len(cols))
		for i, col := range colNames {
			row[col] = vals[i]
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("timeseries.clickhouse %q: rows: %w", m.name, err)
	}
	if result == nil {
		result = []map[string]any{}
	}
	return result, nil
}

func (m *ClickHouseModule) buildOptions() *clickhouse.Options {
	opts := &clickhouse.Options{
		Addr: m.config.Endpoints,
		Auth: clickhouse.Auth{
			Database: m.config.Database,
			Username: m.config.Username,
			Password: m.config.Password,
		},
		MaxOpenConns:  m.config.MaxOpenConns,
		MaxIdleConns:  m.config.MaxIdleConns,
	}
	if m.config.DialTimeout > 0 {
		opts.DialTimeout = m.config.DialTimeout
	}
	if m.config.Secure {
		opts.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	switch strings.ToLower(m.config.Compression) {
	case "lz4":
		opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
	case "zstd":
		opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionZSTD}
	}
	return opts
}

func parseClickHouseConfig(config map[string]any) (ClickHouseConfig, error) {
	var cfg ClickHouseConfig
	switch ep := config["endpoints"].(type) {
	case []string:
		cfg.Endpoints = ep
	case []any:
		for _, v := range ep {
			if s, ok := v.(string); ok {
				cfg.Endpoints = append(cfg.Endpoints, s)
			}
		}
	}
	if v, ok := config["database"].(string); ok {
		cfg.Database = v
	}
	if v, ok := config["username"].(string); ok {
		cfg.Username = v
	}
	if v, ok := config["password"].(string); ok {
		cfg.Password = v
	}
	if v, ok := config["maxOpenConns"]; ok {
		cfg.MaxOpenConns = toInt(v)
	}
	if v, ok := config["maxIdleConns"]; ok {
		cfg.MaxIdleConns = toInt(v)
	}
	if v, ok := config["compression"].(string); ok {
		cfg.Compression = v
	}
	if v, ok := config["secure"].(bool); ok {
		cfg.Secure = v
	}
	if v, ok := config["dialTimeout"]; ok {
		switch d := v.(type) {
		case string:
			parsed, err := time.ParseDuration(d)
			if err != nil {
				return cfg, fmt.Errorf("invalid dialTimeout %q: %w", d, err)
			}
			cfg.DialTimeout = parsed
		case time.Duration:
			cfg.DialTimeout = d
		}
	}
	if len(cfg.Endpoints) == 0 {
		return cfg, fmt.Errorf("endpoints is required")
	}
	return cfg, nil
}

func mapToSortedSlices(m map[string]string) ([]string, []string) {
	if len(m) == 0 {
		return []string{}, []string{}
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Stable sort for consistent ordering.
	sortStrings(keys)
	vals := make([]string, len(keys))
	for i, k := range keys {
		vals[i] = m[k]
	}
	return keys, vals
}

func anyMapToSortedSlices(m map[string]any) ([]string, []string) {
	if len(m) == 0 {
		return []string{}, []string{}
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sortStrings(keys)
	vals := make([]string, len(keys))
	for i, k := range keys {
		vals[i] = fmt.Sprintf("%v", m[k])
	}
	return keys, vals
}

func sortStrings(ss []string) {
	for i := 1; i < len(ss); i++ {
		for j := i; j > 0 && ss[j] < ss[j-1]; j-- {
			ss[j], ss[j-1] = ss[j-1], ss[j]
		}
	}
}
