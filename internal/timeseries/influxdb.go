package timeseries

import (
	"context"
	"fmt"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	influxdomain "github.com/influxdata/influxdb-client-go/v2/domain"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// InfluxConfig holds configuration for the timeseries.influxdb module.
type InfluxConfig struct {
	URL           string        `json:"url"           yaml:"url"`
	Token         string        `json:"token"         yaml:"token"`
	Org           string        `json:"org"           yaml:"org"`
	Bucket        string        `json:"bucket"        yaml:"bucket"`
	BatchSize     uint          `json:"batchSize"     yaml:"batchSize"`
	FlushInterval time.Duration `json:"flushInterval" yaml:"flushInterval"`
	Precision     string        `json:"precision"     yaml:"precision"`
}

// InfluxModule is the timeseries.influxdb module.
type InfluxModule struct {
	mu       sync.RWMutex
	name     string
	config   InfluxConfig
	client   influxdb2.Client
	writeAPI api.WriteAPIBlocking
	queryAPI api.QueryAPI
}

// NewInfluxModule creates a new timeseries.influxdb module instance.
func NewInfluxModule(name string, config map[string]any) (sdk.ModuleInstance, error) {
	cfg, err := parseInfluxConfig(config)
	if err != nil {
		return nil, fmt.Errorf("timeseries.influxdb %q: %w", name, err)
	}
	return &InfluxModule{name: name, config: cfg}, nil
}

// Init validates the module configuration.
func (m *InfluxModule) Init() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.config.URL == "" {
		return fmt.Errorf("timeseries.influxdb %q: url is required", m.name)
	}
	if m.config.Token == "" {
		return fmt.Errorf("timeseries.influxdb %q: token is required", m.name)
	}
	if m.config.Org == "" {
		return fmt.Errorf("timeseries.influxdb %q: org is required", m.name)
	}
	if m.config.Bucket == "" {
		return fmt.Errorf("timeseries.influxdb %q: bucket is required", m.name)
	}
	return nil
}

// Start creates the InfluxDB client, pings the server, and registers the module.
func (m *InfluxModule) Start(ctx context.Context) error {
	opts := influxdb2.DefaultOptions()
	if m.config.BatchSize > 0 {
		opts.SetBatchSize(m.config.BatchSize)
	}
	if m.config.FlushInterval > 0 {
		opts.SetFlushInterval(uint(m.config.FlushInterval.Milliseconds()))
	}
	opts.SetPrecision(influxPrecision(m.config.Precision))

	m.mu.Lock()
	m.client = influxdb2.NewClientWithOptions(m.config.URL, m.config.Token, opts)
	m.writeAPI = m.client.WriteAPIBlocking(m.config.Org, m.config.Bucket)
	m.queryAPI = m.client.QueryAPI(m.config.Org)
	m.mu.Unlock()

	ok, err := m.client.Ping(ctx)
	if err != nil {
		return fmt.Errorf("timeseries.influxdb %q: ping: %w", m.name, err)
	}
	if !ok {
		return fmt.Errorf("timeseries.influxdb %q: ping returned not-ok", m.name)
	}

	if err := Register(m.name, m); err != nil {
		m.client.Close()
		return fmt.Errorf("timeseries.influxdb %q: register: %w", m.name, err)
	}
	return nil
}

// Stop closes the InfluxDB client (flushes pending writes) and deregisters.
func (m *InfluxModule) Stop(_ context.Context) error {
	Unregister(m.name)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.client != nil {
		m.client.Close() //nolint:staticcheck
	}
	return nil
}

// WriteAPI returns the non-blocking write API for direct use.
func (m *InfluxModule) WriteAPI() api.WriteAPI {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.client.WriteAPI(m.config.Org, m.config.Bucket)
}

// QueryAPI returns the query API for direct use.
func (m *InfluxModule) QueryAPI() api.QueryAPI {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.queryAPI
}

// WritePoint implements TimeSeriesWriter.
func (m *InfluxModule) WritePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]any, timestamp time.Time) error {
	m.mu.RLock()
	wapi := m.writeAPI
	m.mu.RUnlock()
	if wapi == nil {
		return fmt.Errorf("timeseries.influxdb %q: not started", m.name)
	}
	p := write.NewPoint(measurement, tags, fields, timestamp)
	if err := wapi.WritePoint(ctx, p); err != nil {
		return fmt.Errorf("timeseries.influxdb %q: write: %w", m.name, err)
	}
	return nil
}

// WriteBatch implements TimeSeriesWriter.
func (m *InfluxModule) WriteBatch(ctx context.Context, points []Point) error {
	m.mu.RLock()
	wapi := m.writeAPI
	m.mu.RUnlock()
	if wapi == nil {
		return fmt.Errorf("timeseries.influxdb %q: not started", m.name)
	}
	ps := make([]*write.Point, len(points))
	for i, pt := range points {
		ps[i] = write.NewPoint(pt.Measurement, pt.Tags, pt.Fields, pt.Timestamp)
	}
	if err := wapi.WritePoint(ctx, ps...); err != nil {
		return fmt.Errorf("timeseries.influxdb %q: batch write: %w", m.name, err)
	}
	return nil
}

// Query implements TimeSeriesWriter using the Flux query language.
func (m *InfluxModule) Query(ctx context.Context, query string, _ ...any) ([]map[string]any, error) {
	m.mu.RLock()
	qapi := m.queryAPI
	org := m.config.Org
	m.mu.RUnlock()
	if qapi == nil {
		return nil, fmt.Errorf("timeseries.influxdb %q: not started", m.name)
	}
	result, err := qapi.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("timeseries.influxdb %q: query: %w", m.name, err)
	}
	_ = org
	var rows []map[string]any
	for result.Next() {
		record := result.Record()
		row := map[string]any{
			"measurement": record.Measurement(),
			"field":       record.Field(),
			"value":       record.Value(),
			"time":        record.Time(),
		}
		for k, v := range record.Values() {
			if _, exists := row[k]; !exists {
				row[k] = v
			}
		}
		rows = append(rows, row)
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("timeseries.influxdb %q: query result: %w", m.name, err)
	}
	if rows == nil {
		rows = []map[string]any{}
	}
	return rows, nil
}

// BucketsAPI returns the buckets management API.
func (m *InfluxModule) BucketsAPI() api.BucketsAPI {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.client.BucketsAPI()
}

// UpdateBucketRetention updates the retention rules for a bucket.
func (m *InfluxModule) UpdateBucketRetention(ctx context.Context, bucketName string, durationSeconds int64) error {
	m.mu.RLock()
	bapi := m.client.BucketsAPI()
	m.mu.RUnlock()

	bucket, err := bapi.FindBucketByName(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("timeseries.influxdb %q: find bucket %q: %w", m.name, bucketName, err)
	}
	expireType := influxdomain.RetentionRuleType("expire")
	bucket.RetentionRules = influxdomain.RetentionRules{
		{Type: &expireType, EverySeconds: durationSeconds},
	}
	_, err = bapi.UpdateBucket(ctx, bucket)
	if err != nil {
		return fmt.Errorf("timeseries.influxdb %q: update bucket %q: %w", m.name, bucketName, err)
	}
	return nil
}

// Config returns the module configuration.
func (m *InfluxModule) Config() InfluxConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

func parseInfluxConfig(config map[string]any) (InfluxConfig, error) {
	var cfg InfluxConfig
	if v, ok := config["url"].(string); ok {
		cfg.URL = v
	}
	if v, ok := config["token"].(string); ok {
		cfg.Token = v
	}
	if v, ok := config["org"].(string); ok {
		cfg.Org = v
	}
	if v, ok := config["bucket"].(string); ok {
		cfg.Bucket = v
	}
	if v, ok := config["batchSize"]; ok {
		switch n := v.(type) {
		case int:
			cfg.BatchSize = uint(n)
		case float64:
			cfg.BatchSize = uint(n)
		case uint:
			cfg.BatchSize = n
		}
	}
	if v, ok := config["flushInterval"]; ok {
		switch d := v.(type) {
		case string:
			parsed, err := time.ParseDuration(d)
			if err != nil {
				return cfg, fmt.Errorf("invalid flushInterval %q: %w", d, err)
			}
			cfg.FlushInterval = parsed
		case time.Duration:
			cfg.FlushInterval = d
		}
	}
	if v, ok := config["precision"].(string); ok {
		cfg.Precision = v
	}
	if cfg.URL == "" {
		return cfg, fmt.Errorf("url is required")
	}
	return cfg, nil
}

func influxPrecision(p string) time.Duration {
	switch p {
	case "s":
		return time.Second
	case "ms":
		return time.Millisecond
	case "us":
		return time.Microsecond
	default:
		return time.Nanosecond
	}
}
