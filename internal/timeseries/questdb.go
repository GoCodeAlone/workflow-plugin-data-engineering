package timeseries

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// SenderFactory creates a qdb.LineSender from config. Injectable for tests.
type SenderFactory func(ctx context.Context, cfg QuestDBConfig) (qdb.LineSender, error)

// QuestDBConfig holds configuration for the timeseries.questdb module.
type QuestDBConfig struct {
	ILPEndpoint   string        `json:"ilpEndpoint"   yaml:"ilpEndpoint"`
	HTTPEndpoint  string        `json:"httpEndpoint"  yaml:"httpEndpoint"`
	AuthToken     string        `json:"authToken"     yaml:"authToken"`
	TLSEnabled    bool          `json:"tlsEnabled"    yaml:"tlsEnabled"`
	AutoFlush     bool          `json:"autoFlush"     yaml:"autoFlush"`
	FlushInterval time.Duration `json:"flushInterval" yaml:"flushInterval"`
}

// QuestDBModule implements the timeseries.questdb module.
type QuestDBModule struct {
	mu            sync.RWMutex
	name          string
	config        QuestDBConfig
	sender        qdb.LineSender
	httpClient    *http.Client
	senderFactory SenderFactory
}

// NewQuestDBModule creates a new timeseries.questdb module instance.
func NewQuestDBModule(name string, config map[string]any) (sdk.ModuleInstance, error) {
	cfg, err := parseQuestDBConfig(config)
	if err != nil {
		return nil, fmt.Errorf("timeseries.questdb %q: %w", name, err)
	}
	return &QuestDBModule{
		name:          name,
		config:        cfg,
		httpClient:    &http.Client{Timeout: 30 * time.Second},
		senderFactory: defaultQuestDBSenderFactory,
	}, nil
}

func defaultQuestDBSenderFactory(ctx context.Context, cfg QuestDBConfig) (qdb.LineSender, error) {
	opts := []qdb.LineSenderOption{qdb.WithHttp(), qdb.WithAddress(cfg.ILPEndpoint)}
	if cfg.AuthToken != "" {
		opts = append(opts, qdb.WithBearerToken(cfg.AuthToken))
	}
	if cfg.TLSEnabled {
		opts = append(opts, qdb.WithTls())
	}
	if !cfg.AutoFlush {
		opts = append(opts, qdb.WithAutoFlushDisabled())
	}
	if cfg.FlushInterval > 0 {
		opts = append(opts, qdb.WithAutoFlushInterval(cfg.FlushInterval))
	}
	return qdb.NewLineSender(ctx, opts...)
}

// Init validates the module configuration.
func (m *QuestDBModule) Init() error {
	m.mu.RLock()
	cfg := m.config
	m.mu.RUnlock()
	if cfg.ILPEndpoint == "" {
		return fmt.Errorf("timeseries.questdb %q: ilpEndpoint is required", m.name)
	}
	if cfg.HTTPEndpoint == "" {
		return fmt.Errorf("timeseries.questdb %q: httpEndpoint is required", m.name)
	}
	return nil
}

// Start verifies HTTP health and initializes the ILP sender, then registers in the global registry.
func (m *QuestDBModule) Start(ctx context.Context) error {
	resp, err := m.httpClient.Get(m.config.HTTPEndpoint + "/")
	if err != nil {
		return fmt.Errorf("timeseries.questdb %q: health check: %w", m.name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return fmt.Errorf("timeseries.questdb %q: health check returned status %d", m.name, resp.StatusCode)
	}

	sender, err := m.senderFactory(ctx, m.config)
	if err != nil {
		return fmt.Errorf("timeseries.questdb %q: create ILP sender: %w", m.name, err)
	}

	m.mu.Lock()
	m.sender = sender
	m.mu.Unlock()

	if err := Register(m.name, m); err != nil {
		_ = m.Stop(ctx)
		return fmt.Errorf("timeseries.questdb %q: register: %w", m.name, err)
	}
	return nil
}

// Stop flushes pending messages, closes the ILP sender, and deregisters.
func (m *QuestDBModule) Stop(ctx context.Context) error {
	Unregister(m.name)

	m.mu.Lock()
	sender := m.sender
	m.sender = nil
	m.mu.Unlock()

	if sender == nil {
		return nil
	}
	if err := sender.Flush(ctx); err != nil {
		_ = sender.Close(ctx)
		return fmt.Errorf("timeseries.questdb %q: flush on stop: %w", m.name, err)
	}
	return sender.Close(ctx)
}

// WritePoint implements TimeSeriesWriter using the ILP protocol.
func (m *QuestDBModule) WritePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]any, ts time.Time) error {
	m.mu.RLock()
	sender := m.sender
	m.mu.RUnlock()
	if sender == nil {
		return fmt.Errorf("timeseries.questdb %q: not started", m.name)
	}
	w := &questDBWriter{sender: sender, httpClient: m.httpClient, httpEndpoint: m.config.HTTPEndpoint}
	return w.WritePoint(ctx, measurement, tags, fields, ts)
}

// WriteBatch implements TimeSeriesWriter.
func (m *QuestDBModule) WriteBatch(ctx context.Context, points []Point) error {
	m.mu.RLock()
	sender := m.sender
	m.mu.RUnlock()
	if sender == nil {
		return fmt.Errorf("timeseries.questdb %q: not started", m.name)
	}
	w := &questDBWriter{sender: sender, httpClient: m.httpClient, httpEndpoint: m.config.HTTPEndpoint}
	return w.WriteBatch(ctx, points)
}

// Query implements TimeSeriesWriter using the HTTP /exec endpoint.
func (m *QuestDBModule) Query(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	m.mu.RLock()
	sender := m.sender
	m.mu.RUnlock()
	if sender == nil {
		return nil, fmt.Errorf("timeseries.questdb %q: not started", m.name)
	}
	w := &questDBWriter{sender: sender, httpClient: m.httpClient, httpEndpoint: m.config.HTTPEndpoint}
	return w.Query(ctx, query, args...)
}

// Config returns the module configuration.
func (m *QuestDBModule) Config() QuestDBConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

// questDBWriter holds a sender + http client and implements TimeSeriesWriter.
type questDBWriter struct {
	sender       qdb.LineSender
	httpClient   *http.Client
	httpEndpoint string
}

func (w *questDBWriter) WritePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]any, ts time.Time) error {
	if err := buildILPRow(w.sender, measurement, tags, fields).At(ctx, ts); err != nil {
		return fmt.Errorf("questdb WritePoint: %w", err)
	}
	return nil
}

func (w *questDBWriter) WriteBatch(ctx context.Context, points []Point) error {
	for _, p := range points {
		if err := buildILPRow(w.sender, p.Measurement, p.Tags, p.Fields).At(ctx, p.Timestamp); err != nil {
			return fmt.Errorf("questdb WriteBatch: %w", err)
		}
	}
	return w.sender.Flush(ctx)
}

func (w *questDBWriter) Query(ctx context.Context, query string, _ ...any) ([]map[string]any, error) {
	queryURL := w.httpEndpoint + "/exec?query=" + url.QueryEscape(query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, queryURL, nil)
	if err != nil {
		return nil, fmt.Errorf("questdb Query: build request: %w", err)
	}
	resp, err := w.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("questdb Query: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("questdb Query: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("questdb Query: status %d: %s", resp.StatusCode, body)
	}
	return parseQuestDBResponse(body)
}

func buildILPRow(ls qdb.LineSender, measurement string, tags map[string]string, fields map[string]any) qdb.LineSender {
	ls = ls.Table(measurement)
	for k, v := range tags {
		ls = ls.Symbol(k, v)
	}
	for k, v := range fields {
		switch val := v.(type) {
		case float64:
			ls = ls.Float64Column(k, val)
		case int64:
			ls = ls.Int64Column(k, val)
		case int:
			ls = ls.Int64Column(k, int64(val))
		case string:
			ls = ls.StringColumn(k, val)
		case bool:
			ls = ls.BoolColumn(k, val)
		}
	}
	return ls
}

func parseQuestDBResponse(body []byte) ([]map[string]any, error) {
	var result struct {
		Columns []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"columns"`
		Dataset [][]any `json:"dataset"`
		Error   string  `json:"error"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("questdb Query: parse response: %w", err)
	}
	if result.Error != "" {
		return nil, fmt.Errorf("questdb Query error: %s", result.Error)
	}
	rows := make([]map[string]any, len(result.Dataset))
	for i, row := range result.Dataset {
		m := make(map[string]any, len(result.Columns))
		for j, col := range result.Columns {
			if j < len(row) {
				m[col.Name] = row[j]
			}
		}
		rows[i] = m
	}
	return rows, nil
}

func parseQuestDBConfig(config map[string]any) (QuestDBConfig, error) {
	var cfg QuestDBConfig
	cfg.AutoFlush = true // default on

	if v, ok := config["ilpEndpoint"].(string); ok {
		cfg.ILPEndpoint = v
	}
	if v, ok := config["httpEndpoint"].(string); ok {
		cfg.HTTPEndpoint = v
	}
	if v, ok := config["authToken"].(string); ok {
		cfg.AuthToken = v
	}
	if v, ok := config["tlsEnabled"].(bool); ok {
		cfg.TLSEnabled = v
	}
	if v, ok := config["autoFlush"].(bool); ok {
		cfg.AutoFlush = v
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
	return cfg, nil
}
