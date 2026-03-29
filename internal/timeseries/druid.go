package timeseries

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// DruidConfig holds configuration for the timeseries.druid module.
type DruidConfig struct {
	RouterURL   string        `json:"routerUrl"    yaml:"routerUrl"`
	Username    string        `json:"username"     yaml:"username"`
	Password    string        `json:"password"     yaml:"password"`
	HTTPTimeout time.Duration `json:"httpTimeout"  yaml:"httpTimeout"`
}

// DruidModule implements the timeseries.druid module.
type DruidModule struct {
	mu         sync.RWMutex
	name       string
	config     DruidConfig
	client     DruidClient
	httpClient *http.Client
	newClient  func(cfg DruidConfig) DruidClient
}

// NewDruidModule creates a new timeseries.druid module instance.
func NewDruidModule(name string, config map[string]any) (sdk.ModuleInstance, error) {
	cfg, err := parseDruidConfig(config)
	if err != nil {
		return nil, fmt.Errorf("timeseries.druid %q: %w", name, err)
	}
	return &DruidModule{
		name:   name,
		config: cfg,
		newClient: func(c DruidConfig) DruidClient {
			return NewDruidClient(c.RouterURL, c.Username, c.Password, c.HTTPTimeout)
		},
	}, nil
}

// Init validates the module configuration.
func (m *DruidModule) Init() error {
	if m.config.RouterURL == "" {
		return fmt.Errorf("timeseries.druid %q: routerUrl is required", m.name)
	}
	return nil
}

// Start pings Druid and registers the module.
func (m *DruidModule) Start(ctx context.Context) error {
	client := m.newClient(m.config)

	if _, err := client.GetStatus(ctx); err != nil {
		return fmt.Errorf("timeseries.druid %q: connect: %w", m.name, err)
	}

	m.mu.Lock()
	m.client = client
	m.mu.Unlock()

	if err := Register(m.name, m); err != nil {
		return fmt.Errorf("timeseries.druid %q: register: %w", m.name, err)
	}
	return nil
}

// Stop deregisters the module (stateless — no connection to close).
func (m *DruidModule) Stop(_ context.Context) error {
	Unregister(m.name)
	m.mu.Lock()
	m.client = nil
	m.mu.Unlock()
	return nil
}

// Client returns the underlying DruidClient.
func (m *DruidModule) Client() DruidClient {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.client
}

// Config returns the module configuration.
func (m *DruidModule) Config() DruidConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

// WritePoint implements TimeSeriesWriter by submitting a single-row inline batch task.
func (m *DruidModule) WritePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]any, ts time.Time) error {
	return m.WriteBatch(ctx, []Point{{
		Measurement: measurement,
		Tags:        tags,
		Fields:      fields,
		Timestamp:   ts,
	}})
}

// WriteBatch implements TimeSeriesWriter by submitting a native index_parallel task.
func (m *DruidModule) WriteBatch(ctx context.Context, points []Point) error {
	m.mu.RLock()
	client := m.client
	cfg := m.config
	m.mu.RUnlock()
	if client == nil {
		return fmt.Errorf("timeseries.druid %q: not started", m.name)
	}

	// Group by measurement (datasource).
	byDS := map[string][]map[string]any{}
	for _, p := range points {
		row := make(map[string]any, len(p.Tags)+len(p.Fields)+1)
		row["__time"] = p.Timestamp.UTC().Format(time.RFC3339)
		for k, v := range p.Tags {
			row[k] = v
		}
		for k, v := range p.Fields {
			row[k] = v
		}
		byDS[p.Measurement] = append(byDS[p.Measurement], row)
	}

	for datasource, rows := range byDS {
		spec := buildDruidInlineTask(datasource, rows)
		if err := submitDruidTask(ctx, cfg, spec); err != nil {
			return fmt.Errorf("timeseries.druid %q: WriteBatch: %w", m.name, err)
		}
	}
	return nil
}

// Query implements TimeSeriesWriter using the Druid SQL endpoint.
func (m *DruidModule) Query(ctx context.Context, query string, _ ...any) ([]map[string]any, error) {
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	if client == nil {
		return nil, fmt.Errorf("timeseries.druid %q: not started", m.name)
	}
	result, err := client.SQLQuery(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("timeseries.druid %q: Query: %w", m.name, err)
	}
	return result.Rows, nil
}

// buildDruidInlineTask creates a native batch index_parallel task spec with inline data.
func buildDruidInlineTask(datasource string, rows []map[string]any) map[string]any {
	return map[string]any{
		"type": "index_parallel",
		"spec": map[string]any{
			"dataSchema": map[string]any{
				"dataSource": datasource,
				"timestampSpec": map[string]any{
					"column": "__time",
					"format": "iso",
				},
				"dimensionsSpec": map[string]any{
					"useSchemaDiscovery": true,
				},
				"granularitySpec": map[string]any{
					"rollup": false,
				},
			},
			"ioConfig": map[string]any{
				"type": "index_parallel",
				"inputSource": map[string]any{
					"type": "inline",
					"data": func() string {
						b, _ := json.Marshal(rows)
						return string(b)
					}(),
				},
				"inputFormat": map[string]any{
					"type": "json",
				},
			},
		},
	}
}

// submitDruidTask POSTs a task spec to Druid's overlord.
func submitDruidTask(ctx context.Context, cfg DruidConfig, spec map[string]any) error {
	timeout := cfg.HTTPTimeout
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	client := &http.Client{Timeout: timeout}

	body, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.RouterURL+"/druid/indexer/v1/task", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.Username != "" {
		req.SetBasicAuth(cfg.Username, cfg.Password)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("status %d: %s", resp.StatusCode, data)
	}
	return nil
}

func parseDruidConfig(config map[string]any) (DruidConfig, error) {
	var cfg DruidConfig
	if v, ok := config["routerUrl"].(string); ok {
		cfg.RouterURL = v
	}
	if auth, ok := config["auth"].(map[string]any); ok {
		if u, ok := auth["username"].(string); ok {
			cfg.Username = u
		}
		if p, ok := auth["password"].(string); ok {
			cfg.Password = p
		}
	}
	if v, ok := config["username"].(string); ok {
		cfg.Username = v
	}
	if v, ok := config["password"].(string); ok {
		cfg.Password = v
	}
	if v, ok := config["httpTimeout"]; ok {
		switch d := v.(type) {
		case string:
			parsed, err := time.ParseDuration(d)
			if err != nil {
				return cfg, fmt.Errorf("invalid httpTimeout %q: %w", d, err)
			}
			cfg.HTTPTimeout = parsed
		case time.Duration:
			cfg.HTTPTimeout = d
		}
	}
	if cfg.RouterURL == "" {
		return cfg, fmt.Errorf("routerUrl is required")
	}
	return cfg, nil
}
