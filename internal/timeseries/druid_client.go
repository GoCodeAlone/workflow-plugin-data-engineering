package timeseries

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/httpclient"
)

// DruidStatus represents Druid's status response.
type DruidStatus struct {
	Version string `json:"version"`
	Loading bool   `json:"loading"`
}

// QueryResult holds rows returned from a Druid query.
type QueryResult struct {
	Rows []map[string]any
}

// SupervisorStatus represents a Druid supervisor's state.
type SupervisorStatus struct {
	ID      string `json:"id"`
	State   string `json:"state"`
	Healthy bool   `json:"healthy"`
}

// DatasourceInfo contains metadata about a Druid datasource.
type DatasourceInfo struct {
	Name       string         `json:"name"`
	Properties map[string]any `json:"properties,omitempty"`
}

// CompactionConfig holds parameters for a Druid compaction task.
type CompactionConfig struct {
	TargetCompactionSizeBytes int64  `json:"targetCompactionSizeBytes,omitempty" yaml:"targetCompactionSizeBytes,omitempty"`
	SkipOffsetFromLatest      string `json:"skipOffsetFromLatest,omitempty"      yaml:"skipOffsetFromLatest,omitempty"`
}

// CompactionStatus holds the compaction state for a datasource.
type CompactionStatus struct {
	DataSource string `json:"dataSource"`
	State      string `json:"state"`
}

// DruidClient is the interface for Druid Router API operations.
type DruidClient interface {
	GetStatus(ctx context.Context) (*DruidStatus, error)
	SQLQuery(ctx context.Context, query string, params []any) (*QueryResult, error)
	NativeQuery(ctx context.Context, query map[string]any) (*QueryResult, error)
	SubmitSupervisor(ctx context.Context, spec map[string]any) (*SupervisorStatus, error)
	GetSupervisorStatus(ctx context.Context, id string) (*SupervisorStatus, error)
	SuspendSupervisor(ctx context.Context, id string) error
	ResumeSupervisor(ctx context.Context, id string) error
	TerminateSupervisor(ctx context.Context, id string) error
	ListDatasources(ctx context.Context) ([]string, error)
	GetDatasource(ctx context.Context, name string) (*DatasourceInfo, error)
	DisableDatasource(ctx context.Context, name string) error
	SubmitCompaction(ctx context.Context, datasource string, config CompactionConfig) error
	GetCompactionStatus(ctx context.Context, datasource string) (*CompactionStatus, error)
}

// druidHTTPClient implements DruidClient via HTTP.
type druidHTTPClient struct {
	base *httpclient.Client
}

// NewDruidClient creates a Druid HTTP client.
func NewDruidClient(routerURL, username, password string, timeout time.Duration) DruidClient {
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	return &druidHTTPClient{
		base: httpclient.New(routerURL, httpclient.AuthConfig{Type: "basic", Username: username, Password: password}, timeout),
	}
}

func (c *druidHTTPClient) GetStatus(ctx context.Context) (*DruidStatus, error) {
	var status DruidStatus
	if err := c.base.DoJSON(ctx, http.MethodGet, "/status", nil, &status); err != nil {
		return nil, fmt.Errorf("druid GetStatus: %w", err)
	}
	return &status, nil
}

func (c *druidHTTPClient) SQLQuery(ctx context.Context, query string, params []any) (*QueryResult, error) {
	reqBody := map[string]any{"query": query}
	if len(params) > 0 {
		reqBody["parameters"] = params
	}
	resp, err := c.base.Do(ctx, http.MethodPost, "/druid/v2/sql", reqBody)
	if err != nil {
		return nil, fmt.Errorf("druid SQLQuery: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("druid SQLQuery: read body: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("druid SQLQuery: status %d: %s", resp.StatusCode, data)
	}
	var rows []map[string]any
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("druid SQLQuery: parse response: %w", err)
	}
	return &QueryResult{Rows: rows}, nil
}

func (c *druidHTTPClient) NativeQuery(ctx context.Context, query map[string]any) (*QueryResult, error) {
	resp, err := c.base.Do(ctx, http.MethodPost, "/druid/v2", query)
	if err != nil {
		return nil, fmt.Errorf("druid NativeQuery: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("druid NativeQuery: read body: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("druid NativeQuery: status %d: %s", resp.StatusCode, data)
	}
	var rows []map[string]any
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("druid NativeQuery: parse response: %w", err)
	}
	return &QueryResult{Rows: rows}, nil
}

func (c *druidHTTPClient) SubmitSupervisor(ctx context.Context, spec map[string]any) (*SupervisorStatus, error) {
	var status SupervisorStatus
	if err := c.base.DoJSON(ctx, http.MethodPost, "/druid/indexer/v1/supervisor", spec, &status); err != nil {
		return nil, fmt.Errorf("druid SubmitSupervisor: %w", err)
	}
	return &status, nil
}

func (c *druidHTTPClient) GetSupervisorStatus(ctx context.Context, id string) (*SupervisorStatus, error) {
	var status SupervisorStatus
	if err := c.base.DoJSON(ctx, http.MethodGet, "/druid/indexer/v1/supervisor/"+url.PathEscape(id)+"/status", nil, &status); err != nil {
		return nil, fmt.Errorf("druid GetSupervisorStatus: %w", err)
	}
	return &status, nil
}

func (c *druidHTTPClient) SuspendSupervisor(ctx context.Context, id string) error {
	if err := c.base.DoJSON(ctx, http.MethodPost, "/druid/indexer/v1/supervisor/"+url.PathEscape(id)+"/suspend", nil, nil); err != nil {
		return fmt.Errorf("druid SuspendSupervisor: %w", err)
	}
	return nil
}

func (c *druidHTTPClient) ResumeSupervisor(ctx context.Context, id string) error {
	if err := c.base.DoJSON(ctx, http.MethodPost, "/druid/indexer/v1/supervisor/"+url.PathEscape(id)+"/resume", nil, nil); err != nil {
		return fmt.Errorf("druid ResumeSupervisor: %w", err)
	}
	return nil
}

func (c *druidHTTPClient) TerminateSupervisor(ctx context.Context, id string) error {
	if err := c.base.DoJSON(ctx, http.MethodPost, "/druid/indexer/v1/supervisor/"+url.PathEscape(id)+"/terminate", nil, nil); err != nil {
		return fmt.Errorf("druid TerminateSupervisor: %w", err)
	}
	return nil
}

func (c *druidHTTPClient) ListDatasources(ctx context.Context) ([]string, error) {
	var sources []string
	if err := c.base.DoJSON(ctx, http.MethodGet, "/druid/coordinator/v1/datasources", nil, &sources); err != nil {
		return nil, fmt.Errorf("druid ListDatasources: %w", err)
	}
	return sources, nil
}

func (c *druidHTTPClient) GetDatasource(ctx context.Context, name string) (*DatasourceInfo, error) {
	var info DatasourceInfo
	if err := c.base.DoJSON(ctx, http.MethodGet, "/druid/coordinator/v1/datasources/"+url.PathEscape(name), nil, &info); err != nil {
		return nil, fmt.Errorf("druid GetDatasource: %w", err)
	}
	return &info, nil
}

func (c *druidHTTPClient) DisableDatasource(ctx context.Context, name string) error {
	if err := c.base.DoJSON(ctx, http.MethodDelete, "/druid/coordinator/v1/datasources/"+url.PathEscape(name), nil, nil); err != nil {
		return fmt.Errorf("druid DisableDatasource: %w", err)
	}
	return nil
}

func (c *druidHTTPClient) SubmitCompaction(ctx context.Context, datasource string, cfg CompactionConfig) error {
	body := map[string]any{"dataSource": datasource}
	if cfg.TargetCompactionSizeBytes > 0 {
		body["targetCompactionSizeBytes"] = cfg.TargetCompactionSizeBytes
	}
	if cfg.SkipOffsetFromLatest != "" {
		body["skipOffsetFromLatest"] = cfg.SkipOffsetFromLatest
	}
	if err := c.base.DoJSON(ctx, http.MethodPost, "/druid/coordinator/v1/compaction/compact", body, nil); err != nil {
		return fmt.Errorf("druid SubmitCompaction: %w", err)
	}
	return nil
}

func (c *druidHTTPClient) GetCompactionStatus(ctx context.Context, datasource string) (*CompactionStatus, error) {
	var status CompactionStatus
	if err := c.base.DoJSON(ctx, http.MethodGet, "/druid/coordinator/v1/compaction/progress?dataSource="+datasource, nil, &status); err != nil {
		return nil, fmt.Errorf("druid GetCompactionStatus: %w", err)
	}
	return &status, nil
}
