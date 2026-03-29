package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// OMTable represents a table entity in OpenMetadata.
type OMTable struct {
	ID          string            `json:"id,omitempty"`
	Name        string            `json:"name"`
	FullyQualifiedName string    `json:"fullyQualifiedName,omitempty"`
	Description string            `json:"description,omitempty"`
	Columns     []OMColumn        `json:"columns,omitempty"`
	Tags        []OMTag           `json:"tags,omitempty"`
	Owner       *OMEntityRef      `json:"owner,omitempty"`
}

// OMColumn is a single column in an OpenMetadata table.
type OMColumn struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
}

// OMTag is a tag attached to an entity in OpenMetadata.
type OMTag struct {
	TagFQN string `json:"tagFQN"`
}

// OMEntityRef is a reference to an entity (e.g. owner).
type OMEntityRef struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// OMSearchResult is the result of an OpenMetadata search query.
type OMSearchResult struct {
	Tables []OMTable `json:"tables"`
	Total  int       `json:"total"`
}

// OMLineageResult holds lineage information from OpenMetadata.
type OMLineageResult struct {
	FQN      string    `json:"fullyQualifiedName"`
	Upstream []OMTable `json:"upstream,omitempty"`
	Downstream []OMTable `json:"downstream,omitempty"`
}

// OpenMetadataClient is the interface for OpenMetadata REST API operations.
type OpenMetadataClient interface {
	GetTable(ctx context.Context, fqn string) (*OMTable, error)
	SearchTables(ctx context.Context, query string, limit int) (*OMSearchResult, error)
	CreateOrUpdateTable(ctx context.Context, table OMTable) error
	AddTag(ctx context.Context, fqn, tag string) error
	SetOwner(ctx context.Context, fqn, owner string) error
	GetLineage(ctx context.Context, fqn string) (*OMLineageResult, error)
}

// omHTTPClient implements OpenMetadataClient via HTTP.
type omHTTPClient struct {
	endpoint   string
	token      string
	httpClient *http.Client
}

// NewOpenMetadataClient creates a new OpenMetadata HTTP client.
func NewOpenMetadataClient(endpoint, token string, timeout time.Duration) OpenMetadataClient {
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &omHTTPClient{
		endpoint:   endpoint,
		token:      token,
		httpClient: &http.Client{Timeout: timeout},
	}
}

func (c *omHTTPClient) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("openmetadata: marshal request: %w", err)
		}
		reqBody = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, reqBody)
	if err != nil {
		return nil, fmt.Errorf("openmetadata: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return c.httpClient.Do(req)
}

func (c *omHTTPClient) decodeResponse(resp *http.Response, out any) error {
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("openmetadata: read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("openmetadata: HTTP %d: %s", resp.StatusCode, string(body))
	}
	if out != nil && len(body) > 0 {
		if err := json.Unmarshal(body, out); err != nil {
			return fmt.Errorf("openmetadata: unmarshal response: %w", err)
		}
	}
	return nil
}

func (c *omHTTPClient) GetTable(ctx context.Context, fqn string) (*OMTable, error) {
	resp, err := c.do(ctx, http.MethodGet, "/api/v1/tables/name/"+url.PathEscape(fqn), nil)
	if err != nil {
		return nil, fmt.Errorf("openmetadata: GetTable: %w", err)
	}
	var table OMTable
	if err := c.decodeResponse(resp, &table); err != nil {
		return nil, fmt.Errorf("openmetadata: GetTable: %w", err)
	}
	return &table, nil
}

func (c *omHTTPClient) SearchTables(ctx context.Context, query string, limit int) (*OMSearchResult, error) {
	path := fmt.Sprintf("/api/v1/search/query?q=%s&size=%d", url.QueryEscape(query), limit)
	resp, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("openmetadata: SearchTables: %w", err)
	}
	var raw struct {
		Hits struct {
			Total struct {
				Value int `json:"value"`
			} `json:"total"`
			Hits []struct {
				Source OMTable `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := c.decodeResponse(resp, &raw); err != nil {
		return nil, fmt.Errorf("openmetadata: SearchTables: %w", err)
	}
	result := &OMSearchResult{Total: raw.Hits.Total.Value}
	for _, h := range raw.Hits.Hits {
		result.Tables = append(result.Tables, h.Source)
	}
	return result, nil
}

func (c *omHTTPClient) CreateOrUpdateTable(ctx context.Context, table OMTable) error {
	resp, err := c.do(ctx, http.MethodPut, "/api/v1/tables", table)
	if err != nil {
		return fmt.Errorf("openmetadata: CreateOrUpdateTable: %w", err)
	}
	return c.decodeResponse(resp, nil)
}

func (c *omHTTPClient) AddTag(ctx context.Context, fqn, tag string) error {
	table, err := c.GetTable(ctx, fqn)
	if err != nil {
		return fmt.Errorf("openmetadata: AddTag: %w", err)
	}
	reqBody := []map[string]any{{"tagFQN": tag}}
	resp, err := c.do(ctx, http.MethodPut, "/api/v1/tables/"+table.ID+"/tags", reqBody)
	if err != nil {
		return fmt.Errorf("openmetadata: AddTag: %w", err)
	}
	return c.decodeResponse(resp, nil)
}

func (c *omHTTPClient) SetOwner(ctx context.Context, fqn, owner string) error {
	table, err := c.GetTable(ctx, fqn)
	if err != nil {
		return fmt.Errorf("openmetadata: SetOwner: %w", err)
	}
	table.Owner = &OMEntityRef{Name: owner, Type: "user"}
	resp, err := c.do(ctx, http.MethodPut, "/api/v1/tables", table)
	if err != nil {
		return fmt.Errorf("openmetadata: SetOwner: %w", err)
	}
	return c.decodeResponse(resp, nil)
}

func (c *omHTTPClient) GetLineage(ctx context.Context, fqn string) (*OMLineageResult, error) {
	resp, err := c.do(ctx, http.MethodGet, "/api/v1/lineage/table/name/"+url.PathEscape(fqn), nil)
	if err != nil {
		return nil, fmt.Errorf("openmetadata: GetLineage: %w", err)
	}
	var raw struct {
		Entity struct {
			FQN string `json:"fullyQualifiedName"`
		} `json:"entity"`
		Nodes []struct {
			FQN string `json:"fullyQualifiedName"`
		} `json:"nodes"`
	}
	if err := c.decodeResponse(resp, &raw); err != nil {
		return nil, fmt.Errorf("openmetadata: GetLineage: %w", err)
	}
	result := &OMLineageResult{FQN: raw.Entity.FQN}
	for _, n := range raw.Nodes {
		result.Upstream = append(result.Upstream, OMTable{FullyQualifiedName: n.FQN})
	}
	return result, nil
}
