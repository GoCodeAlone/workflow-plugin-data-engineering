package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Dataset represents a dataset entity in DataHub.
type Dataset struct {
	URN        string            `json:"urn"`
	Name       string            `json:"name"`
	Platform   string            `json:"platform"`
	Schema     *DatasetSchema    `json:"schema,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
	Tags       []string          `json:"tags,omitempty"`
	Owner      string            `json:"owner,omitempty"`
}

// DatasetSchema holds the schema fields of a dataset.
type DatasetSchema struct {
	Fields []SchemaField `json:"fields"`
}

// SchemaField is a single field in a dataset schema.
type SchemaField struct {
	Name     string `json:"fieldPath" yaml:"name"`
	Type     string `json:"nativeDataType" yaml:"type"`
	Nullable bool   `json:"nullable"       yaml:"nullable"`
}

// MetadataProposal is a single aspect to emit to DataHub.
type MetadataProposal struct {
	EntityURN  string         `json:"entityUrn"`
	AspectName string         `json:"aspectName"`
	Aspect     map[string]any `json:"aspect"`
}

// SearchResult is the result of a DataHub search query.
type SearchResult struct {
	Entities []Dataset `json:"entities"`
	Total    int       `json:"total"`
	Start    int       `json:"start"`
	Count    int       `json:"count"`
}

// LineageResult holds lineage information for a dataset.
type LineageResult struct {
	URN       string    `json:"urn"`
	Direction string    `json:"direction"`
	Entities  []Dataset `json:"entities"`
}

// DataHubClient is the interface for DataHub GMS REST API operations.
type DataHubClient interface {
	GetDataset(ctx context.Context, urn string) (*Dataset, error)
	SearchDatasets(ctx context.Context, query string, start, count int) (*SearchResult, error)
	EmitMetadata(ctx context.Context, proposals []MetadataProposal) error
	AddTag(ctx context.Context, urn, tag string) error
	AddGlossaryTerm(ctx context.Context, urn, term string) error
	SetOwner(ctx context.Context, urn, ownerUrn, ownerType string) error
	GetLineage(ctx context.Context, urn, direction string) (*LineageResult, error)
}

// dhHTTPClient implements DataHubClient via HTTP.
type dhHTTPClient struct {
	endpoint   string
	token      string
	httpClient *http.Client
}

// NewDataHubClient creates a new DataHub HTTP client.
func NewDataHubClient(endpoint, token string, timeout time.Duration) DataHubClient {
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &dhHTTPClient{
		endpoint:   endpoint,
		token:      token,
		httpClient: &http.Client{Timeout: timeout},
	}
}

func (c *dhHTTPClient) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("datahub: marshal request: %w", err)
		}
		reqBody = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, reqBody)
	if err != nil {
		return nil, fmt.Errorf("datahub: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return c.httpClient.Do(req)
}

func (c *dhHTTPClient) decodeResponse(resp *http.Response, out any) error {
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("datahub: read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("datahub: HTTP %d: %s", resp.StatusCode, string(body))
	}
	if out != nil {
		if err := json.Unmarshal(body, out); err != nil {
			return fmt.Errorf("datahub: unmarshal response: %w", err)
		}
	}
	return nil
}

func (c *dhHTTPClient) GetDataset(ctx context.Context, urn string) (*Dataset, error) {
	resp, err := c.do(ctx, http.MethodGet, "/entities/v1/"+urn, nil)
	if err != nil {
		return nil, fmt.Errorf("datahub: GetDataset: %w", err)
	}
	var result struct {
		URN    string `json:"urn"`
		Entity struct {
			DatasetProperties struct {
				Name string `json:"name"`
			} `json:"datasetProperties"`
			DataPlatformInstance struct {
				Platform string `json:"platform"`
			} `json:"dataPlatformInstance"`
		} `json:"entity"`
	}
	if err := c.decodeResponse(resp, &result); err != nil {
		return nil, fmt.Errorf("datahub: GetDataset: %w", err)
	}
	return &Dataset{
		URN:      result.URN,
		Name:     result.Entity.DatasetProperties.Name,
		Platform: result.Entity.DataPlatformInstance.Platform,
	}, nil
}

func (c *dhHTTPClient) SearchDatasets(ctx context.Context, query string, start, count int) (*SearchResult, error) {
	reqBody := map[string]any{
		"input": query,
		"start": start,
		"count": count,
	}
	resp, err := c.do(ctx, http.MethodPost, "/entities/v1/search", reqBody)
	if err != nil {
		return nil, fmt.Errorf("datahub: SearchDatasets: %w", err)
	}
	var raw struct {
		NumEntities int `json:"numEntities"`
		From        int `json:"from"`
		PageSize    int `json:"pageSize"`
		Entities    []struct {
			URN  string `json:"urn"`
			Name string `json:"name"`
		} `json:"entities"`
	}
	if err := c.decodeResponse(resp, &raw); err != nil {
		return nil, fmt.Errorf("datahub: SearchDatasets: %w", err)
	}
	result := &SearchResult{
		Total: raw.NumEntities,
		Start: raw.From,
		Count: len(raw.Entities),
	}
	for _, e := range raw.Entities {
		result.Entities = append(result.Entities, Dataset{URN: e.URN, Name: e.Name})
	}
	return result, nil
}

func (c *dhHTTPClient) EmitMetadata(ctx context.Context, proposals []MetadataProposal) error {
	reqBody := map[string]any{"proposals": proposals}
	resp, err := c.do(ctx, http.MethodPost, "/aspects?action=ingestProposal", reqBody)
	if err != nil {
		return fmt.Errorf("datahub: EmitMetadata: %w", err)
	}
	return c.decodeResponse(resp, nil)
}

func (c *dhHTTPClient) AddTag(ctx context.Context, urn, tag string) error {
	reqBody := map[string]any{"urn": urn, "tag": tag}
	resp, err := c.do(ctx, http.MethodPost, "/entities?action=setTag", reqBody)
	if err != nil {
		return fmt.Errorf("datahub: AddTag: %w", err)
	}
	return c.decodeResponse(resp, nil)
}

func (c *dhHTTPClient) AddGlossaryTerm(ctx context.Context, urn, term string) error {
	reqBody := map[string]any{"urn": urn, "term": term}
	resp, err := c.do(ctx, http.MethodPost, "/entities?action=setGlossaryTerm", reqBody)
	if err != nil {
		return fmt.Errorf("datahub: AddGlossaryTerm: %w", err)
	}
	return c.decodeResponse(resp, nil)
}

func (c *dhHTTPClient) SetOwner(ctx context.Context, urn, ownerUrn, ownerType string) error {
	reqBody := map[string]any{"urn": urn, "ownerUrn": ownerUrn, "ownerType": ownerType}
	resp, err := c.do(ctx, http.MethodPost, "/entities?action=setOwner", reqBody)
	if err != nil {
		return fmt.Errorf("datahub: SetOwner: %w", err)
	}
	return c.decodeResponse(resp, nil)
}

func (c *dhHTTPClient) GetLineage(ctx context.Context, urn, direction string) (*LineageResult, error) {
	reqBody := map[string]any{"urn": urn, "direction": direction}
	resp, err := c.do(ctx, http.MethodPost, "/relationships?action=setLineage", reqBody)
	if err != nil {
		return nil, fmt.Errorf("datahub: GetLineage: %w", err)
	}
	var raw struct {
		Entities []struct {
			URN  string `json:"urn"`
			Name string `json:"name"`
		} `json:"entities"`
	}
	if err := c.decodeResponse(resp, &raw); err != nil {
		return nil, fmt.Errorf("datahub: GetLineage: %w", err)
	}
	result := &LineageResult{URN: urn, Direction: direction}
	for _, e := range raw.Entities {
		result.Entities = append(result.Entities, Dataset{URN: e.URN, Name: e.Name})
	}
	return result, nil
}
