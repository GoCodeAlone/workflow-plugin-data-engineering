package catalog

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/httpclient"
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
	SetLineage(ctx context.Context, upstream, downstream string) error
	GetLineage(ctx context.Context, urn, direction string) (*LineageResult, error)
}

// dhHTTPClient implements DataHubClient via HTTP.
type dhHTTPClient struct {
	client *httpclient.Client
}

// NewDataHubClient creates a new DataHub HTTP client.
func NewDataHubClient(endpoint, token string, timeout time.Duration) DataHubClient {
	return &dhHTTPClient{
		client: httpclient.New(endpoint, httpclient.AuthConfig{Type: "bearer", Token: token}, timeout),
	}
}

func (c *dhHTTPClient) GetDataset(ctx context.Context, urn string) (*Dataset, error) {
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
	if err := c.client.DoJSON(ctx, http.MethodGet, "/entities/v1/"+urn, nil, &result); err != nil {
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
	var raw struct {
		NumEntities int `json:"numEntities"`
		From        int `json:"from"`
		Entities    []struct {
			URN  string `json:"urn"`
			Name string `json:"name"`
		} `json:"entities"`
	}
	if err := c.client.DoJSON(ctx, http.MethodPost, "/entities/v1/search", reqBody, &raw); err != nil {
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
	return c.client.DoJSON(ctx, http.MethodPost, "/aspects?action=ingestProposal", map[string]any{"proposals": proposals}, nil)
}

func (c *dhHTTPClient) AddTag(ctx context.Context, urn, tag string) error {
	return c.client.DoJSON(ctx, http.MethodPost, "/entities?action=setTag", map[string]any{"urn": urn, "tag": tag}, nil)
}

func (c *dhHTTPClient) AddGlossaryTerm(ctx context.Context, urn, term string) error {
	return c.client.DoJSON(ctx, http.MethodPost, "/entities?action=setGlossaryTerm", map[string]any{"urn": urn, "term": term}, nil)
}

func (c *dhHTTPClient) SetOwner(ctx context.Context, urn, ownerUrn, ownerType string) error {
	return c.client.DoJSON(ctx, http.MethodPost, "/entities?action=setOwner", map[string]any{"urn": urn, "ownerUrn": ownerUrn, "ownerType": ownerType}, nil)
}

func (c *dhHTTPClient) SetLineage(ctx context.Context, upstream, downstream string) error {
	return c.client.DoJSON(ctx, http.MethodPost, "/relationships?action=setLineage", map[string]any{
		"upstreamUrns":   []string{upstream},
		"downstreamUrns": []string{downstream},
	}, nil)
}

func (c *dhHTTPClient) GetLineage(ctx context.Context, urn, direction string) (*LineageResult, error) {
	var raw struct {
		Entities []struct {
			URN  string `json:"urn"`
			Name string `json:"name"`
		} `json:"entities"`
	}
	if err := c.client.DoJSON(ctx, http.MethodPost, "/relationships?action=setLineage", map[string]any{"urn": urn, "direction": direction}, &raw); err != nil {
		return nil, fmt.Errorf("datahub: GetLineage: %w", err)
	}
	result := &LineageResult{URN: urn, Direction: direction}
	for _, e := range raw.Entities {
		result.Entities = append(result.Entities, Dataset{URN: e.URN, Name: e.Name})
	}
	return result, nil
}
