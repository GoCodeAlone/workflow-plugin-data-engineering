// Package lakehouse implements Iceberg lakehouse modules and steps.
package lakehouse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Namespace is a multi-level Iceberg namespace (e.g. ["analytics", "raw"]).
type Namespace []string

// TableIdentifier uniquely identifies an Iceberg table.
type TableIdentifier struct {
	Namespace Namespace
	Name      string
}

// CatalogConfig holds catalog-level configuration defaults and overrides.
type CatalogConfig struct {
	Defaults  map[string]string `json:"defaults"`
	Overrides map[string]string `json:"overrides"`
}

// NamespaceInfo holds namespace metadata returned from the catalog.
type NamespaceInfo struct {
	Namespace  Namespace         `json:"namespace"`
	Properties map[string]string `json:"properties"`
}

// SchemaField is a single field in an Iceberg table schema.
type SchemaField struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
	Doc      string `json:"doc,omitempty"`
}

// Schema is an Iceberg table schema.
type Schema struct {
	SchemaID int           `json:"schema-id"`
	Type     string        `json:"type"`
	Fields   []SchemaField `json:"fields"`
}

// PartitionSpec describes how data is partitioned.
type PartitionSpec struct {
	SpecID int              `json:"spec-id"`
	Fields []PartitionField `json:"fields"`
}

// PartitionField is a single partition field.
type PartitionField struct {
	SourceID  int    `json:"source-id"`
	FieldID   int    `json:"field-id"`
	Name      string `json:"name"`
	Transform string `json:"transform"`
}

// SortOrder describes the sort order for data files.
type SortOrder struct {
	OrderID int         `json:"order-id"`
	Fields  []SortField `json:"fields"`
}

// SortField is a single sort field.
type SortField struct {
	SourceID  int    `json:"source-id"`
	Transform string `json:"transform"`
	Direction string `json:"direction"`
	NullOrder string `json:"null-order"`
}

// CreateTableRequest is the body for creating a new Iceberg table.
type CreateTableRequest struct {
	Name          string            `json:"name"`
	Schema        Schema            `json:"schema"`
	PartitionSpec *PartitionSpec    `json:"partition-spec,omitempty"`
	WriteOrder    *SortOrder        `json:"write-order,omitempty"`
	Properties    map[string]string `json:"properties,omitempty"`
	Location      string            `json:"location,omitempty"`
}

// Snapshot represents an Iceberg table snapshot.
type Snapshot struct {
	SnapshotID   int64             `json:"snapshot-id"`
	Timestamp    int64             `json:"timestamp-ms"`
	Summary      map[string]string `json:"summary"`
	ManifestList string            `json:"manifest-list"`
}

// TableMetadata holds the full metadata for an Iceberg table.
type TableMetadata struct {
	FormatVersion     int               `json:"format-version"`
	TableUUID         string            `json:"table-uuid"`
	Location          string            `json:"location"`
	CurrentSchemaID   int               `json:"current-schema-id"`
	Schemas           []Schema          `json:"schemas"`
	CurrentSnapshotID *int64            `json:"current-snapshot-id,omitempty"`
	Snapshots         []Snapshot        `json:"snapshots"`
	Properties        map[string]string `json:"properties"`
}

// TableUpdate represents a single schema or metadata update.
type TableUpdate struct {
	Action string         `json:"action"`
	Fields map[string]any `json:"fields,omitempty"`
}

// TableRequirement asserts preconditions before applying updates.
type TableRequirement struct {
	Type   string         `json:"type"`
	Fields map[string]any `json:"fields,omitempty"`
}

// IcebergCatalogClient is the interface for interacting with an Iceberg REST Catalog.
type IcebergCatalogClient interface {
	// GetConfig retrieves catalog-level configuration.
	GetConfig(ctx context.Context) (*CatalogConfig, error)

	// ListNamespaces lists namespaces, optionally filtered by parent.
	ListNamespaces(ctx context.Context, parent string) ([]Namespace, error)

	// CreateNamespace creates a new namespace with optional properties.
	CreateNamespace(ctx context.Context, ns Namespace, properties map[string]string) error

	// LoadNamespace retrieves namespace info and properties.
	LoadNamespace(ctx context.Context, ns Namespace) (*NamespaceInfo, error)

	// DropNamespace removes a namespace.
	DropNamespace(ctx context.Context, ns Namespace) error

	// UpdateNamespaceProperties sets or removes namespace properties.
	UpdateNamespaceProperties(ctx context.Context, ns Namespace, updates, removals map[string]string) error

	// ListTables lists all tables within a namespace.
	ListTables(ctx context.Context, ns Namespace) ([]TableIdentifier, error)

	// CreateTable creates a new table in the given namespace.
	CreateTable(ctx context.Context, ns Namespace, req CreateTableRequest) (*TableMetadata, error)

	// LoadTable retrieves table metadata by identifier.
	LoadTable(ctx context.Context, id TableIdentifier) (*TableMetadata, error)

	// UpdateTable applies schema or metadata updates to a table.
	UpdateTable(ctx context.Context, id TableIdentifier, updates []TableUpdate, requirements []TableRequirement) (*TableMetadata, error)

	// DropTable removes a table; if purge is true, data files are also deleted.
	DropTable(ctx context.Context, id TableIdentifier, purge bool) error

	// TableExists returns true if the table exists.
	TableExists(ctx context.Context, id TableIdentifier) (bool, error)

	// ListSnapshots returns all snapshots for a table.
	ListSnapshots(ctx context.Context, id TableIdentifier) ([]Snapshot, error)
}

// IcebergClientConfig holds configuration for the HTTP catalog client.
type IcebergClientConfig struct {
	// Endpoint is the base URL of the REST catalog (e.g. "https://catalog.example.com/v1").
	Endpoint string `json:"endpoint" yaml:"endpoint"`
	// Token is a static Bearer token for authentication.
	Token string `json:"credential" yaml:"credential"`
	// HTTPTimeout is the HTTP client timeout (default 30s).
	HTTPTimeout time.Duration `json:"httpTimeout" yaml:"httpTimeout"`
}

// httpCatalogClient implements IcebergCatalogClient using the Iceberg REST Catalog API.
// It is stateless and safe for concurrent use after construction.
type httpCatalogClient struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

// NewIcebergCatalogClient creates a new IcebergCatalogClient from config.
func NewIcebergCatalogClient(cfg IcebergClientConfig) (IcebergCatalogClient, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("iceberg client: endpoint is required")
	}
	timeout := cfg.HTTPTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &httpCatalogClient{
		baseURL: strings.TrimSuffix(cfg.Endpoint, "/"),
		token:   cfg.Token,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}, nil
}

// encodeNamespacePath encodes a Namespace for use in URL path segments.
// Per the Iceberg REST spec, namespace parts are joined with the Unicode
// unit separator (\x1F) and URL-path-escaped.
func encodeNamespacePath(ns Namespace) string {
	return url.PathEscape(strings.Join(ns, "\x1F"))
}

// apiError is the Iceberg REST error response body.
type apiError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    int    `json:"code"`
}

// catalogError wraps an API error response.
type catalogError struct {
	err    apiError
	status int
}

func (e *catalogError) Error() string {
	if e.err.Type != "" {
		return fmt.Sprintf("iceberg catalog: %s (HTTP %d): %s", e.err.Type, e.status, e.err.Message)
	}
	return fmt.Sprintf("iceberg catalog: HTTP %d", e.status)
}

// doRequest executes an HTTP request and decodes the response body into out.
// If out is nil, the response body is discarded.
func (c *httpCatalogClient) doRequest(ctx context.Context, method, path string, body any, out any) error {
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("iceberg client: marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return fmt.Errorf("iceberg client: build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("iceberg client: request %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("iceberg client: read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Try to parse the Iceberg error format.
		var wrapper struct {
			Error apiError `json:"error"`
		}
		if jsonErr := json.Unmarshal(respBody, &wrapper); jsonErr == nil && wrapper.Error.Type != "" {
			return &catalogError{err: wrapper.Error, status: resp.StatusCode}
		}
		return &catalogError{err: apiError{Code: resp.StatusCode}, status: resp.StatusCode}
	}

	if out != nil {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("iceberg client: decode response: %w", err)
		}
	}
	return nil
}

func (c *httpCatalogClient) GetConfig(ctx context.Context) (*CatalogConfig, error) {
	var cfg CatalogConfig
	if err := c.doRequest(ctx, http.MethodGet, "/config", nil, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *httpCatalogClient) ListNamespaces(ctx context.Context, parent string) ([]Namespace, error) {
	path := "/namespaces"
	if parent != "" {
		path += "?parent=" + url.QueryEscape(parent)
	}
	var resp struct {
		Namespaces []Namespace `json:"namespaces"`
	}
	if err := c.doRequest(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Namespaces, nil
}

func (c *httpCatalogClient) CreateNamespace(ctx context.Context, ns Namespace, properties map[string]string) error {
	body := map[string]any{
		"namespace":  ns,
		"properties": properties,
	}
	return c.doRequest(ctx, http.MethodPost, "/namespaces", body, nil)
}

func (c *httpCatalogClient) LoadNamespace(ctx context.Context, ns Namespace) (*NamespaceInfo, error) {
	path := "/namespaces/" + encodeNamespacePath(ns)
	var info NamespaceInfo
	if err := c.doRequest(ctx, http.MethodGet, path, nil, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

func (c *httpCatalogClient) DropNamespace(ctx context.Context, ns Namespace) error {
	path := "/namespaces/" + encodeNamespacePath(ns)
	return c.doRequest(ctx, http.MethodDelete, path, nil, nil)
}

func (c *httpCatalogClient) UpdateNamespaceProperties(ctx context.Context, ns Namespace, updates, removals map[string]string) error {
	removalsSlice := make([]string, 0, len(removals))
	for k := range removals {
		removalsSlice = append(removalsSlice, k)
	}
	body := map[string]any{
		"updates":  updates,
		"removals": removalsSlice,
	}
	path := "/namespaces/" + encodeNamespacePath(ns) + "/properties"
	return c.doRequest(ctx, http.MethodPost, path, body, nil)
}

func (c *httpCatalogClient) ListTables(ctx context.Context, ns Namespace) ([]TableIdentifier, error) {
	path := "/namespaces/" + encodeNamespacePath(ns) + "/tables"
	var resp struct {
		Identifiers []struct {
			Namespace Namespace `json:"namespace"`
			Name      string    `json:"name"`
		} `json:"identifiers"`
	}
	if err := c.doRequest(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	ids := make([]TableIdentifier, len(resp.Identifiers))
	for i, id := range resp.Identifiers {
		ids[i] = TableIdentifier{Namespace: id.Namespace, Name: id.Name}
	}
	return ids, nil
}

func (c *httpCatalogClient) CreateTable(ctx context.Context, ns Namespace, req CreateTableRequest) (*TableMetadata, error) {
	path := "/namespaces/" + encodeNamespacePath(ns) + "/tables"
	var resp struct {
		Metadata tableMetadataWire `json:"metadata"`
	}
	if err := c.doRequest(ctx, http.MethodPost, path, req, &resp); err != nil {
		return nil, err
	}
	return resp.Metadata.toTableMetadata(), nil
}

func (c *httpCatalogClient) LoadTable(ctx context.Context, id TableIdentifier) (*TableMetadata, error) {
	path := "/namespaces/" + encodeNamespacePath(id.Namespace) + "/tables/" + url.PathEscape(id.Name)
	var resp struct {
		Metadata tableMetadataWire `json:"metadata"`
	}
	if err := c.doRequest(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Metadata.toTableMetadata(), nil
}

func (c *httpCatalogClient) UpdateTable(ctx context.Context, id TableIdentifier, updates []TableUpdate, requirements []TableRequirement) (*TableMetadata, error) {
	path := "/namespaces/" + encodeNamespacePath(id.Namespace) + "/tables/" + url.PathEscape(id.Name)
	body := map[string]any{
		"identifier": map[string]any{
			"namespace": id.Namespace,
			"name":      id.Name,
		},
		"updates":      updates,
		"requirements": requirements,
	}
	var resp struct {
		Metadata tableMetadataWire `json:"metadata"`
	}
	if err := c.doRequest(ctx, http.MethodPost, path, body, &resp); err != nil {
		return nil, err
	}
	return resp.Metadata.toTableMetadata(), nil
}

func (c *httpCatalogClient) DropTable(ctx context.Context, id TableIdentifier, purge bool) error {
	path := "/namespaces/" + encodeNamespacePath(id.Namespace) + "/tables/" + url.PathEscape(id.Name)
	if purge {
		path += "?purgeRequested=true"
	}
	return c.doRequest(ctx, http.MethodDelete, path, nil, nil)
}

func (c *httpCatalogClient) TableExists(ctx context.Context, id TableIdentifier) (bool, error) {
	path := "/namespaces/" + encodeNamespacePath(id.Namespace) + "/tables/" + url.PathEscape(id.Name)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.baseURL+path, nil)
	if err != nil {
		return false, fmt.Errorf("iceberg client: build HEAD request: %w", err)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("iceberg client: HEAD %s: %w", path, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, &catalogError{err: apiError{Code: resp.StatusCode}, status: resp.StatusCode}
	}
}

func (c *httpCatalogClient) ListSnapshots(ctx context.Context, id TableIdentifier) ([]Snapshot, error) {
	meta, err := c.LoadTable(ctx, id)
	if err != nil {
		return nil, err
	}
	return meta.Snapshots, nil
}

// tableMetadataWire is the JSON-wire representation of Iceberg table metadata,
// using JSON field names with hyphens as defined in the Iceberg REST spec.
type tableMetadataWire struct {
	FormatVersion     int               `json:"format-version"`
	TableUUID         string            `json:"table-uuid"`
	Location          string            `json:"location"`
	CurrentSchemaID   int               `json:"current-schema-id"`
	Schemas           []schemaWire      `json:"schemas"`
	CurrentSnapshotID *int64            `json:"current-snapshot-id,omitempty"`
	Snapshots         []snapshotWire    `json:"snapshots"`
	Properties        map[string]string `json:"properties"`
}

type schemaWire struct {
	SchemaID int               `json:"schema-id"`
	Type     string            `json:"type"`
	Fields   []schemaFieldWire `json:"fields"`
}

type schemaFieldWire struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
	Doc      string `json:"doc,omitempty"`
}

type snapshotWire struct {
	SnapshotID   int64             `json:"snapshot-id"`
	Timestamp    int64             `json:"timestamp-ms"`
	Summary      map[string]string `json:"summary"`
	ManifestList string            `json:"manifest-list"`
}

func (w *tableMetadataWire) toTableMetadata() *TableMetadata {
	schemas := make([]Schema, len(w.Schemas))
	for i, s := range w.Schemas {
		fields := make([]SchemaField, len(s.Fields))
		for j, f := range s.Fields {
			fields[j] = SchemaField{ID: f.ID, Name: f.Name, Type: f.Type, Required: f.Required, Doc: f.Doc}
		}
		schemas[i] = Schema{SchemaID: s.SchemaID, Type: s.Type, Fields: fields}
	}
	snapshots := make([]Snapshot, len(w.Snapshots))
	for i, s := range w.Snapshots {
		snapshots[i] = Snapshot{
			SnapshotID:   s.SnapshotID,
			Timestamp:    s.Timestamp,
			Summary:      s.Summary,
			ManifestList: s.ManifestList,
		}
	}
	return &TableMetadata{
		FormatVersion:     w.FormatVersion,
		TableUUID:         w.TableUUID,
		Location:          w.Location,
		CurrentSchemaID:   w.CurrentSchemaID,
		Schemas:           schemas,
		CurrentSnapshotID: w.CurrentSnapshotID,
		Snapshots:         snapshots,
		Properties:        w.Properties,
	}
}
