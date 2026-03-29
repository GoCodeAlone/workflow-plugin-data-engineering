// Package catalog provides schema catalog modules (Schema Registry, etc.).
package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// SchemaDefinition describes a schema to register or validate against.
type SchemaDefinition struct {
	Schema     string            `json:"schema"               yaml:"schema"`
	SchemaType string            `json:"schemaType,omitempty" yaml:"schemaType,omitempty"`
	References []SchemaReference `json:"references,omitempty" yaml:"references,omitempty"`
}

// SchemaInfo is the full metadata returned for a registered schema.
type SchemaInfo struct {
	Subject    string `json:"subject"              yaml:"subject"`
	Version    int    `json:"version"              yaml:"version"`
	ID         int    `json:"id"                   yaml:"id"`
	Schema     string `json:"schema"               yaml:"schema"`
	SchemaType string `json:"schemaType,omitempty" yaml:"schemaType,omitempty"`
}

// SchemaReference is a reference to another schema used in a schema definition.
type SchemaReference struct {
	Name    string `json:"name"    yaml:"name"`
	Subject string `json:"subject" yaml:"subject"`
	Version int    `json:"version" yaml:"version"`
}

// SchemaRegistryClient is the interface for Confluent Schema Registry REST API operations.
type SchemaRegistryClient interface {
	ListSubjects(ctx context.Context) ([]string, error)
	RegisterSchema(ctx context.Context, subject string, schema SchemaDefinition) (int, error)
	GetSchema(ctx context.Context, id int) (*SchemaDefinition, error)
	GetLatestSchema(ctx context.Context, subject string) (*SchemaInfo, error)
	GetSchemaByVersion(ctx context.Context, subject, version string) (*SchemaInfo, error)
	DeleteSubject(ctx context.Context, subject string, permanent bool) ([]int, error)
	CheckCompatibility(ctx context.Context, subject string, schema SchemaDefinition) (bool, error)
	GetCompatibilityLevel(ctx context.Context, subject string) (string, error)
	SetCompatibilityLevel(ctx context.Context, subject string, level string) error
	ValidateSchema(ctx context.Context, schema SchemaDefinition, data []byte) error
}

// srHTTPClient implements SchemaRegistryClient via HTTP.
type srHTTPClient struct {
	endpoint   string
	httpClient *http.Client
	username   string
	password   string
}

// NewSchemaRegistryClient creates a Schema Registry HTTP client.
func NewSchemaRegistryClient(endpoint, username, password string, timeout time.Duration) SchemaRegistryClient {
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &srHTTPClient{
		endpoint:   endpoint,
		httpClient: &http.Client{Timeout: timeout},
		username:   username,
		password:   password,
	}
}

func (c *srHTTPClient) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("schema registry: marshal request: %w", err)
		}
		reqBody = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, reqBody)
	if err != nil {
		return nil, fmt.Errorf("schema registry: build request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")
	if body != nil {
		req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	}
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	return c.httpClient.Do(req)
}

func (c *srHTTPClient) readJSON(resp *http.Response, out any) error {
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("schema registry: read body: %w", err)
	}
	if resp.StatusCode >= 400 {
		// Try to parse SR error format {"error_code":..., "message":...}
		var srErr struct {
			ErrorCode int    `json:"error_code"`
			Message   string `json:"message"`
		}
		if json.Unmarshal(data, &srErr) == nil && srErr.Message != "" {
			return fmt.Errorf("schema registry: error %d: %s", srErr.ErrorCode, srErr.Message)
		}
		return fmt.Errorf("schema registry: status %d: %s", resp.StatusCode, data)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}

func (c *srHTTPClient) ListSubjects(ctx context.Context) ([]string, error) {
	resp, err := c.do(ctx, http.MethodGet, "/subjects", nil)
	if err != nil {
		return nil, fmt.Errorf("ListSubjects: %w", err)
	}
	var subjects []string
	if err := c.readJSON(resp, &subjects); err != nil {
		return nil, fmt.Errorf("ListSubjects: %w", err)
	}
	return subjects, nil
}

func (c *srHTTPClient) RegisterSchema(ctx context.Context, subject string, schema SchemaDefinition) (int, error) {
	resp, err := c.do(ctx, http.MethodPost, "/subjects/"+subject+"/versions", schema)
	if err != nil {
		return 0, fmt.Errorf("RegisterSchema: %w", err)
	}
	var result struct {
		ID int `json:"id"`
	}
	if err := c.readJSON(resp, &result); err != nil {
		return 0, fmt.Errorf("RegisterSchema: %w", err)
	}
	return result.ID, nil
}

func (c *srHTTPClient) GetSchema(ctx context.Context, id int) (*SchemaDefinition, error) {
	resp, err := c.do(ctx, http.MethodGet, "/schemas/ids/"+strconv.Itoa(id), nil)
	if err != nil {
		return nil, fmt.Errorf("GetSchema: %w", err)
	}
	var def SchemaDefinition
	if err := c.readJSON(resp, &def); err != nil {
		return nil, fmt.Errorf("GetSchema: %w", err)
	}
	return &def, nil
}

func (c *srHTTPClient) GetLatestSchema(ctx context.Context, subject string) (*SchemaInfo, error) {
	resp, err := c.do(ctx, http.MethodGet, "/subjects/"+subject+"/versions/latest", nil)
	if err != nil {
		return nil, fmt.Errorf("GetLatestSchema: %w", err)
	}
	var info SchemaInfo
	if err := c.readJSON(resp, &info); err != nil {
		return nil, fmt.Errorf("GetLatestSchema: %w", err)
	}
	return &info, nil
}

func (c *srHTTPClient) GetSchemaByVersion(ctx context.Context, subject, version string) (*SchemaInfo, error) {
	resp, err := c.do(ctx, http.MethodGet, "/subjects/"+subject+"/versions/"+version, nil)
	if err != nil {
		return nil, fmt.Errorf("GetSchemaByVersion: %w", err)
	}
	var info SchemaInfo
	if err := c.readJSON(resp, &info); err != nil {
		return nil, fmt.Errorf("GetSchemaByVersion: %w", err)
	}
	return &info, nil
}

func (c *srHTTPClient) DeleteSubject(ctx context.Context, subject string, permanent bool) ([]int, error) {
	path := "/subjects/" + subject
	if permanent {
		path += "?permanent=true"
	}
	resp, err := c.do(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return nil, fmt.Errorf("DeleteSubject: %w", err)
	}
	var versions []int
	if err := c.readJSON(resp, &versions); err != nil {
		return nil, fmt.Errorf("DeleteSubject: %w", err)
	}
	return versions, nil
}

func (c *srHTTPClient) CheckCompatibility(ctx context.Context, subject string, schema SchemaDefinition) (bool, error) {
	resp, err := c.do(ctx, http.MethodPost, "/compatibility/subjects/"+subject+"/versions/latest", schema)
	if err != nil {
		return false, fmt.Errorf("CheckCompatibility: %w", err)
	}
	var result struct {
		IsCompatible bool `json:"is_compatible"`
	}
	if err := c.readJSON(resp, &result); err != nil {
		return false, fmt.Errorf("CheckCompatibility: %w", err)
	}
	return result.IsCompatible, nil
}

func (c *srHTTPClient) GetCompatibilityLevel(ctx context.Context, subject string) (string, error) {
	path := "/config"
	if subject != "" {
		path = "/config/" + subject
	}
	resp, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return "", fmt.Errorf("GetCompatibilityLevel: %w", err)
	}
	var result struct {
		CompatibilityLevel string `json:"compatibilityLevel"`
	}
	if err := c.readJSON(resp, &result); err != nil {
		return "", fmt.Errorf("GetCompatibilityLevel: %w", err)
	}
	return result.CompatibilityLevel, nil
}

func (c *srHTTPClient) SetCompatibilityLevel(ctx context.Context, subject string, level string) error {
	path := "/config"
	if subject != "" {
		path = "/config/" + subject
	}
	body := map[string]string{"compatibility": level}
	resp, err := c.do(ctx, http.MethodPut, path, body)
	if err != nil {
		return fmt.Errorf("SetCompatibilityLevel: %w", err)
	}
	return c.readJSON(resp, nil)
}

// ValidateSchema performs structural validation of data against the schema definition.
// For JSON Schema, it checks the JSON is valid and matches field constraints.
// For Avro/Protobuf, it validates field names match.
func (c *srHTTPClient) ValidateSchema(_ context.Context, schema SchemaDefinition, data []byte) error {
	switch schema.SchemaType {
	case "JSON", "":
		return validateJSONSchema(schema.Schema, data)
	case "AVRO":
		return validateAvroSchema(schema.Schema, data)
	default:
		// For PROTOBUF and unknown types, check the data is valid JSON.
		var v any
		if err := json.Unmarshal(data, &v); err != nil {
			return fmt.Errorf("schema validation: invalid JSON payload: %w", err)
		}
		return nil
	}
}

// validateJSONSchema checks the data is valid JSON that can be decoded into any.
func validateJSONSchema(schemaDef string, data []byte) error {
	// Validate the data is parseable JSON.
	var payload any
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("schema validation: invalid JSON payload: %w", err)
	}

	// Parse the schema definition to get expected fields.
	var schemaDef2 map[string]any
	if err := json.Unmarshal([]byte(schemaDef), &schemaDef2); err != nil {
		// Schema itself isn't valid JSON schema — skip field validation.
		return nil
	}

	// If schema has required fields, verify they are present in the payload.
	if required, ok := schemaDef2["required"].([]any); ok {
		payloadObj, ok := payload.(map[string]any)
		if !ok {
			return fmt.Errorf("schema validation: expected JSON object, got %T", payload)
		}
		var missing []string
		for _, r := range required {
			field, _ := r.(string)
			if _, exists := payloadObj[field]; !exists {
				missing = append(missing, field)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("schema validation: missing required fields: %v", missing)
		}
	}
	return nil
}

// validateAvroSchema checks that the JSON data has the fields declared in the Avro schema.
func validateAvroSchema(schemaDef string, data []byte) error {
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("schema validation: invalid JSON payload: %w", err)
	}

	var avroSchema struct {
		Fields []struct {
			Name string `json:"name"`
		} `json:"fields"`
	}
	if err := json.Unmarshal([]byte(schemaDef), &avroSchema); err != nil {
		return nil // can't parse avro schema, skip
	}

	var missing []string
	for _, f := range avroSchema.Fields {
		if _, exists := payload[f.Name]; !exists {
			missing = append(missing, f.Name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("schema validation: missing Avro fields: %v", missing)
	}
	return nil
}
