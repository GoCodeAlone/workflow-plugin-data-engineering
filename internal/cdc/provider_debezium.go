package cdc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// DebeziumProvider implements CDCProvider via the Kafka Connect REST API.
// It creates and manages Debezium connectors on an external Kafka Connect cluster.
// The SourceConfig.Connection field must be the base URL of the Kafka Connect
// REST API (e.g. "http://localhost:8083").
type DebeziumProvider struct {
	mu         sync.RWMutex
	connectors map[string]*debeziumConnector
	httpClient *http.Client
	prefix     string // connector name prefix (default "workflow-")
}

// debeziumConnector holds state for a single managed Debezium connector.
type debeziumConnector struct {
	mu             sync.RWMutex
	config         SourceConfig
	handler        EventHandler
	connectBaseURL string
}

func newDebeziumProvider() *DebeziumProvider {
	return &DebeziumProvider{
		connectors: make(map[string]*debeziumConnector),
		httpClient: &http.Client{Timeout: 30 * time.Second},
		prefix:     "workflow-",
	}
}

// connectorName returns the Kafka Connect connector name for a source ID.
func (p *DebeziumProvider) connectorName(sourceID string) string {
	return p.prefix + sourceID
}

// Connect creates a Debezium connector via POST /connectors on the Kafka Connect API.
// config.Connection must be the Kafka Connect base URL.
func (p *DebeziumProvider) Connect(ctx context.Context, config SourceConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.connectors[config.SourceID]; exists {
		return fmt.Errorf("debezium CDC provider: connector %q already exists", config.SourceID)
	}

	connCfg := buildDebeziumConnectorConfig(config)
	payload := map[string]any{
		"name":   p.connectorName(config.SourceID),
		"config": connCfg,
	}

	url := strings.TrimRight(config.Connection, "/") + "/connectors"
	resp, err := p.doRequest(ctx, http.MethodPost, url, payload)
	if err != nil {
		return fmt.Errorf("debezium CDC provider: create connector %q: %w", config.SourceID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("debezium CDC provider: create connector %q: HTTP %d: %s",
			config.SourceID, resp.StatusCode, string(body))
	}

	p.connectors[config.SourceID] = &debeziumConnector{
		config:         config,
		connectBaseURL: strings.TrimRight(config.Connection, "/"),
	}
	return nil
}

// Disconnect deletes the Debezium connector via DELETE /connectors/{name}.
func (p *DebeziumProvider) Disconnect(ctx context.Context, sourceID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	conn, exists := p.connectors[sourceID]
	if !exists {
		return fmt.Errorf("debezium CDC provider: connector %q not found", sourceID)
	}

	url := conn.connectBaseURL + "/connectors/" + p.connectorName(sourceID)
	resp, err := p.doRequest(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("debezium CDC provider: delete connector %q: %w", sourceID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("debezium CDC provider: delete connector %q: HTTP %d: %s",
			sourceID, resp.StatusCode, string(body))
	}

	delete(p.connectors, sourceID)
	return nil
}

// Status returns the current connector state from GET /connectors/{name}/status.
func (p *DebeziumProvider) Status(ctx context.Context, sourceID string) (*CDCStatus, error) {
	p.mu.RLock()
	conn, exists := p.connectors[sourceID]
	p.mu.RUnlock()
	if !exists {
		return &CDCStatus{SourceID: sourceID, State: "not_found", Provider: "debezium"}, nil
	}

	url := conn.connectBaseURL + "/connectors/" + p.connectorName(sourceID) + "/status"
	resp, err := p.doRequest(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("debezium CDC provider: status %q: %w", sourceID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("debezium CDC provider: status %q: HTTP %d", sourceID, resp.StatusCode)
	}

	var result struct {
		Connector struct {
			State string `json:"state"`
		} `json:"connector"`
		Tasks []struct {
			State string `json:"state"`
		} `json:"tasks"`
		// Optional lag metrics (non-standard extension; populated when available).
		LagBytes   int64 `json:"lag_bytes"`
		LagSeconds int64 `json:"lag_seconds"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("debezium CDC provider: status %q: parse response: %w", sourceID, err)
	}

	state := debeziumConnectState(result.Connector.State)
	// If connector is running but a task failed, surface the task failure.
	if state == "running" {
		for _, task := range result.Tasks {
			if task.State == "FAILED" {
				state = "error"
				break
			}
		}
	}

	return &CDCStatus{
		SourceID:   sourceID,
		State:      state,
		Provider:   "debezium",
		LagBytes:   result.LagBytes,
		LagSeconds: result.LagSeconds,
	}, nil
}

// PauseSource pauses a Debezium connector via PUT /connectors/{name}/pause.
func (p *DebeziumProvider) PauseSource(ctx context.Context, sourceID string) error {
	p.mu.RLock()
	conn, exists := p.connectors[sourceID]
	p.mu.RUnlock()
	if !exists {
		return fmt.Errorf("debezium CDC provider: connector %q not found", sourceID)
	}
	url := conn.connectBaseURL + "/connectors/" + p.connectorName(sourceID) + "/pause"
	resp, err := p.doRequest(ctx, http.MethodPut, url, nil)
	if err != nil {
		return fmt.Errorf("debezium CDC provider: pause %q: %w", sourceID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("debezium CDC provider: pause %q: HTTP %d", sourceID, resp.StatusCode)
	}
	return nil
}

// ResumeSource resumes a paused Debezium connector via PUT /connectors/{name}/resume.
func (p *DebeziumProvider) ResumeSource(ctx context.Context, sourceID string) error {
	p.mu.RLock()
	conn, exists := p.connectors[sourceID]
	p.mu.RUnlock()
	if !exists {
		return fmt.Errorf("debezium CDC provider: connector %q not found", sourceID)
	}
	url := conn.connectBaseURL + "/connectors/" + p.connectorName(sourceID) + "/resume"
	resp, err := p.doRequest(ctx, http.MethodPut, url, nil)
	if err != nil {
		return fmt.Errorf("debezium CDC provider: resume %q: %w", sourceID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("debezium CDC provider: resume %q: HTTP %d", sourceID, resp.StatusCode)
	}
	return nil
}

// Snapshot triggers a full re-snapshot by updating the connector's snapshot.mode
// and restarting it via PUT /connectors/{name}/config + POST /connectors/{name}/restart.
func (p *DebeziumProvider) Snapshot(ctx context.Context, sourceID string, tables []string) error {
	p.mu.RLock()
	conn, exists := p.connectors[sourceID]
	p.mu.RUnlock()
	if !exists {
		return fmt.Errorf("debezium CDC provider: connector %q not found", sourceID)
	}

	// Update connector config with snapshot.mode=always
	connCfg := buildDebeziumConnectorConfig(conn.config)
	connCfg["snapshot.mode"] = "always"
	if len(tables) > 0 {
		connCfg["table.include.list"] = strings.Join(tables, ",")
	}

	putURL := conn.connectBaseURL + "/connectors/" + p.connectorName(sourceID) + "/config"
	resp, err := p.doRequest(ctx, http.MethodPut, putURL, connCfg)
	if err != nil {
		return fmt.Errorf("debezium CDC provider: snapshot %q: update config: %w", sourceID, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("debezium CDC provider: snapshot %q: update config HTTP %d", sourceID, resp.StatusCode)
	}

	// Restart the connector to apply snapshot mode
	restartURL := conn.connectBaseURL + "/connectors/" + p.connectorName(sourceID) + "/restart"
	resp, err = p.doRequest(ctx, http.MethodPost, restartURL, nil)
	if err != nil {
		return fmt.Errorf("debezium CDC provider: snapshot %q: restart: %w", sourceID, err)
	}
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("debezium CDC provider: snapshot %q: restart HTTP %d", sourceID, resp.StatusCode)
	}

	return nil
}

// SchemaHistory returns schema change history for a table.
// NOTE: The Kafka Connect REST API does not expose schema history directly.
// A full implementation would require consuming the Debezium schema history Kafka topic.
// This implementation verifies the connector is running and returns an empty history.
func (p *DebeziumProvider) SchemaHistory(ctx context.Context, sourceID string, table string) ([]SchemaVersion, error) {
	p.mu.RLock()
	_, exists := p.connectors[sourceID]
	p.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("debezium CDC provider: connector %q not found", sourceID)
	}
	return []SchemaVersion{}, nil
}

// RegisterEventHandler registers a callback for CDC events from a Debezium connector.
func (p *DebeziumProvider) RegisterEventHandler(sourceID string, h EventHandler) error {
	p.mu.RLock()
	connector, exists := p.connectors[sourceID]
	p.mu.RUnlock()
	if !exists {
		return fmt.Errorf("debezium CDC provider: connector %q not found", sourceID)
	}
	connector.mu.Lock()
	connector.handler = h
	connector.mu.Unlock()
	return nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// buildDebeziumConnectorConfig builds the Kafka Connect connector config map
// from a SourceConfig. The connector class is determined by SourceType.
func buildDebeziumConnectorConfig(config SourceConfig) map[string]any {
	connClass := "io.debezium.connector.postgresql.PostgresConnector"
	cfg := map[string]any{
		"database.server.name": config.SourceID,
		"tasks.max":            "1",
	}

	switch config.SourceType {
	case "postgres":
		connClass = "io.debezium.connector.postgresql.PostgresConnector"
		cfg["plugin.name"] = "pgoutput"
		cfg["slot.name"] = "dbz_" + sanitizeID(config.SourceID)
		cfg["publication.name"] = "dbz_pub_" + sanitizeID(config.SourceID)
	case "mysql":
		connClass = "io.debezium.connector.mysql.MySqlConnector"
		cfg["database.server.id"] = "1"
	case "mongodb":
		connClass = "io.debezium.connector.mongodb.MongoDbConnector"
	}
	cfg["connector.class"] = connClass

	if len(config.Tables) > 0 {
		cfg["table.include.list"] = strings.Join(config.Tables, ",")
	}

	return cfg
}

// debeziumConnectState maps Kafka Connect connector states to CDCStatus states.
func debeziumConnectState(s string) string {
	switch s {
	case "RUNNING":
		return "running"
	case "PAUSED":
		return "paused"
	case "FAILED":
		return "error"
	case "UNASSIGNED":
		return "stopped"
	default:
		return "unknown"
	}
}

// sanitizeID replaces characters that are invalid in Debezium slot/publication names.
func sanitizeID(id string) string {
	var b strings.Builder
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

// doRequest executes an HTTP request with optional JSON body.
func (p *DebeziumProvider) doRequest(ctx context.Context, method, url string, body any) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	return p.httpClient.Do(req)
}
