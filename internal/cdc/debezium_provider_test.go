package cdc

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// kafkaConnectMock is a test helper that records calls made to the Kafka Connect REST API.
type kafkaConnectMock struct {
	mu       sync.Mutex
	calls    []mockCall
	handlers map[string]http.HandlerFunc // "METHOD /path" → handler
}

type mockCall struct {
	Method string
	Path   string
	Body   map[string]any
}

func newKafkaConnectMock() *kafkaConnectMock {
	return &kafkaConnectMock{handlers: make(map[string]http.HandlerFunc)}
}

func (m *kafkaConnectMock) handle(pattern string, fn http.HandlerFunc) {
	m.handlers[pattern] = fn
}

func (m *kafkaConnectMock) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	call := mockCall{Method: r.Method, Path: r.URL.Path}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &call.Body)
	}
	m.mu.Lock()
	m.calls = append(m.calls, call)
	m.mu.Unlock()

	key := r.Method + " " + r.URL.Path
	if fn, ok := m.handlers[key]; ok {
		r.Body = io.NopCloser(bytes.NewReader(body))
		fn(w, r)
		return
	}
	// Fallback: match prefix handlers (longest prefix wins, to avoid ambiguity).
	bestLen := -1
	var bestFn http.HandlerFunc
	for pattern, fn := range m.handlers {
		parts := strings.SplitN(pattern, " ", 2)
		if len(parts) == 2 && parts[0] == r.Method && strings.HasPrefix(r.URL.Path, parts[1]) {
			if len(parts[1]) > bestLen {
				bestLen = len(parts[1])
				bestFn = fn
			}
		}
	}
	if bestFn != nil {
		r.Body = io.NopCloser(bytes.NewReader(body))
		bestFn(w, r)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func (m *kafkaConnectMock) callsFor(method, pathPrefix string) []mockCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []mockCall
	for _, c := range m.calls {
		if c.Method == method && strings.HasPrefix(c.Path, pathPrefix) {
			out = append(out, c)
		}
	}
	return out
}

func jsonResp(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

func TestDebeziumProvider_CreatePostgresConnector(t *testing.T) {
	mock := newKafkaConnectMock()
	var capturedConfig map[string]any
	mock.handle("POST /connectors", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if cfg, ok := body["config"].(map[string]any); ok {
			capturedConfig = cfg
		}
		jsonResp(w, http.StatusCreated, body)
	})
	ts := httptest.NewServer(mock)
	defer ts.Close()

	p := newDebeziumProvider()
	cfg := SourceConfig{
		SourceID:   "pg-source",
		SourceType: "postgres",
		Connection: ts.URL,
		Tables:     []string{"public.users", "public.orders"},
	}
	if err := p.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	calls := mock.callsFor("POST", "/connectors")
	if len(calls) != 1 {
		t.Fatalf("expected 1 POST /connectors call, got %d", len(calls))
	}
	wantClass := "io.debezium.connector.postgresql.PostgresConnector"
	if capturedConfig["connector.class"] != wantClass {
		t.Errorf("connector.class = %v, want %v", capturedConfig["connector.class"], wantClass)
	}
	if capturedConfig["database.server.name"] != "pg-source" {
		t.Errorf("database.server.name = %v, want pg-source", capturedConfig["database.server.name"])
	}
	if !strings.Contains(capturedConfig["table.include.list"].(string), "public.users") {
		t.Errorf("table.include.list = %v, expected to contain public.users", capturedConfig["table.include.list"])
	}
}

func TestDebeziumProvider_CreateMySQLConnector(t *testing.T) {
	mock := newKafkaConnectMock()
	var capturedConfig map[string]any
	mock.handle("POST /connectors", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if cfg, ok := body["config"].(map[string]any); ok {
			capturedConfig = cfg
		}
		jsonResp(w, http.StatusCreated, body)
	})
	ts := httptest.NewServer(mock)
	defer ts.Close()

	p := newDebeziumProvider()
	if err := p.Connect(context.Background(), SourceConfig{
		SourceID:   "mysql-source",
		SourceType: "mysql",
		Connection: ts.URL,
	}); err != nil {
		t.Fatal(err)
	}

	wantClass := "io.debezium.connector.mysql.MySqlConnector"
	if capturedConfig["connector.class"] != wantClass {
		t.Errorf("connector.class = %v, want %v", capturedConfig["connector.class"], wantClass)
	}
	// MySQL requires server-id
	if _, ok := capturedConfig["database.server.id"]; !ok {
		t.Error("expected database.server.id in MySQL connector config")
	}
}

func TestDebeziumProvider_StatusMapping(t *testing.T) {
	tests := []struct {
		kcState   string
		wantState string
	}{
		{"RUNNING", "running"},
		{"PAUSED", "paused"},
		{"FAILED", "error"},
		{"UNASSIGNED", "stopped"},
		{"UNKNOWN_STATE", "unknown"},
	}
	for _, tc := range tests {
		t.Run(tc.kcState, func(t *testing.T) {
			mock := newKafkaConnectMock()
			mock.handle("POST /connectors", func(w http.ResponseWriter, r *http.Request) {
				jsonResp(w, http.StatusCreated, map[string]any{})
			})
			mock.handle("GET /connectors/", func(w http.ResponseWriter, r *http.Request) {
				jsonResp(w, http.StatusOK, map[string]any{
					"name": "workflow-status-test",
					"connector": map[string]any{
						"state":     tc.kcState,
						"worker_id": "localhost:8083",
					},
					"tasks": []any{},
				})
			})
			ts := httptest.NewServer(mock)
			defer ts.Close()

			p := newDebeziumProvider()
			_ = p.Connect(context.Background(), SourceConfig{
				SourceID: "status-test", SourceType: "postgres", Connection: ts.URL,
			})

			status, err := p.Status(context.Background(), "status-test")
			if err != nil {
				t.Fatal(err)
			}
			if status.State != tc.wantState {
				t.Errorf("kcState=%q: got State=%q want %q", tc.kcState, status.State, tc.wantState)
			}
			if status.Provider != "debezium" {
				t.Errorf("Provider = %q, want debezium", status.Provider)
			}
		})
	}
}

func TestDebeziumProvider_Stop(t *testing.T) {
	mock := newKafkaConnectMock()
	mock.handle("POST /connectors", func(w http.ResponseWriter, r *http.Request) {
		jsonResp(w, http.StatusCreated, map[string]any{})
	})
	mock.handle("DELETE /connectors/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	ts := httptest.NewServer(mock)
	defer ts.Close()

	p := newDebeziumProvider()
	cfg := SourceConfig{SourceID: "stop-test", SourceType: "postgres", Connection: ts.URL}
	if err := p.Connect(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if err := p.Disconnect(context.Background(), "stop-test"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	// Verify DELETE was called
	deleteCalls := mock.callsFor("DELETE", "/connectors/")
	if len(deleteCalls) != 1 {
		t.Errorf("expected 1 DELETE call, got %d", len(deleteCalls))
	}
	if !strings.Contains(deleteCalls[0].Path, "stop-test") {
		t.Errorf("DELETE path %q should contain connector name", deleteCalls[0].Path)
	}
}

func TestDebeziumProvider_Snapshot(t *testing.T) {
	mock := newKafkaConnectMock()
	mock.handle("POST /connectors", func(w http.ResponseWriter, r *http.Request) {
		jsonResp(w, http.StatusCreated, map[string]any{})
	})
	mock.handle("PUT /connectors/", func(w http.ResponseWriter, r *http.Request) {
		jsonResp(w, http.StatusOK, map[string]any{})
	})
	mock.handle("POST /connectors/", func(w http.ResponseWriter, r *http.Request) {
		// This matches the /connectors/{name}/restart POST
		jsonResp(w, http.StatusOK, map[string]any{})
	})
	ts := httptest.NewServer(mock)
	defer ts.Close()

	p := newDebeziumProvider()
	cfg := SourceConfig{SourceID: "snap-test", SourceType: "postgres", Connection: ts.URL}
	if err := p.Connect(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if err := p.Snapshot(context.Background(), "snap-test", []string{"public.users"}); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Verify PUT config was called (to set snapshot.mode)
	putCalls := mock.callsFor("PUT", "/connectors/")
	if len(putCalls) != 1 {
		t.Errorf("expected 1 PUT /connectors/{name}/config call, got %d", len(putCalls))
	}

	// Verify POST restart was called
	postCalls := mock.callsFor("POST", "/connectors/")
	// One for original Connect, one for restart
	restartCalls := 0
	for _, c := range postCalls {
		if strings.Contains(c.Path, "/restart") {
			restartCalls++
		}
	}
	if restartCalls != 1 {
		t.Errorf("expected 1 restart POST, got %d (all POSTs: %v)",
			restartCalls, mock.callsFor("POST", "/"))
	}
}

func TestDebeziumProvider_ConnectionError(t *testing.T) {
	p := newDebeziumProvider()
	cfg := SourceConfig{
		SourceID:   "unreachable",
		SourceType: "postgres",
		Connection: "http://localhost:0", // nothing listening
	}
	err := p.Connect(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error when Kafka Connect is unreachable")
	}
}

func TestDebeziumProvider_SchemaHistory(t *testing.T) {
	mock := newKafkaConnectMock()
	mock.handle("POST /connectors", func(w http.ResponseWriter, r *http.Request) {
		jsonResp(w, http.StatusCreated, map[string]any{})
	})
	mock.handle("GET /connectors/", func(w http.ResponseWriter, r *http.Request) {
		jsonResp(w, http.StatusOK, map[string]any{
			"name":      "workflow-history-test",
			"connector": map[string]any{"state": "RUNNING"},
			"tasks":     []any{},
		})
	})
	ts := httptest.NewServer(mock)
	defer ts.Close()

	p := newDebeziumProvider()
	cfg := SourceConfig{SourceID: "history-test", SourceType: "postgres", Connection: ts.URL}
	if err := p.Connect(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	history, err := p.SchemaHistory(context.Background(), "history-test", "public.users")
	if err != nil {
		t.Fatalf("SchemaHistory: %v", err)
	}
	// Debezium REST API does not expose full schema history; returns empty.
	if history == nil {
		t.Error("expected non-nil slice (may be empty)")
	}
}

func TestDebeziumProvider_DuplicateConnect(t *testing.T) {
	mock := newKafkaConnectMock()
	mock.handle("POST /connectors", func(w http.ResponseWriter, r *http.Request) {
		jsonResp(w, http.StatusCreated, map[string]any{})
	})
	ts := httptest.NewServer(mock)
	defer ts.Close()

	p := newDebeziumProvider()
	cfg := SourceConfig{SourceID: "dup", SourceType: "postgres", Connection: ts.URL}
	if err := p.Connect(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	err := p.Connect(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for duplicate connector")
	}
}

func TestDebeziumProvider_DisconnectNotFound(t *testing.T) {
	p := newDebeziumProvider()
	err := p.Disconnect(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error when disconnecting unknown source")
	}
}

func TestDebeziumProvider_RegisterEventHandler(t *testing.T) {
	mock := newKafkaConnectMock()
	mock.handle("POST /connectors", func(w http.ResponseWriter, r *http.Request) {
		jsonResp(w, http.StatusCreated, map[string]any{})
	})
	ts := httptest.NewServer(mock)
	defer ts.Close()

	p := newDebeziumProvider()
	cfg := SourceConfig{SourceID: "ev-test", SourceType: "postgres", Connection: ts.URL}
	if err := p.Connect(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}

	called := false
	handler := EventHandler(func(sourceID string, event map[string]any) error {
		called = true
		_ = called
		return nil
	})
	if err := p.RegisterEventHandler("ev-test", handler); err != nil {
		t.Fatalf("RegisterEventHandler: %v", err)
	}
}

func TestDebeziumProvider_RegisterEventHandler_NotFound(t *testing.T) {
	p := newDebeziumProvider()
	err := p.RegisterEventHandler("missing", func(string, map[string]any) error { return nil })
	if err == nil {
		t.Fatal("expected error for unknown source")
	}
}
