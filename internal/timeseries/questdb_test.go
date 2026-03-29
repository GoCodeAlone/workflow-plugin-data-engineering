package timeseries

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"
)

// mockLineSender implements qdb.LineSender for testing — no network required.
type mockLineSender struct {
	rows     []mockRow
	current  *mockRow
	flushed  bool
	closed   bool
	flushErr error
	closeErr error
	atErr    error
}

type mockRow struct {
	table  string
	tags   map[string]string
	fields map[string]any
	ts     time.Time
}

func (m *mockLineSender) Table(name string) qdb.LineSender {
	m.current = &mockRow{
		table:  name,
		tags:   make(map[string]string),
		fields: make(map[string]any),
	}
	return m
}
func (m *mockLineSender) Symbol(name, val string) qdb.LineSender {
	if m.current != nil {
		m.current.tags[name] = val
	}
	return m
}
func (m *mockLineSender) Int64Column(name string, val int64) qdb.LineSender {
	if m.current != nil {
		m.current.fields[name] = val
	}
	return m
}
func (m *mockLineSender) Long256Column(name string, val *big.Int) qdb.LineSender {
	if m.current != nil {
		m.current.fields[name] = val.String()
	}
	return m
}
func (m *mockLineSender) TimestampColumn(name string, ts time.Time) qdb.LineSender {
	if m.current != nil {
		m.current.fields[name] = ts
	}
	return m
}
func (m *mockLineSender) Float64Column(name string, val float64) qdb.LineSender {
	if m.current != nil {
		m.current.fields[name] = val
	}
	return m
}
func (m *mockLineSender) StringColumn(name, val string) qdb.LineSender {
	if m.current != nil {
		m.current.fields[name] = val
	}
	return m
}
func (m *mockLineSender) BoolColumn(name string, val bool) qdb.LineSender {
	if m.current != nil {
		m.current.fields[name] = val
	}
	return m
}
func (m *mockLineSender) At(ctx context.Context, ts time.Time) error {
	if m.atErr != nil {
		return m.atErr
	}
	if m.current != nil {
		m.current.ts = ts
		m.rows = append(m.rows, *m.current)
		m.current = nil
	}
	return nil
}
func (m *mockLineSender) AtNow(ctx context.Context) error { return m.At(ctx, time.Time{}) }
func (m *mockLineSender) Flush(_ context.Context) error   { m.flushed = true; return m.flushErr }
func (m *mockLineSender) Close(_ context.Context) error   { m.closed = true; return m.closeErr }

// mockSenderFactory injects a mock into tests.
func mockSenderFactory(mock *mockLineSender) SenderFactory {
	return func(_ context.Context, _ QuestDBConfig) (qdb.LineSender, error) {
		return mock, nil
	}
}

// --- Tests ---

func TestQuestDBModule_Init(t *testing.T) {
	mod, err := NewQuestDBModule("q", map[string]any{
		"ilpEndpoint":  "localhost:9009",
		"httpEndpoint": "http://localhost:9000",
	})
	if err != nil {
		t.Fatalf("NewQuestDBModule: %v", err)
	}
	if err := mod.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
}

func TestQuestDBModule_InvalidConfig(t *testing.T) {
	_, err := NewQuestDBModule("q", map[string]any{
		"flushInterval": "notaduration",
	})
	if err == nil {
		t.Fatal("expected error for invalid flushInterval")
	}
}

func TestQuestDBModule_Init_MissingEndpoints(t *testing.T) {
	tests := []struct {
		name   string
		config map[string]any
	}{
		{"missing ilp", map[string]any{"httpEndpoint": "http://localhost:9000"}},
		{"missing http", map[string]any{"ilpEndpoint": "localhost:9009"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mod, err := NewQuestDBModule("q", tc.config)
			if err != nil {
				t.Fatalf("NewQuestDBModule: %v", err)
			}
			if err := mod.Init(); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestQuestDBModule_Start_HealthCheck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	mock := &mockLineSender{}
	m := &QuestDBModule{
		name: "q",
		config: QuestDBConfig{
			ILPEndpoint:  "localhost:9009",
			HTTPEndpoint: srv.URL,
			AutoFlush:    true,
		},
		httpClient:    srv.Client(),
		senderFactory: mockSenderFactory(mock),
	}

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func TestQuestDBModule_Start_HealthCheckFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	m := &QuestDBModule{
		name: "q",
		config: QuestDBConfig{
			ILPEndpoint:  "localhost:9009",
			HTTPEndpoint: srv.URL,
		},
		httpClient:    srv.Client(),
		senderFactory: mockSenderFactory(&mockLineSender{}),
	}

	if err := m.Start(context.Background()); err == nil {
		t.Fatal("expected error on 500 health check")
	}
}

func TestQuestDBModule_Stop_Flushes(t *testing.T) {
	mock := &mockLineSender{}
	m := &QuestDBModule{
		name:   "q",
		sender: mock,
	}

	if err := m.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !mock.flushed {
		t.Error("expected Flush to be called on Stop")
	}
	if !mock.closed {
		t.Error("expected Close to be called on Stop")
	}
}

func TestQuestDBWriter_WritePoint_ILP(t *testing.T) {
	mock := &mockLineSender{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w := &questDBWriter{sender: mock, httpClient: srv.Client(), httpEndpoint: srv.URL}
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	err := w.WritePoint(context.Background(), "metrics",
		map[string]string{"host": "srv1"},
		map[string]any{"cpu": 0.85, "mem_used": int64(4096)},
		ts,
	)
	if err != nil {
		t.Fatalf("WritePoint: %v", err)
	}

	if len(mock.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(mock.rows))
	}
	row := mock.rows[0]
	if row.table != "metrics" {
		t.Errorf("table: got %q, want %q", row.table, "metrics")
	}
	if row.tags["host"] != "srv1" {
		t.Errorf("tag host: got %q, want %q", row.tags["host"], "srv1")
	}
	if row.fields["cpu"] != 0.85 {
		t.Errorf("field cpu: got %v", row.fields["cpu"])
	}
	if row.fields["mem_used"] != int64(4096) {
		t.Errorf("field mem_used: got %v", row.fields["mem_used"])
	}
	if !row.ts.Equal(ts) {
		t.Errorf("timestamp: got %v, want %v", row.ts, ts)
	}
}

func TestQuestDBWriter_WriteBatch(t *testing.T) {
	mock := &mockLineSender{}
	w := &questDBWriter{
		sender:       mock,
		httpClient:   http.DefaultClient,
		httpEndpoint: "http://localhost:9000",
	}

	points := []Point{
		{Measurement: "cpu", Tags: map[string]string{"host": "a"}, Fields: map[string]any{"value": 0.5}, Timestamp: time.Now()},
		{Measurement: "cpu", Tags: map[string]string{"host": "b"}, Fields: map[string]any{"value": 0.7}, Timestamp: time.Now()},
	}

	if err := w.WriteBatch(context.Background(), points); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	if len(mock.rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(mock.rows))
	}
	if !mock.flushed {
		t.Error("expected Flush after WriteBatch")
	}
}

func TestQuestDBWriter_Query_SQL(t *testing.T) {
	response := map[string]any{
		"columns": []map[string]any{
			{"name": "host", "type": "STRING"},
			{"name": "cpu", "type": "DOUBLE"},
		},
		"dataset": [][]any{
			{"srv1", 0.85},
			{"srv2", 0.42},
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/exec" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer srv.Close()

	writer := &questDBWriter{
		sender:       &mockLineSender{},
		httpClient:   srv.Client(),
		httpEndpoint: srv.URL,
	}

	rows, err := writer.Query(context.Background(), "SELECT host, cpu FROM metrics")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0]["host"] != "srv1" {
		t.Errorf("row[0].host: got %v", rows[0]["host"])
	}
	if rows[1]["cpu"] != 0.42 {
		t.Errorf("row[1].cpu: got %v", rows[1]["cpu"])
	}
}

func TestQuestDBWriter_Query_EmptyResult(t *testing.T) {
	response := map[string]any{
		"columns": []map[string]any{{"name": "ts", "type": "TIMESTAMP"}},
		"dataset": [][]any{},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer srv.Close()

	writer := &questDBWriter{httpClient: srv.Client(), httpEndpoint: srv.URL, sender: &mockLineSender{}}
	rows, err := writer.Query(context.Background(), "SELECT ts FROM empty")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestQuestDBWriter_Query_ErrorResponse(t *testing.T) {
	response := map[string]any{
		"error": "table 'nosuch' does not exist",
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer srv.Close()

	writer := &questDBWriter{httpClient: srv.Client(), httpEndpoint: srv.URL, sender: &mockLineSender{}}
	_, err := writer.Query(context.Background(), "SELECT * FROM nosuch")
	if err == nil {
		t.Fatal("expected error for QuestDB error response")
	}
}

func TestQuestDBModule_Register_Deregister(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	mock := &mockLineSender{}
	m := &QuestDBModule{
		name: "qtest-reg",
		config: QuestDBConfig{
			ILPEndpoint:  "localhost:9009",
			HTTPEndpoint: srv.URL,
			AutoFlush:    true,
		},
		httpClient:    srv.Client(),
		senderFactory: mockSenderFactory(mock),
	}

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Registered — should be findable.
	if _, err := Lookup("qtest-reg"); err != nil {
		t.Errorf("Lookup after Start: %v", err)
	}

	if err := m.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Deregistered.
	if _, err := Lookup("qtest-reg"); err == nil {
		t.Error("expected Lookup to fail after Stop")
	}
}
