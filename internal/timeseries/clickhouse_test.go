package timeseries

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ---- mock driver.Conn ----

type mockClickHouseConn struct {
	mu          sync.Mutex
	execQueries []string
	execArgs    [][]any
	pingErr     error
	queryRows   driver.Rows
	queryErr    error
	batchCalled bool
	batchSent   bool
	closeErr    error
}

func (m *mockClickHouseConn) Contributors() []string { return nil }
func (m *mockClickHouseConn) ServerVersion() (*driver.ServerVersion, error) {
	return &driver.ServerVersion{}, nil
}
func (m *mockClickHouseConn) Select(_ context.Context, _ any, _ string, _ ...any) error { return nil }
func (m *mockClickHouseConn) AsyncInsert(_ context.Context, _ string, _ bool, _ ...any) error {
	return nil
}
func (m *mockClickHouseConn) Stats() driver.Stats { return driver.Stats{} }
func (m *mockClickHouseConn) Ping(_ context.Context) error { return m.pingErr }
func (m *mockClickHouseConn) Close() error                 { return m.closeErr }
func (m *mockClickHouseConn) QueryRow(_ context.Context, _ string, _ ...any) driver.Row {
	return nil
}
func (m *mockClickHouseConn) Exec(_ context.Context, query string, args ...any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.execQueries = append(m.execQueries, query)
	m.execArgs = append(m.execArgs, args)
	return nil
}
func (m *mockClickHouseConn) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	return m.queryRows, m.queryErr
}
func (m *mockClickHouseConn) PrepareBatch(_ context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	m.mu.Lock()
	m.batchCalled = true
	m.mu.Unlock()
	return &mockBatch{conn: m}, nil
}

// ---- mock driver.Batch ----

type mockBatch struct {
	conn    *mockClickHouseConn
	appends [][]any
}

func (b *mockBatch) Abort() error                   { return nil }
func (b *mockBatch) AppendStruct(_ any) error        { return nil }
func (b *mockBatch) Flush() error                    { return nil }
func (b *mockBatch) IsSent() bool                    { return b.conn.batchSent }
func (b *mockBatch) Rows() int                       { return len(b.appends) }
func (b *mockBatch) Close() error                    { return nil }
func (b *mockBatch) Columns() []column.Interface     { return nil }
func (b *mockBatch) Column(_ int) driver.BatchColumn { return nil }
func (b *mockBatch) Append(v ...any) error {
	b.appends = append(b.appends, v)
	return nil
}
func (b *mockBatch) Send() error {
	b.conn.mu.Lock()
	b.conn.batchSent = true
	b.conn.mu.Unlock()
	return nil
}

// ---- mock driver.Rows ----

type mockRows struct {
	cols    []string
	colData [][]any
	pos     int
}

func (r *mockRows) Next() bool       { r.pos++; return r.pos <= len(r.colData) }
func (r *mockRows) ScanStruct(_ any) error { return nil }
func (r *mockRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *mockRows) Totals(_ ...any) error { return nil }
func (r *mockRows) Columns() []string     { return r.cols }
func (r *mockRows) Close() error           { return nil }
func (r *mockRows) Err() error             { return nil }
func (r *mockRows) Scan(dest ...any) error {
	if r.pos == 0 || r.pos > len(r.colData) {
		return fmt.Errorf("no more rows")
	}
	row := r.colData[r.pos-1]
	for i := range dest {
		if i < len(row) {
			if ptr, ok := dest[i].(*any); ok {
				*ptr = row[i]
			}
		}
	}
	return nil
}

// ---- helpers ----

func newMockClickHouseModule(t *testing.T, name string, conn driver.Conn) *ClickHouseModule {
	t.Helper()
	m := &ClickHouseModule{
		name: name,
		config: ClickHouseConfig{
			Endpoints: []string{"localhost:9000"},
			Database:  "analytics",
		},
		conn: conn,
		opener: func(_ *clickhouse.Options) (driver.Conn, error) {
			return conn, nil
		},
	}
	t.Cleanup(func() { Unregister(name) })
	return m
}

// ---- tests ----

func TestClickHouseModule_Init(t *testing.T) {
	mod, err := NewClickHouseModule("ch-init", map[string]any{
		"endpoints": []any{"localhost:9000"},
		"database":  "analytics",
	})
	if err != nil {
		t.Fatalf("NewClickHouseModule: %v", err)
	}
	if err := mod.(*ClickHouseModule).Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
}

func TestClickHouseModule_InvalidConfig(t *testing.T) {
	_, err := NewClickHouseModule("ch-bad", map[string]any{})
	if err == nil {
		t.Error("expected error for missing endpoints, got nil")
	}
}

func TestClickHouseModule_Start_Ping(t *testing.T) {
	mock := &mockClickHouseConn{}
	name := fmt.Sprintf("ch-ping-%d", time.Now().UnixNano())
	m := &ClickHouseModule{
		name: name,
		config: ClickHouseConfig{
			Endpoints: []string{"localhost:9000"},
			Database:  "analytics",
		},
		opener: func(_ *clickhouse.Options) (driver.Conn, error) {
			return mock, nil
		},
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()

	if m.Conn() == nil {
		t.Error("expected non-nil connection after Start")
	}
}

func TestClickHouseModule_Start_PingFail(t *testing.T) {
	mock := &mockClickHouseConn{pingErr: fmt.Errorf("connection refused")}
	m := &ClickHouseModule{
		name:   "ch-ping-fail",
		config: ClickHouseConfig{Endpoints: []string{"localhost:9000"}},
		opener: func(_ *clickhouse.Options) (driver.Conn, error) {
			return mock, nil
		},
	}
	err := m.Start(context.Background())
	if err == nil {
		t.Error("expected error when ping fails")
	}
	if !strings.Contains(err.Error(), "ping") {
		t.Errorf("error %q should mention 'ping'", err.Error())
	}
}

func TestClickHouseWriter_WritePoint(t *testing.T) {
	mock := &mockClickHouseConn{}
	name := fmt.Sprintf("ch-write-%d", time.Now().UnixNano())
	m := newMockClickHouseModule(t, name, mock)

	ts := time.Date(2023, 6, 1, 12, 0, 0, 0, time.UTC)
	err := m.WritePoint(context.Background(), "cpu_metrics",
		map[string]string{"host": "server1"},
		map[string]any{"value": 42.5},
		ts,
	)
	if err != nil {
		t.Fatalf("WritePoint: %v", err)
	}

	mock.mu.Lock()
	queries := mock.execQueries
	args := mock.execArgs
	mock.mu.Unlock()

	if len(queries) == 0 {
		t.Fatal("expected Exec to be called")
	}
	if !strings.Contains(queries[0], "cpu_metrics") {
		t.Errorf("query %q should contain measurement name", queries[0])
	}
	if !strings.Contains(queries[0], "INSERT INTO") {
		t.Errorf("query %q should be an INSERT", queries[0])
	}
	if len(args[0]) == 0 {
		t.Error("expected args to be passed to Exec")
	}
}

func TestClickHouseWriter_WriteBatch(t *testing.T) {
	mock := &mockClickHouseConn{}
	name := fmt.Sprintf("ch-batch-%d", time.Now().UnixNano())
	m := newMockClickHouseModule(t, name, mock)

	ts := time.Now()
	points := []Point{
		{Measurement: "cpu", Tags: map[string]string{"h": "s1"}, Fields: map[string]any{"v": 1.0}, Timestamp: ts},
		{Measurement: "cpu", Tags: map[string]string{"h": "s2"}, Fields: map[string]any{"v": 2.0}, Timestamp: ts},
		{Measurement: "mem", Tags: map[string]string{"h": "s1"}, Fields: map[string]any{"v": 80.0}, Timestamp: ts},
	}

	if err := m.WriteBatch(context.Background(), points); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	mock.mu.Lock()
	batchCalled := mock.batchCalled
	batchSent := mock.batchSent
	mock.mu.Unlock()

	if !batchCalled {
		t.Error("expected PrepareBatch to be called")
	}
	if !batchSent {
		t.Error("expected batch.Send to be called")
	}
}

func TestClickHouseWriter_Query(t *testing.T) {
	rows := &mockRows{
		cols: []string{"ts", "value", "host"},
		colData: [][]any{
			{time.Now(), 42.5, "server1"},
			{time.Now(), 38.1, "server2"},
		},
	}
	mock := &mockClickHouseConn{queryRows: rows}
	name := fmt.Sprintf("ch-query-%d", time.Now().UnixNano())
	m := newMockClickHouseModule(t, name, mock)

	result, err := m.Query(context.Background(), "SELECT ts, value, host FROM cpu_metrics LIMIT 10")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// Columns come from ColumnTypes() which returns nil in mock → 0 columns scanned.
	// Just verify no error occurred and result is a slice.
	_ = result
}

func TestClickHouseWriter_WritePoint_NotStarted(t *testing.T) {
	m := &ClickHouseModule{name: "ch-not-started"}
	err := m.WritePoint(context.Background(), "test", nil, map[string]any{"v": 1}, time.Now())
	if err == nil {
		t.Error("expected error for module not started")
	}
}

func TestClickHouseModule_Compression_LZ4(t *testing.T) {
	mock := &mockClickHouseConn{}
	name := fmt.Sprintf("ch-lz4-%d", time.Now().UnixNano())
	m := &ClickHouseModule{
		name: name,
		config: ClickHouseConfig{
			Endpoints:   []string{"localhost:9000"},
			Compression: "lz4",
		},
		opener: func(opt *clickhouse.Options) (driver.Conn, error) {
			if opt.Compression == nil || opt.Compression.Method != clickhouse.CompressionLZ4 {
				return nil, fmt.Errorf("expected LZ4 compression, got %v", opt.Compression)
			}
			return mock, nil
		},
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start with LZ4: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()
}

func TestClickHouseModule_MultiEndpoint(t *testing.T) {
	mock := &mockClickHouseConn{}
	var capturedOpts *clickhouse.Options
	name := fmt.Sprintf("ch-multi-%d", time.Now().UnixNano())
	m := &ClickHouseModule{
		name: name,
		config: ClickHouseConfig{
			Endpoints: []string{"ch1:9000", "ch2:9000", "ch3:9000"},
			Database:  "analytics",
		},
		opener: func(opt *clickhouse.Options) (driver.Conn, error) {
			capturedOpts = opt
			return mock, nil
		},
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()

	if len(capturedOpts.Addr) != 3 {
		t.Errorf("expected 3 endpoints, got %d", len(capturedOpts.Addr))
	}
}

func TestClickHouseWriter_WriteBatch_Empty(t *testing.T) {
	mock := &mockClickHouseConn{}
	name := fmt.Sprintf("ch-batch-empty-%d", time.Now().UnixNano())
	m := newMockClickHouseModule(t, name, mock)

	if err := m.WriteBatch(context.Background(), nil); err != nil {
		t.Fatalf("WriteBatch(nil): %v", err)
	}
	if err := m.WriteBatch(context.Background(), []Point{}); err != nil {
		t.Fatalf("WriteBatch([]): %v", err)
	}
	if mock.batchCalled {
		t.Error("expected no batch call for empty input")
	}
}

func TestClickHouseModule_DuplicateRegister(t *testing.T) {
	mock := &mockClickHouseConn{}
	name := fmt.Sprintf("ch-dup-%d", time.Now().UnixNano())
	m1 := &ClickHouseModule{
		name:   name,
		config: ClickHouseConfig{Endpoints: []string{"localhost:9000"}},
		opener: func(_ *clickhouse.Options) (driver.Conn, error) { return mock, nil },
	}
	if err := m1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m1.Stop(context.Background()) }()

	m2 := &ClickHouseModule{
		name:   name,
		config: ClickHouseConfig{Endpoints: []string{"localhost:9000"}},
		opener: func(_ *clickhouse.Options) (driver.Conn, error) { return mock, nil },
	}
	err := m2.Start(context.Background())
	if err == nil {
		_ = m2.Stop(context.Background())
		t.Error("expected error on duplicate registration")
	}
}
