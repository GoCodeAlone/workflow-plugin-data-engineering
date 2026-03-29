package timeseries

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	parquet "github.com/parquet-go/parquet-go"
)

// -- Parquet write test --

func TestParquetWrite(t *testing.T) {
	dir := t.TempDir()
	rows := []map[string]any{
		{"time": time.Now(), "measurement": "cpu", "value": 42.5},
		{"time": time.Now().Add(-time.Hour), "measurement": "cpu", "value": 38.1},
	}
	filePath, bytes, err := writeArchiveFile(rows, dir, "cpu", "parquet")
	if err != nil {
		t.Fatalf("writeArchiveFile: %v", err)
	}
	if bytes == 0 {
		t.Error("expected non-zero bytes written")
	}

	// Verify the file is valid parquet by reading it back
	f, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	defer f.Close()

	reader := parquet.NewGenericReader[archiveRow](f, parquet.NewSchema("archive", parquet.SchemaOf(archiveRow{})))
	defer reader.Close()

	buf := make([]archiveRow, 10)
	n, _ := reader.Read(buf)
	if n != len(rows) {
		t.Errorf("expected %d rows in parquet file, got %d", len(rows), n)
	}
}

func TestParquetWrite_CSV(t *testing.T) {
	dir := t.TempDir()
	rows := []map[string]any{
		{"time": time.Now(), "measurement": "mem", "value": 100.0},
	}
	filePath, bytes, err := writeArchiveFile(rows, dir, "mem", "csv")
	if err != nil {
		t.Fatalf("writeArchiveFile csv: %v", err)
	}
	if bytes == 0 {
		t.Error("expected non-zero bytes written for csv")
	}
	if ext := filepath.Ext(filePath); ext != ".csv" {
		t.Errorf("expected .csv extension, got %q", ext)
	}
}

// -- InfluxDB tiering tests --

// newInfluxTestServerWithDeleteHandler creates an httptest.Server that handles /api/v2/delete
// plus all regular InfluxDB endpoints (by proxying to an underlying influxTestServer).
func newInfluxTestServerWithDeleteHandler(t *testing.T) (*httptest.Server, *influxTestServer, *atomic.Int32) {
	t.Helper()
	// First start the base influx mock
	baseSrv, its := newInfluxTestServer(t)
	var deleteCalls atomic.Int32

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/delete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "wrong method", http.StatusMethodNotAllowed)
			return
		}
		deleteCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Proxy everything else to base server
		proxyURL := baseSrv.URL + r.RequestURI
		req, err := http.NewRequestWithContext(r.Context(), r.Method, proxyURL, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for k, v := range r.Header {
			req.Header[k] = v
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		buf := make([]byte, 32*1024)
		for {
			n, readErr := resp.Body.Read(buf)
			if n > 0 {
				_, _ = w.Write(buf[:n])
			}
			if readErr != nil {
				break
			}
		}
	})
	combinedSrv := httptest.NewServer(mux)
	t.Cleanup(func() { combinedSrv.Close() })
	return combinedSrv, its, &deleteCalls
}

func TestTSArchive_InfluxDB(t *testing.T) {
	srv, _, _ := newInfluxTestServerWithDeleteHandler(t)

	mod, err := NewInfluxModule("influx-archive", map[string]any{
		"url": srv.URL, "token": "tok", "org": "org1", "bucket": "default",
	})
	if err != nil {
		t.Fatalf("NewInfluxModule: %v", err)
	}
	im := mod.(*InfluxModule)
	if err := im.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = im.Stop(context.Background()) })

	dest := t.TempDir()
	result, err := im.Archive(context.Background(), 30*24*time.Hour, dest, "parquet", false)
	if err != nil {
		t.Fatalf("Archive: %v", err)
	}
	if result.RowsArchived < 0 {
		t.Error("rowsArchived should be >= 0")
	}
	if result.Destination == "" {
		t.Error("expected non-empty destination")
	}
}

func TestTSArchive_InfluxDB_Step(t *testing.T) {
	srv, _, _ := newInfluxTestServerWithDeleteHandler(t)

	mod, err := NewInfluxModule("influx-archive-step", map[string]any{
		"url": srv.URL, "token": "tok", "org": "org1", "bucket": "default",
	})
	if err != nil {
		t.Fatalf("NewInfluxModule: %v", err)
	}
	im := mod.(*InfluxModule)
	if err := im.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = im.Stop(context.Background()) })

	dest := t.TempDir()
	step, _ := NewTSArchiveStep("arch1", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":      "influx-archive-step",
		"olderThan":   "720h",
		"destination": dest,
		"format":      "parquet",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "archived" {
		t.Errorf("expected status=archived, got %v", result.Output["status"])
	}
}

func TestArchive_DeleteAfter(t *testing.T) {
	srv, _, deleteCalls := newInfluxTestServerWithDeleteHandler(t)

	mod, err := NewInfluxModule("influx-delete-test", map[string]any{
		"url": srv.URL, "token": "tok", "org": "org1", "bucket": "default",
	})
	if err != nil {
		t.Fatalf("NewInfluxModule: %v", err)
	}
	im := mod.(*InfluxModule)
	if err := im.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = im.Stop(context.Background()) })

	dest := t.TempDir()
	// The default query response returns 1 row, so delete should be triggered
	result, err := im.Archive(context.Background(), 30*24*time.Hour, dest, "parquet", true)
	if err != nil {
		t.Fatalf("Archive with deleteAfter: %v", err)
	}
	if result.RowsArchived > 0 && !result.DeletedFromHot {
		t.Error("expected DeletedFromHot=true when deleteAfterArchive=true and rows > 0")
	}
	if result.RowsArchived > 0 && deleteCalls.Load() == 0 {
		t.Errorf("expected delete endpoint to be called, got %d calls", deleteCalls.Load())
	}
}

// -- ClickHouse tiering tests --

func newCHModuleForTiering(t *testing.T, name string, mockConn *mockClickHouseConn) *ClickHouseModule {
	t.Helper()
	m := &ClickHouseModule{
		name:   name,
		config: ClickHouseConfig{Endpoints: []string{"localhost:9000"}, Database: "default"},
		conn:   mockConn,
	}
	if err := Register(name, m); err != nil {
		t.Fatalf("register %q: %v", name, err)
	}
	t.Cleanup(func() { Unregister(name) })
	return m
}

func TestTSArchive_ClickHouse(t *testing.T) {
	mockConn := &mockClickHouseConn{
		queryRows: &mockRows{
			cols: []string{"ts", "tag_keys", "tag_values", "field_keys", "field_values"},
			colData: [][]any{
				{time.Now().Add(-48 * time.Hour), []string{"host"}, []string{"srv1"}, []string{"cpu"}, []string{"99.0"}},
			},
		},
	}
	m := newCHModuleForTiering(t, "ch-archive", mockConn)

	dest := t.TempDir()
	result, err := m.Archive(context.Background(), 24*time.Hour, dest, "parquet", false)
	if err != nil {
		t.Fatalf("Archive: %v", err)
	}
	if result.RowsArchived != 1 {
		t.Errorf("expected 1 row archived, got %d", result.RowsArchived)
	}
	if result.DeletedFromHot {
		t.Error("expected deleteFromHot=false when deleteAfterArchive=false")
	}
	if _, err := os.Stat(result.Destination); err != nil {
		t.Errorf("archive file not found: %v", err)
	}
}

func TestTSArchive_ClickHouse_DeleteAfter(t *testing.T) {
	mockConn := &mockClickHouseConn{
		queryRows: &mockRows{
			cols: []string{"ts", "tag_keys", "tag_values", "field_keys", "field_values"},
			colData: [][]any{
				{time.Now().Add(-48 * time.Hour), []string{"host"}, []string{"srv1"}, []string{"cpu"}, []string{"99.0"}},
			},
		},
	}
	m := newCHModuleForTiering(t, "ch-archive-del", mockConn)

	dest := t.TempDir()
	result, err := m.Archive(context.Background(), 24*time.Hour, dest, "parquet", true)
	if err != nil {
		t.Fatalf("Archive: %v", err)
	}
	if !result.DeletedFromHot {
		t.Error("expected deleteFromHot=true when deleteAfterArchive=true and rows > 0")
	}
	mockConn.mu.Lock()
	queries := mockConn.execQueries
	mockConn.mu.Unlock()
	if len(queries) == 0 {
		t.Error("expected DELETE SQL to be executed")
	}
	found := false
	for _, q := range queries {
		if strings.Contains(q, "DELETE") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected DELETE in exec queries, got: %v", queries)
	}
}

func TestTSTierStatus(t *testing.T) {
	mockConn := &mockClickHouseConn{
		queryRows: &mockRows{
			cols: []string{"rows", "bytes", "oldest"},
			colData: [][]any{
				{uint64(1000), uint64(512000), time.Now().Add(-72 * time.Hour)},
			},
		},
	}
	m := newCHModuleForTiering(t, "ch-status", mockConn)

	status, err := m.Status(context.Background(), "events")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.HotRows != 1000 {
		t.Errorf("expected hotRows=1000, got %d", status.HotRows)
	}
	if status.TotalBytes != 512000 {
		t.Errorf("expected totalBytes=512000, got %d", status.TotalBytes)
	}
	if status.HotOldestTimestamp == nil {
		t.Error("expected hotOldestTimestamp to be set")
	}
}

func TestTSTierStatus_Step(t *testing.T) {
	mockConn := &mockClickHouseConn{
		queryRows: &mockRows{
			cols: []string{"rows", "bytes", "oldest"},
			colData: [][]any{
				{uint64(500), uint64(256000), time.Now().Add(-24 * time.Hour)},
			},
		},
	}
	newCHModuleForTiering(t, "ch-status-step", mockConn)

	step, _ := NewTSTierStatusStep("status1", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":      "ch-status-step",
		"measurement": "metrics",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["hotRows"] == nil {
		t.Error("expected hotRows in output")
	}
}

func TestTSArchive_Step_MissingModule(t *testing.T) {
	step, _ := NewTSArchiveStep("arch_bad", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":      fmt.Sprintf("nonexistent-%d", time.Now().UnixNano()),
		"olderThan":   "30d",
		"destination": t.TempDir(),
	})
	if err == nil {
		t.Fatal("expected error for missing module")
	}
}

func TestTSArchive_Step_NoTierSupport(t *testing.T) {
	name := fmt.Sprintf("non-tier-%d", time.Now().UnixNano())
	mock := &mockNonTierWriter{}
	if err := Register(name, mock); err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { Unregister(name) })

	step, _ := NewTSArchiveStep("arch_notier", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":      name,
		"olderThan":   "30d",
		"destination": t.TempDir(),
	})
	if err == nil {
		t.Fatal("expected error for non-tier module")
	}
}

// mockNonTierWriter implements TimeSeriesWriter but NOT TierManager.
type mockNonTierWriter struct{}

func (m *mockNonTierWriter) WritePoint(_ context.Context, _ string, _ map[string]string, _ map[string]any, _ time.Time) error {
	return nil
}
func (m *mockNonTierWriter) WriteBatch(_ context.Context, _ []Point) error { return nil }
func (m *mockNonTierWriter) Query(_ context.Context, _ string, _ ...any) ([]map[string]any, error) {
	return nil, nil
}
