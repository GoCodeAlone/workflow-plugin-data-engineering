package timeseries

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// influxTestServer is a minimal InfluxDB v2 HTTP mock.
type influxTestServer struct {
	mu           sync.Mutex
	writeReqs    []string
	queryReqs    []string
	queryResp    string
	buckets      map[string]string // name -> id
	updatedRules map[string]any
	pingFail     bool
}

func newInfluxTestServer(t *testing.T) (*httptest.Server, *influxTestServer) {
	t.Helper()
	its := &influxTestServer{
		buckets: map[string]string{"default": "bucket-id-1", "b1": "bucket-id-2"},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		if its.pingFail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if its.pingFail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"influxdb","status":"pass","version":"2.7.0"}`))
	})
	mux.HandleFunc("/api/v2/write", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		its.mu.Lock()
		its.writeReqs = append(its.writeReqs, string(body))
		its.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/api/v2/query", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		its.mu.Lock()
		its.queryReqs = append(its.queryReqs, string(body))
		resp := its.queryResp
		its.mu.Unlock()
		if resp == "" {
			resp = defaultInfluxQueryResp
		}
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		_, _ = w.Write([]byte(resp))
	})
	mux.HandleFunc("/api/v2/buckets", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		id, ok := its.buckets[name]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":"not found"}`))
			return
		}
		resp := map[string]any{
			"buckets": []map[string]any{
				{"id": id, "name": name, "retentionRules": []any{}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/api/v2/buckets/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			body, _ := io.ReadAll(r.Body)
			var patch map[string]any
			_ = json.Unmarshal(body, &patch)
			its.mu.Lock()
			its.updatedRules = patch
			its.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(func() { srv.Close() })
	return srv, its
}

const defaultInfluxQueryResp = `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,0,2023-01-01T00:00:00Z,2023-12-31T00:00:00Z,2023-06-01T00:00:00Z,42.5,cpu_percent,cpu_usage,server1

`

func newStartedInfluxModule(t *testing.T, srvURL, name string) *InfluxModule {
	t.Helper()
	mod, err := NewInfluxModule(name, map[string]any{
		"url":    srvURL,
		"token":  "test-token",
		"org":    "my-org",
		"bucket": "default",
	})
	if err != nil {
		t.Fatalf("NewInfluxModule: %v", err)
	}
	im := mod.(*InfluxModule)
	if err := im.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := im.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = im.Stop(context.Background()) })
	return im
}

func TestInfluxModule_Init_And_Start(t *testing.T) {
	srv, _ := newInfluxTestServer(t)
	mod := newStartedInfluxModule(t, srv.URL, "influx-init-test")

	cfg := mod.Config()
	if cfg.Org != "my-org" {
		t.Errorf("org = %q, want %q", cfg.Org, "my-org")
	}
	if cfg.Bucket != "default" {
		t.Errorf("bucket = %q, want %q", cfg.Bucket, "default")
	}
}

func TestInfluxModule_Stop_Flushes(t *testing.T) {
	srv, _ := newInfluxTestServer(t)
	mod, err := NewInfluxModule("influx-stop-test", map[string]any{
		"url": srv.URL, "token": "tok", "org": "org1", "bucket": "default",
	})
	if err != nil {
		t.Fatal(err)
	}
	im := mod.(*InfluxModule)
	if err := im.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := im.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestInfluxModule_InvalidConfig(t *testing.T) {
	tests := []struct {
		name   string
		config map[string]any
		newErr bool
	}{
		{"missing url", map[string]any{"token": "t", "org": "o", "bucket": "b"}, true},
		{"missing token", map[string]any{"url": "http://x", "org": "o", "bucket": "b"}, false},
		{"missing org", map[string]any{"url": "http://x", "token": "t", "bucket": "b"}, false},
		{"missing bucket", map[string]any{"url": "http://x", "token": "t", "org": "o"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mod, err := NewInfluxModule("bad", tc.config)
			if tc.newErr {
				if err == nil {
					t.Error("expected NewInfluxModule error, got nil")
				}
				return
			}
			if err != nil {
				return // already errored at construction
			}
			if initErr := mod.(*InfluxModule).Init(); initErr == nil {
				t.Error("expected Init error, got nil")
			}
		})
	}
}

func TestTSWrite_SinglePoint(t *testing.T) {
	srv, its := newInfluxTestServer(t)
	_ = newStartedInfluxModule(t, srv.URL, "influx-write-test")

	ts := time.Date(2023, 6, 1, 12, 0, 0, 0, time.UTC)
	step, err := NewTSWriteStep("write1", nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":      "influx-write-test",
		"measurement": "cpu_usage",
		"tags":        map[string]any{"host": "server1", "region": "us-east-1"},
		"fields":      map[string]any{"value": 42.5},
		"timestamp":   ts,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "written" {
		t.Errorf("status = %v, want written", result.Output["status"])
	}
	if result.Output["measurement"] != "cpu_usage" {
		t.Errorf("measurement = %v, want cpu_usage", result.Output["measurement"])
	}

	its.mu.Lock()
	bodies := its.writeReqs
	its.mu.Unlock()

	if len(bodies) == 0 {
		t.Fatal("expected write request to influx server, got none")
	}
	body := bodies[len(bodies)-1]
	if !strings.Contains(body, "cpu_usage") {
		t.Errorf("line protocol body %q missing measurement name", body)
	}
}

func TestTSWriteBatch_MultiplePoints(t *testing.T) {
	srv, its := newInfluxTestServer(t)
	_ = newStartedInfluxModule(t, srv.URL, "influx-batch-test")

	step, err := NewTSWriteBatchStep("batch1", nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Now()
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "influx-batch-test",
		"points": []any{
			map[string]any{
				"measurement": "cpu_usage",
				"tags":        map[string]any{"host": "s1"},
				"fields":      map[string]any{"value": 10.0},
				"timestamp":   ts,
			},
			map[string]any{
				"measurement": "mem_usage",
				"tags":        map[string]any{"host": "s1"},
				"fields":      map[string]any{"value": 80.0},
				"timestamp":   ts,
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["count"] != 2 {
		t.Errorf("count = %v, want 2", result.Output["count"])
	}
	if result.Output["status"] != "written" {
		t.Errorf("status = %v, want written", result.Output["status"])
	}
	its.mu.Lock()
	bodies := its.writeReqs
	its.mu.Unlock()
	if len(bodies) == 0 {
		t.Fatal("expected write requests to influx server")
	}
}

func TestTSQuery_FluxQuery(t *testing.T) {
	srv, its := newInfluxTestServer(t)
	its.queryResp = defaultInfluxQueryResp
	_ = newStartedInfluxModule(t, srv.URL, "influx-query-test")

	step, err := NewTSQueryStep("query1", nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "influx-query-test",
		"query":  `from(bucket:"default") |> range(start: -1h) |> filter(fn: (r) => r._measurement == "cpu_usage")`,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	rows, ok := result.Output["rows"].([]map[string]any)
	if !ok {
		t.Fatalf("rows is %T, want []map[string]any", result.Output["rows"])
	}
	if len(rows) == 0 {
		t.Error("expected at least one row")
	}
	its.mu.Lock()
	qreqs := its.queryReqs
	its.mu.Unlock()
	if len(qreqs) == 0 {
		t.Fatal("expected query request to influx server")
	}
}

func TestTSDownsample_GeneratesFluxQuery(t *testing.T) {
	srv, _ := newInfluxTestServer(t)
	_ = newStartedInfluxModule(t, srv.URL, "influx-ds-test")

	step, err := NewTSDownsampleStep("ds1", nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":      "influx-ds-test",
		"source":      "cpu_usage",
		"target":      "cpu_hourly",
		"aggregation": "mean",
		"interval":    "1h",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "downsampled" {
		t.Errorf("status = %v, want downsampled", result.Output["status"])
	}
	q, _ := result.Output["query"].(string)
	if !strings.Contains(q, "cpu_usage") {
		t.Errorf("query %q missing source measurement", q)
	}
	if !strings.Contains(q, "cpu_hourly") {
		t.Errorf("query %q missing target measurement", q)
	}
	if !strings.Contains(q, "mean") {
		t.Errorf("query %q missing aggregation function", q)
	}
}

func TestTSRetention_UpdatesBucket(t *testing.T) {
	srv, its := newInfluxTestServer(t)
	_ = newStartedInfluxModule(t, srv.URL, "influx-ret-test")

	step, err := NewTSRetentionStep("ret1", nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":   "influx-ret-test",
		"bucket":   "default",
		"duration": "30d",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "updated" {
		t.Errorf("status = %v, want updated", result.Output["status"])
	}
	if result.Output["bucket"] != "default" {
		t.Errorf("bucket = %v, want default", result.Output["bucket"])
	}
	its.mu.Lock()
	rules := its.updatedRules
	its.mu.Unlock()
	if rules == nil {
		t.Error("expected bucket retention rules to be updated")
	}
}

func TestTSWrite_MissingModule(t *testing.T) {
	step, err := NewTSWriteStep("write-missing", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":      fmt.Sprintf("nonexistent-%d", time.Now().UnixNano()),
		"measurement": "test",
		"fields":      map[string]any{"v": 1.0},
	})
	if err == nil {
		t.Fatal("expected error for missing module, got nil")
	}
}

func TestTSQuery_EmptyResult(t *testing.T) {
	srv, its := newInfluxTestServer(t)
	its.queryResp = `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,

`
	_ = newStartedInfluxModule(t, srv.URL, "influx-empty-test")

	step, err := NewTSQueryStep("query-empty", nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "influx-empty-test",
		"query":  `from(bucket:"default") |> range(start: -1h)`,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	rows, ok := result.Output["rows"].([]map[string]any)
	if !ok {
		t.Fatalf("rows type %T", result.Output["rows"])
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
	if result.Output["count"] != 0 {
		t.Errorf("count = %v, want 0", result.Output["count"])
	}
}

func TestInfluxModule_BatchSizeAndPrecision(t *testing.T) {
	srv, _ := newInfluxTestServer(t)
	mod, err := NewInfluxModule("influx-prec-test", map[string]any{
		"url":           srv.URL,
		"token":         "tok",
		"org":           "org1",
		"bucket":        "default",
		"batchSize":     float64(500),
		"flushInterval": "500ms",
		"precision":     "ms",
	})
	if err != nil {
		t.Fatal(err)
	}
	im := mod.(*InfluxModule)
	if err := im.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = im.Stop(context.Background()) }()
	cfg := im.Config()
	if cfg.BatchSize != 500 {
		t.Errorf("batchSize = %d, want 500", cfg.BatchSize)
	}
	if cfg.Precision != "ms" {
		t.Errorf("precision = %q, want ms", cfg.Precision)
	}
}

func TestTSWriteBatch_EmptyPoints(t *testing.T) {
	step, err := NewTSWriteBatchStep("batch-empty", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "any",
		"points": []any{},
	})
	if err == nil {
		t.Error("expected error for empty points")
	}
}

func TestTSWrite_MissingMeasurement(t *testing.T) {
	step, err := NewTSWriteStep("write-no-meas", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "any",
		"fields": map[string]any{"v": 1.0},
	})
	if err == nil {
		t.Error("expected error for missing measurement")
	}
}

func TestInfluxModule_DuplicateRegister(t *testing.T) {
	srv, _ := newInfluxTestServer(t)
	name := fmt.Sprintf("dup-register-%d", time.Now().UnixNano())
	mod1, _ := NewInfluxModule(name, map[string]any{
		"url": srv.URL, "token": "tok", "org": "org1", "bucket": "default",
	})
	if err := mod1.(*InfluxModule).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mod1.(*InfluxModule).Stop(context.Background()) }()

	mod2, _ := NewInfluxModule(name, map[string]any{
		"url": srv.URL, "token": "tok", "org": "org1", "bucket": "default",
	})
	err := mod2.(*InfluxModule).Start(context.Background())
	if err == nil {
		_ = mod2.(*InfluxModule).Stop(context.Background())
		t.Error("expected error on duplicate registration")
	}
}
