package timeseries

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// druidTestServer is a minimal mock Druid router.
type druidTestServer struct {
	mux *http.ServeMux
	srv *httptest.Server
}

func newDruidTestServer() *druidTestServer {
	mux := http.NewServeMux()
	s := &druidTestServer{mux: mux}
	s.srv = httptest.NewServer(mux)
	return s
}

func (s *druidTestServer) close() { s.srv.Close() }

func (s *druidTestServer) handle(path string, fn http.HandlerFunc) {
	s.mux.HandleFunc(path, fn)
}

func (s *druidTestServer) client() DruidClient {
	return NewDruidClient(s.srv.URL, "", "", 60*time.Second)
}

func jsonResp(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// --- Tests ---

func TestDruidModule_Init_Start(t *testing.T) {
	srv := newDruidTestServer()
	defer srv.close()
	srv.handle("/status", func(w http.ResponseWriter, _ *http.Request) {
		jsonResp(w, DruidStatus{Version: "30.0.0", Loading: false})
	})

	m := &DruidModule{
		name:   "druid-test",
		config: DruidConfig{RouterURL: srv.srv.URL},
		newClient: func(cfg DruidConfig) DruidClient {
			return NewDruidClient(cfg.RouterURL, cfg.Username, cfg.Password, 60*time.Second)
		},
	}

	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Should be in registry.
	if _, err := Lookup("druid-test"); err != nil {
		t.Errorf("Lookup after Start: %v", err)
	}

	_ = m.Stop(context.Background())
	if _, err := Lookup("druid-test"); err == nil {
		t.Error("expected Lookup to fail after Stop")
	}
}

func TestDruidModule_Init_MissingURL(t *testing.T) {
	m := &DruidModule{name: "d", config: DruidConfig{}}
	if err := m.Init(); err == nil {
		t.Fatal("expected error for missing routerUrl")
	}
}

func TestDruidClient_SQLQuery(t *testing.T) {
	srv := newDruidTestServer()
	defer srv.close()

	srv.handle("/druid/v2/sql", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		jsonResp(w, []map[string]any{
			{"host": "srv1", "cpu": 0.85},
			{"host": "srv2", "cpu": 0.42},
		})
	})

	result, err := srv.client().SQLQuery(context.Background(), "SELECT host, cpu FROM metrics", nil)
	if err != nil {
		t.Fatalf("SQLQuery: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0]["host"] != "srv1" {
		t.Errorf("row[0].host: got %v", result.Rows[0]["host"])
	}
}

func TestDruidClient_NativeQuery(t *testing.T) {
	srv := newDruidTestServer()
	defer srv.close()

	srv.handle("/druid/v2", func(w http.ResponseWriter, _ *http.Request) {
		jsonResp(w, []map[string]any{{"result": map[string]any{"count": 42}}})
	})

	query := map[string]any{
		"queryType": "timeseries",
		"dataSource": map[string]any{"type": "table", "name": "metrics"},
	}
	result, err := srv.client().NativeQuery(context.Background(), query)
	if err != nil {
		t.Fatalf("NativeQuery: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}
}

func TestDruidClient_SubmitSupervisor(t *testing.T) {
	srv := newDruidTestServer()
	defer srv.close()

	var receivedSpec map[string]any
	srv.handle("/druid/indexer/v1/supervisor", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&receivedSpec)
		jsonResp(w, SupervisorStatus{ID: "kafka-cdc-users", State: "RUNNING", Healthy: true})
	})

	spec := map[string]any{
		"type": "kafka",
		"dataSchema": map[string]any{
			"dataSource": "cdc_users",
		},
		"ioConfig": map[string]any{
			"topic": "cdc.public.users",
			"consumerProperties": map[string]any{
				"bootstrap.servers": "kafka:9092",
			},
		},
	}

	status, err := srv.client().SubmitSupervisor(context.Background(), spec)
	if err != nil {
		t.Fatalf("SubmitSupervisor: %v", err)
	}
	if status.ID != "kafka-cdc-users" {
		t.Errorf("supervisorId: got %q, want %q", status.ID, "kafka-cdc-users")
	}
	if receivedSpec["type"] != "kafka" {
		t.Errorf("spec.type: got %v", receivedSpec["type"])
	}
}

func TestDruidClient_GetSupervisorStatus(t *testing.T) {
	srv := newDruidTestServer()
	defer srv.close()

	srv.handle("/druid/indexer/v1/supervisor/my-supervisor/status", func(w http.ResponseWriter, _ *http.Request) {
		jsonResp(w, SupervisorStatus{ID: "my-supervisor", State: "RUNNING", Healthy: true})
	})

	status, err := srv.client().GetSupervisorStatus(context.Background(), "my-supervisor")
	if err != nil {
		t.Fatalf("GetSupervisorStatus: %v", err)
	}
	if !status.Healthy {
		t.Error("expected healthy=true")
	}
}

func TestDruidClient_SuspendResume(t *testing.T) {
	srv := newDruidTestServer()
	defer srv.close()

	suspended, resumed := false, false
	srv.handle("/druid/indexer/v1/supervisor/sv1/suspend", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			suspended = true
		}
		w.WriteHeader(http.StatusOK)
	})
	srv.handle("/druid/indexer/v1/supervisor/sv1/resume", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			resumed = true
		}
		w.WriteHeader(http.StatusOK)
	})

	if err := srv.client().SuspendSupervisor(context.Background(), "sv1"); err != nil {
		t.Fatalf("SuspendSupervisor: %v", err)
	}
	if !suspended {
		t.Error("suspend not called")
	}

	if err := srv.client().ResumeSupervisor(context.Background(), "sv1"); err != nil {
		t.Fatalf("ResumeSupervisor: %v", err)
	}
	if !resumed {
		t.Error("resume not called")
	}
}

func TestDruidClient_ListDatasources(t *testing.T) {
	srv := newDruidTestServer()
	defer srv.close()

	srv.handle("/druid/coordinator/v1/datasources", func(w http.ResponseWriter, _ *http.Request) {
		jsonResp(w, []string{"cdc_users", "cdc_orders", "metrics"})
	})

	sources, err := srv.client().ListDatasources(context.Background())
	if err != nil {
		t.Fatalf("ListDatasources: %v", err)
	}
	if len(sources) != 3 {
		t.Errorf("expected 3, got %d", len(sources))
	}
	if sources[0] != "cdc_users" {
		t.Errorf("sources[0]: got %q", sources[0])
	}
}

func TestDruidClient_Compaction(t *testing.T) {
	srv := newDruidTestServer()
	defer srv.close()

	var compBody map[string]any
	srv.handle("/druid/coordinator/v1/compaction/compact", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&compBody)
		w.WriteHeader(http.StatusOK)
	})
	srv.handle("/druid/coordinator/v1/compaction/progress", func(w http.ResponseWriter, r *http.Request) {
		jsonResp(w, CompactionStatus{DataSource: "cdc_users", State: "RUNNING"})
	})

	err := srv.client().SubmitCompaction(context.Background(), "cdc_users", CompactionConfig{
		TargetCompactionSizeBytes: 5000000,
		SkipOffsetFromLatest:      "P1D",
	})
	if err != nil {
		t.Fatalf("SubmitCompaction: %v", err)
	}
	if compBody["dataSource"] != "cdc_users" {
		t.Errorf("compBody.dataSource: got %v", compBody["dataSource"])
	}

	status, err := srv.client().GetCompactionStatus(context.Background(), "cdc_users")
	if err != nil {
		t.Fatalf("GetCompactionStatus: %v", err)
	}
	if status.DataSource != "cdc_users" {
		t.Errorf("status.DataSource: got %q", status.DataSource)
	}
}

func TestDruidClient_ErrorHandling(t *testing.T) {
	srv := newDruidTestServer()
	defer srv.close()

	srv.handle("/druid/v2/sql", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"Query not supported"}`, http.StatusBadRequest)
	})

	_, err := srv.client().SQLQuery(context.Background(), "INVALID", nil)
	if err == nil {
		t.Fatal("expected error on 400")
	}
}

// --- Step tests ---

func TestDruidIngestStep(t *testing.T) {
	srv := newDruidTestServer()
	defer srv.close()

	srv.handle("/status", func(w http.ResponseWriter, _ *http.Request) {
		jsonResp(w, DruidStatus{Version: "30.0.0"})
	})
	srv.handle("/druid/indexer/v1/supervisor", func(w http.ResponseWriter, _ *http.Request) {
		jsonResp(w, SupervisorStatus{ID: "kafka-sv", State: "RUNNING"})
	})

	m := &DruidModule{
		name:   "druid-ingest-test",
		config: DruidConfig{RouterURL: srv.srv.URL},
		newClient: func(cfg DruidConfig) DruidClient {
			return NewDruidClient(cfg.RouterURL, cfg.Username, cfg.Password, 60*time.Second)
		},
	}
	_ = m.Start(context.Background())
	defer m.Stop(context.Background())

	step, _ := NewDruidIngestStep("ingest", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "druid-ingest-test",
		"spec": map[string]any{
			"type": "kafka",
			"dataSchema": map[string]any{"dataSource": "cdc_users"},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["supervisorId"] != "kafka-sv" {
		t.Errorf("supervisorId: got %v", result.Output["supervisorId"])
	}
}

func TestDruidQueryStep_SQL(t *testing.T) {
	srv := newDruidTestServer()
	defer srv.close()

	srv.handle("/status", func(w http.ResponseWriter, _ *http.Request) {
		jsonResp(w, DruidStatus{Version: "30.0.0"})
	})
	srv.handle("/druid/v2/sql", func(w http.ResponseWriter, _ *http.Request) {
		jsonResp(w, []map[string]any{{"host": "a", "count": float64(100)}})
	})

	m := &DruidModule{
		name:   "druid-query-test",
		config: DruidConfig{RouterURL: srv.srv.URL},
		newClient: func(cfg DruidConfig) DruidClient {
			return NewDruidClient(cfg.RouterURL, cfg.Username, cfg.Password, 60*time.Second)
		},
	}
	_ = m.Start(context.Background())
	defer m.Stop(context.Background())

	step, _ := NewDruidQueryStep("query", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":    "druid-query-test",
		"query":     "SELECT host, count(*) FROM metrics GROUP BY host",
		"queryType": "sql",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	rows := result.Output["rows"].([]map[string]any)
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}
}

func TestDruidQueryStep_Native(t *testing.T) {
	srv := newDruidTestServer()
	defer srv.close()

	srv.handle("/status", func(w http.ResponseWriter, _ *http.Request) {
		jsonResp(w, DruidStatus{Version: "30.0.0"})
	})
	srv.handle("/druid/v2", func(w http.ResponseWriter, _ *http.Request) {
		jsonResp(w, []map[string]any{{"timestamp": "2026-01-01", "result": map[string]any{"count": 5}}})
	})

	m := &DruidModule{
		name:   "druid-native-test",
		config: DruidConfig{RouterURL: srv.srv.URL},
		newClient: func(cfg DruidConfig) DruidClient {
			return NewDruidClient(cfg.RouterURL, cfg.Username, cfg.Password, 60*time.Second)
		},
	}
	_ = m.Start(context.Background())
	defer m.Stop(context.Background())

	step, _ := NewDruidQueryStep("native", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":    "druid-native-test",
		"queryType": "native",
		"query": map[string]any{
			"queryType":  "timeseries",
			"dataSource": map[string]any{"type": "table", "name": "metrics"},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["count"] != 1 {
		t.Errorf("count: got %v", result.Output["count"])
	}
}

func TestDruidDatasourceStep(t *testing.T) {
	srv := newDruidTestServer()
	defer srv.close()

	srv.handle("/status", func(w http.ResponseWriter, _ *http.Request) {
		jsonResp(w, DruidStatus{Version: "30.0.0"})
	})
	srv.handle("/druid/coordinator/v1/datasources", func(w http.ResponseWriter, _ *http.Request) {
		jsonResp(w, []string{"ds1", "ds2"})
	})

	m := &DruidModule{
		name:   "druid-ds-test",
		config: DruidConfig{RouterURL: srv.srv.URL},
		newClient: func(cfg DruidConfig) DruidClient {
			return NewDruidClient(cfg.RouterURL, cfg.Username, cfg.Password, 60*time.Second)
		},
	}
	_ = m.Start(context.Background())
	defer m.Stop(context.Background())

	step, _ := NewDruidDatasourceStep("ds", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "druid-ds-test",
		"action": "list",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	sources := result.Output["datasources"].([]string)
	if len(sources) != 2 {
		t.Errorf("expected 2 datasources, got %d", len(sources))
	}
}

func TestDruidCompactStep(t *testing.T) {
	srv := newDruidTestServer()
	defer srv.close()

	srv.handle("/status", func(w http.ResponseWriter, _ *http.Request) {
		jsonResp(w, DruidStatus{Version: "30.0.0"})
	})
	srv.handle("/druid/coordinator/v1/compaction/compact", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	m := &DruidModule{
		name:   "druid-compact-test",
		config: DruidConfig{RouterURL: srv.srv.URL},
		newClient: func(cfg DruidConfig) DruidClient {
			return NewDruidClient(cfg.RouterURL, cfg.Username, cfg.Password, 60*time.Second)
		},
	}
	_ = m.Start(context.Background())
	defer m.Stop(context.Background())

	step, _ := NewDruidCompactStep("compact", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module":                    "druid-compact-test",
		"datasource":                "cdc_users",
		"targetCompactionSizeBytes": float64(5000000),
		"skipOffsetFromLatest":      "P1D",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "compaction_submitted" {
		t.Errorf("status: got %v", result.Output["status"])
	}
	if result.Output["datasource"] != "cdc_users" {
		t.Errorf("datasource: got %v", result.Output["datasource"])
	}
}

func TestDruidModule_WritePoint_WriteBatch(t *testing.T) {
	srv := newDruidTestServer()
	defer srv.close()

	srv.handle("/status", func(w http.ResponseWriter, _ *http.Request) {
		jsonResp(w, DruidStatus{Version: "30.0.0"})
	})
	var taskSubmitted bool
	srv.handle("/druid/indexer/v1/task", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			taskSubmitted = true
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"task":"t1"}`))
	})

	m := &DruidModule{
		name:   "druid-write-test",
		config: DruidConfig{RouterURL: srv.srv.URL},
		newClient: func(cfg DruidConfig) DruidClient {
			return NewDruidClient(cfg.RouterURL, cfg.Username, cfg.Password, 60*time.Second)
		},
	}
	_ = m.Start(context.Background())
	defer m.Stop(context.Background())

	err := m.WriteBatch(context.Background(), []Point{
		{
			Measurement: "metrics",
			Tags:        map[string]string{"host": "srv1"},
			Fields:      map[string]any{"cpu": 0.85},
			Timestamp:   time.Now(),
		},
	})
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if !taskSubmitted {
		t.Error("expected task submission")
	}
}
