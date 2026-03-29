package cdc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dms "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	dmstypes "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
)

// ─── BackpressureMonitor unit tests ──────────────────────────────────────────

func TestBackpressureMonitor_Evaluate_Healthy(t *testing.T) {
	mon := &BackpressureMonitor{
		ThresholdLagBytes:   1_000_000,
		ThresholdLagSeconds: 300,
		WarningMultiplier:   0.8,
	}
	bs := mon.Evaluate("src1", 0, 0)
	if bs.Status != "healthy" {
		t.Errorf("status = %q, want healthy", bs.Status)
	}
}

func TestBackpressureMonitor_Evaluate_Warning(t *testing.T) {
	mon := &BackpressureMonitor{
		ThresholdLagBytes:   1_000_000,
		ThresholdLagSeconds: 300,
		WarningMultiplier:   0.8,
	}
	// 85% of threshold → warning
	bs := mon.Evaluate("src1", 850_000, 0)
	if bs.Status != "warning" {
		t.Errorf("status = %q, want warning", bs.Status)
	}
}

func TestBackpressureMonitor_Evaluate_Critical(t *testing.T) {
	mon := &BackpressureMonitor{
		ThresholdLagBytes:   1_000_000,
		ThresholdLagSeconds: 300,
		WarningMultiplier:   0.8,
	}
	bs := mon.Evaluate("src1", 1_500_000, 0)
	if bs.Status != "critical" {
		t.Errorf("status = %q, want critical", bs.Status)
	}
}

func TestBackpressureMonitor_Evaluate_CriticalBySeconds(t *testing.T) {
	mon := &BackpressureMonitor{
		ThresholdLagBytes:   1_000_000_000,
		ThresholdLagSeconds: 60,
		WarningMultiplier:   0.8,
	}
	bs := mon.Evaluate("src1", 0, 90)
	if bs.Status != "critical" {
		t.Errorf("status = %q, want critical", bs.Status)
	}
}

// ─── step.cdc_backpressure ────────────────────────────────────────────────────

func setupBackpressureSource(t *testing.T, sourceID string) *MemoryProvider {
	t.Helper()
	p := NewMemoryProvider()
	if err := p.Connect(context.Background(), SourceConfig{SourceID: sourceID}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := RegisterSource(sourceID, p); err != nil {
		t.Fatalf("RegisterSource: %v", err)
	}
	t.Cleanup(func() { UnregisterSource(sourceID) })
	return p
}

func TestBackpressureCheck_Healthy(t *testing.T) {
	p := setupBackpressureSource(t, "bp-healthy")
	_ = p

	s := &backpressureStep{name: "test"}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"source_id": "bp-healthy",
		"action":    "check",
		"thresholds": map[string]any{
			"lag_bytes":   int64(1_000_000),
			"lag_seconds": int64(300),
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "healthy" {
		t.Errorf("status = %v, want healthy", result.Output["status"])
	}
	if result.Output["lag_bytes"].(int64) != 0 {
		t.Errorf("lag_bytes = %v, want 0", result.Output["lag_bytes"])
	}
}

func TestBackpressureCheck_Warning(t *testing.T) {
	p := setupBackpressureSource(t, "bp-warn")
	if err := p.SetLag("bp-warn", 850_000, 0); err != nil {
		t.Fatal(err)
	}

	s := &backpressureStep{name: "test"}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"source_id": "bp-warn",
		"action":    "check",
		"thresholds": map[string]any{
			"lag_bytes":   int64(1_000_000),
			"lag_seconds": int64(300),
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "warning" {
		t.Errorf("status = %v, want warning", result.Output["status"])
	}
}

func TestBackpressureCheck_Critical(t *testing.T) {
	p := setupBackpressureSource(t, "bp-crit")
	if err := p.SetLag("bp-crit", 2_000_000, 0); err != nil {
		t.Fatal(err)
	}

	s := &backpressureStep{name: "test"}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"source_id": "bp-crit",
		"action":    "check",
		"thresholds": map[string]any{
			"lag_bytes":   int64(1_000_000),
			"lag_seconds": int64(300),
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "critical" {
		t.Errorf("status = %v, want critical", result.Output["status"])
	}
}

func TestBackpressureCheck_MissingSourceID(t *testing.T) {
	s := &backpressureStep{name: "test"}
	_, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{})
	if err == nil {
		t.Fatal("expected error for missing source_id")
	}
}

func TestBackpressureCheck_UnknownAction(t *testing.T) {
	setupBackpressureSource(t, "bp-badact")
	s := &backpressureStep{name: "test"}
	_, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"source_id": "bp-badact",
		"action":    "invalid",
	})
	if err == nil {
		t.Fatal("expected error for unknown action")
	}
}

// ─── Throttle / Resume via Debezium httptest ──────────────────────────────────

func TestBackpressureThrottle(t *testing.T) {
	mock := newKafkaConnectMock()
	mock.handle("POST /connectors", func(w http.ResponseWriter, r *http.Request) {
		jsonResp(w, http.StatusCreated, map[string]any{})
	})
	mock.handle("GET /connectors/", func(w http.ResponseWriter, r *http.Request) {
		jsonResp(w, http.StatusOK, map[string]any{
			"connector": map[string]any{"state": "RUNNING"},
			"tasks":     []any{},
		})
	})
	mock.handle("PUT /connectors/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	ts := httptest.NewServer(mock)
	defer ts.Close()

	provider := newDebeziumProvider()
	cfg := SourceConfig{SourceID: "throttle-test", SourceType: "postgres", Connection: ts.URL}
	if err := provider.Connect(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if err := RegisterSource("throttle-test", provider); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { UnregisterSource("throttle-test") })

	s := &backpressureStep{name: "test"}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"source_id": "throttle-test",
		"action":    "throttle",
	})
	if err != nil {
		t.Fatalf("throttle: %v", err)
	}
	if result.Output["action"] != "throttled" {
		t.Errorf("action = %v, want throttled", result.Output["action"])
	}
	if result.Output["previous_state"] != "running" {
		t.Errorf("previous_state = %v, want running", result.Output["previous_state"])
	}
	// Verify PUT /pause was called
	putCalls := mock.callsFor("PUT", "/connectors/")
	pauseFound := false
	for _, c := range putCalls {
		if len(c.Path) > 0 && containsStr(c.Path, "/pause") {
			pauseFound = true
		}
	}
	if !pauseFound {
		t.Errorf("expected PUT /connectors/{name}/pause call, got %v", mock.callsFor("PUT", "/"))
	}
}

func TestBackpressureResume(t *testing.T) {
	mock := newKafkaConnectMock()
	mock.handle("POST /connectors", func(w http.ResponseWriter, r *http.Request) {
		jsonResp(w, http.StatusCreated, map[string]any{})
	})
	mock.handle("PUT /connectors/", func(w http.ResponseWriter, r *http.Request) {
		jsonResp(w, http.StatusOK, map[string]any{})
	})
	ts := httptest.NewServer(mock)
	defer ts.Close()

	provider := newDebeziumProvider()
	cfg := SourceConfig{SourceID: "resume-test", SourceType: "postgres", Connection: ts.URL}
	if err := provider.Connect(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if err := RegisterSource("resume-test", provider); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { UnregisterSource("resume-test") })

	s := &backpressureStep{name: "test"}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"source_id": "resume-test",
		"action":    "resume",
	})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if result.Output["action"] != "resumed" {
		t.Errorf("action = %v, want resumed", result.Output["action"])
	}
	// Verify PUT /resume was called
	putCalls := mock.callsFor("PUT", "/connectors/")
	resumeFound := false
	for _, c := range putCalls {
		if containsStr(c.Path, "/resume") {
			resumeFound = true
		}
	}
	if !resumeFound {
		t.Errorf("expected PUT /connectors/{name}/resume call, got %v", putCalls)
	}
}

// ─── Debezium lag from status response ───────────────────────────────────────

func TestBackpressure_Debezium(t *testing.T) {
	mock := newKafkaConnectMock()
	mock.handle("POST /connectors", func(w http.ResponseWriter, r *http.Request) {
		jsonResp(w, http.StatusCreated, map[string]any{})
	})
	// Status response includes optional lag fields.
	mock.handle("GET /connectors/", func(w http.ResponseWriter, r *http.Request) {
		jsonResp(w, http.StatusOK, map[string]any{
			"connector":   map[string]any{"state": "RUNNING"},
			"tasks":       []any{},
			"lag_bytes":   int64(900_000),
			"lag_seconds": int64(0),
		})
	})
	mock.handle("PUT /connectors/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	ts := httptest.NewServer(mock)
	defer ts.Close()

	provider := newDebeziumProvider()
	cfg := SourceConfig{SourceID: "dbz-lag-test", SourceType: "postgres", Connection: ts.URL}
	if err := provider.Connect(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if err := RegisterSource("dbz-lag-test", provider); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { UnregisterSource("dbz-lag-test") })

	s := &backpressureStep{name: "test"}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"source_id": "dbz-lag-test",
		"action":    "check",
		"thresholds": map[string]any{
			"lag_bytes":   int64(1_000_000),
			"lag_seconds": int64(300),
		},
	})
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	// 900k of 1M threshold → warning (90% > 80% multiplier)
	if result.Output["status"] != "warning" {
		t.Errorf("status = %v, want warning (lag_bytes=900k, threshold=1M, mult=0.8)", result.Output["status"])
	}
	if result.Output["lag_bytes"].(int64) != 900_000 {
		t.Errorf("lag_bytes = %v, want 900000", result.Output["lag_bytes"])
	}
}

// ─── DMS throttle/resume ──────────────────────────────────────────────────────

func TestBackpressure_DMS(t *testing.T) {
	mock := &mockDMSClient{
		stopFn: func(in *dms.StopReplicationTaskInput) (*dms.StopReplicationTaskOutput, error) {
			return &dms.StopReplicationTaskOutput{
				ReplicationTask: &dmstypes.ReplicationTask{Status: aws.String("stopped")},
			}, nil
		},
		startFn: func(in *dms.StartReplicationTaskInput) (*dms.StartReplicationTaskOutput, error) {
			return &dms.StartReplicationTaskOutput{
				ReplicationTask: &dmstypes.ReplicationTask{Status: aws.String("running")},
			}, nil
		},
		describeFn: func(in *dms.DescribeReplicationTasksInput) (*dms.DescribeReplicationTasksOutput, error) {
			return &dms.DescribeReplicationTasksOutput{
				ReplicationTasks: []dmstypes.ReplicationTask{{Status: aws.String("running")}},
			}, nil
		},
	}
	provider := newDMSProviderForTest(mock, testDMSConfig)
	if err := provider.Connect(context.Background(), SourceConfig{SourceID: "dms-bp-test"}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterSource("dms-bp-test", provider); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { UnregisterSource("dms-bp-test") })

	// Throttle: should call StopReplicationTask.
	s := &backpressureStep{name: "test"}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"source_id": "dms-bp-test",
		"action":    "throttle",
	})
	if err != nil {
		t.Fatalf("throttle: %v", err)
	}
	if result.Output["action"] != "throttled" {
		t.Errorf("action = %v, want throttled", result.Output["action"])
	}
	mock.mu.Lock()
	calls := mock.calls
	mock.mu.Unlock()
	hasStop := false
	for _, c := range calls {
		if c == "StopReplicationTask" {
			hasStop = true
		}
	}
	if !hasStop {
		t.Errorf("expected StopReplicationTask in throttle calls: %v", calls)
	}

	// Resume: should call StartReplicationTask with ResumeProcessing.
	var capturedStart *dms.StartReplicationTaskInput
	mock.startFn = func(in *dms.StartReplicationTaskInput) (*dms.StartReplicationTaskOutput, error) {
		capturedStart = in
		return &dms.StartReplicationTaskOutput{
			ReplicationTask: &dmstypes.ReplicationTask{Status: aws.String("running")},
		}, nil
	}
	result, err = s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"source_id": "dms-bp-test",
		"action":    "resume",
	})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if result.Output["action"] != "resumed" {
		t.Errorf("action = %v, want resumed", result.Output["action"])
	}
	if capturedStart == nil || capturedStart.StartReplicationTaskType != dmstypes.StartReplicationTaskTypeValueResumeProcessing {
		t.Errorf("expected ResumeProcessing start type, got %v", capturedStart)
	}
}

// ─── step.cdc_monitor ────────────────────────────────────────────────────────

func TestCDCMonitor_AutoThrottle(t *testing.T) {
	mock := newKafkaConnectMock()
	mock.handle("POST /connectors", func(w http.ResponseWriter, r *http.Request) {
		jsonResp(w, http.StatusCreated, map[string]any{})
	})
	mock.handle("GET /connectors/", func(w http.ResponseWriter, r *http.Request) {
		// Return critical lag
		jsonResp(w, http.StatusOK, map[string]any{
			"connector":   map[string]any{"state": "RUNNING"},
			"tasks":       []any{},
			"lag_bytes":   int64(0),
			"lag_seconds": int64(400), // exceeds 300s threshold
		})
	})
	mock.handle("PUT /connectors/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	ts := httptest.NewServer(mock)
	defer ts.Close()

	provider := newDebeziumProvider()
	cfg := SourceConfig{SourceID: "monitor-test", SourceType: "postgres", Connection: ts.URL}
	if err := provider.Connect(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if err := RegisterSource("monitor-test", provider); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { UnregisterSource("monitor-test") })

	s := &monitorStep{name: "test"}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"source_id": "monitor-test",
		"thresholds": map[string]any{
			"lag_bytes":   int64(1_073_741_824),
			"lag_seconds": int64(300),
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "critical" {
		t.Errorf("status = %v, want critical", result.Output["status"])
	}
	if result.Output["auto_throttled"] != true {
		t.Errorf("auto_throttled = %v, want true", result.Output["auto_throttled"])
	}
	if result.Output["alerts_sent"].(int) < 1 {
		t.Errorf("alerts_sent = %v, want >= 1", result.Output["alerts_sent"])
	}
}

func TestCDCMonitor_Healthy(t *testing.T) {
	setupBackpressureSource(t, "monitor-healthy")

	s := &monitorStep{name: "test"}
	result, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"source_id": "monitor-healthy",
		"thresholds": map[string]any{
			"lag_bytes":   int64(1_000_000),
			"lag_seconds": int64(300),
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "healthy" {
		t.Errorf("status = %v, want healthy", result.Output["status"])
	}
	if result.Output["auto_throttled"] != false {
		t.Errorf("auto_throttled = %v, want false", result.Output["auto_throttled"])
	}
}

func TestCDCMonitor_MissingSourceID(t *testing.T) {
	s := &monitorStep{name: "test"}
	_, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{})
	if err == nil {
		t.Fatal("expected error for missing source_id")
	}
}

// ─── provider throttle not supported ─────────────────────────────────────────

func TestBackpressureThrottle_ProviderNotThrottleable(t *testing.T) {
	// MemoryProvider does not implement ThrottleableProvider.
	setupBackpressureSource(t, "bp-nothrottle")

	s := &backpressureStep{name: "test"}
	_, err := s.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"source_id": "bp-nothrottle",
		"action":    "throttle",
	})
	if err == nil {
		t.Fatal("expected error when provider does not support throttling")
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsStrHelper(s, sub))
}

func containsStrHelper(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// Verify json serialisation of lag fields (existing tests must still pass).
func TestCDCStatus_LagFieldsSerialized(t *testing.T) {
	st := &CDCStatus{
		SourceID:   "s1",
		State:      "running",
		Provider:   "memory",
		LagBytes:   12345,
		LagSeconds: 67,
	}
	data, err := json.Marshal(st)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	_ = json.Unmarshal(data, &m)
	if m["lag_bytes"] == nil {
		t.Error("lag_bytes missing from JSON")
	}
	if m["lag_seconds"] == nil {
		t.Error("lag_seconds missing from JSON")
	}
}
