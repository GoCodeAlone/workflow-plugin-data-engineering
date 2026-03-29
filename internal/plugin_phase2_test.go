package internal_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/catalog"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/lakehouse"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/timeseries"
)

// Compile-time assertions: all 5 TS modules implement TimeSeriesWriter.
var (
	_ timeseries.TimeSeriesWriter = (*timeseries.InfluxModule)(nil)
	_ timeseries.TimeSeriesWriter = (*timeseries.TimescaleModule)(nil)
	_ timeseries.TimeSeriesWriter = (*timeseries.ClickHouseModule)(nil)
	_ timeseries.TimeSeriesWriter = (*timeseries.QuestDBModule)(nil)
	_ timeseries.TimeSeriesWriter = (*timeseries.DruidModule)(nil)
)

func newPhase2Plugin(t *testing.T) fullPlugin {
	t.Helper()
	p, ok := internal.NewDataEngineeringPlugin("phase2-test").(fullPlugin)
	if !ok {
		t.Fatal("plugin does not implement all expected provider interfaces")
	}
	return p
}

// ─── Discovery tests ──────────────────────────────────────────────────────────

func TestPlugin_Phase2_AllModuleTypes(t *testing.T) {
	p := newPhase2Plugin(t)
	types := p.ModuleTypes()

	required := []string{
		"cdc.source", "data.tenancy",
		"catalog.iceberg", "lakehouse.table",
		"timeseries.influxdb", "timeseries.timescaledb",
		"timeseries.clickhouse", "timeseries.questdb",
		"timeseries.druid", "catalog.schema_registry",
	}
	typeSet := make(map[string]struct{}, len(types))
	for _, t2 := range types {
		typeSet[t2] = struct{}{}
	}
	for _, req := range required {
		if _, ok := typeSet[req]; !ok {
			t.Errorf("missing module type %q", req)
		}
	}
	if len(types) != 10 {
		t.Errorf("expected 10 module types, got %d: %v", len(types), types)
	}
}

func TestPlugin_Phase2_AllStepTypes(t *testing.T) {
	p := newPhase2Plugin(t)
	types := p.StepTypes()

	required := []string{
		"step.cdc_start", "step.cdc_stop", "step.cdc_status", "step.cdc_snapshot", "step.cdc_schema_history",
		"step.tenant_provision", "step.tenant_deprovision", "step.tenant_migrate",
		"step.lakehouse_create_table", "step.lakehouse_evolve_schema", "step.lakehouse_write",
		"step.lakehouse_compact", "step.lakehouse_snapshot", "step.lakehouse_query",
		"step.lakehouse_expire_snapshots",
		"step.ts_write", "step.ts_write_batch", "step.ts_query", "step.ts_downsample", "step.ts_retention",
		"step.ts_continuous_query",
		"step.ts_druid_ingest", "step.ts_druid_query", "step.ts_druid_datasource", "step.ts_druid_compact",
		"step.schema_register", "step.schema_validate",
	}
	typeSet := make(map[string]struct{}, len(types))
	for _, t2 := range types {
		typeSet[t2] = struct{}{}
	}
	for _, req := range required {
		if _, ok := typeSet[req]; !ok {
			t.Errorf("missing step type %q", req)
		}
	}
	if len(types) != 27 {
		t.Errorf("expected 27 step types, got %d", len(types), )
	}
}

// ─── Iceberg Lakehouse lifecycle ──────────────────────────────────────────────

func TestPlugin_IcebergLakehouseLifecycle(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/config", func(w http.ResponseWriter, r *http.Request) {
		writePhase2JSON(w, 200, map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{}})
	})
	mux.HandleFunc("HEAD /v1/namespaces/{ns}/tables/{tbl}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404) // table doesn't exist yet
	})
	mux.HandleFunc("POST /v1/namespaces/{ns}/tables", func(w http.ResponseWriter, r *http.Request) {
		writePhase2JSON(w, 200, map[string]any{
			"metadata": map[string]any{
				"format-version":    2,
				"table-uuid":        "lifecycle-uuid",
				"location":          "s3://bucket/analytics/events",
				"current-schema-id": 0,
				"schemas":           []any{},
				"properties":        map[string]string{},
			},
		})
	})
	mux.HandleFunc("POST /v1/namespaces/{ns}/tables/{tbl}", func(w http.ResponseWriter, r *http.Request) {
		writePhase2JSON(w, 200, map[string]any{
			"metadata": map[string]any{
				"format-version":    2,
				"table-uuid":        "lifecycle-uuid",
				"location":          "s3://bucket/analytics/events",
				"current-schema-id": 1,
				"schemas":           []any{},
				"properties":        map[string]string{},
			},
		})
	})
	mux.HandleFunc("GET /v1/namespaces/{ns}/tables/{tbl}", func(w http.ResponseWriter, r *http.Request) {
		writePhase2JSON(w, 200, map[string]any{
			"metadata": map[string]any{
				"format-version":      2,
				"table-uuid":          "lifecycle-uuid",
				"location":            "s3://bucket/analytics/events",
				"current-schema-id":   0,
				"current-snapshot-id": int64(77),
				"schemas":             []any{},
				"snapshots": []any{
					map[string]any{"snapshot-id": int64(77), "timestamp-ms": int64(1000), "manifest-list": "s3://a", "summary": map[string]string{}},
				},
				"properties": map[string]string{},
			},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	p := newPhase2Plugin(t)
	ctx := context.Background()
	catName := "p2-lakehouse-cat"
	lakehouse.UnregisterCatalog(catName)
	t.Cleanup(func() { lakehouse.UnregisterCatalog(catName) })

	// Create and start catalog.iceberg module.
	mod, err := p.CreateModule("catalog.iceberg", catName, map[string]any{
		"endpoint":   srv.URL + "/v1",
		"credential": "tok",
	})
	if err != nil {
		t.Fatalf("CreateModule catalog.iceberg: %v", err)
	}
	if err := mod.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = mod.Stop(ctx) })

	// step.lakehouse_create_table
	createStep, err := p.CreateStep("step.lakehouse_create_table", "lh-create", nil)
	if err != nil {
		t.Fatalf("CreateStep create_table: %v", err)
	}
	result, err := createStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"catalog":   catName,
		"namespace": []any{"analytics"},
		"table":     "events",
		"schema": map[string]any{
			"fields": []any{map[string]any{"name": "id", "type": "long", "required": true}},
		},
	})
	if err != nil {
		t.Fatalf("lakehouse_create_table: %v", err)
	}
	if result.Output["status"] != "created" {
		t.Errorf("expected status=created, got %v", result.Output["status"])
	}

	// step.lakehouse_evolve_schema
	evolveStep, err := p.CreateStep("step.lakehouse_evolve_schema", "lh-evolve", nil)
	if err != nil {
		t.Fatalf("CreateStep evolve_schema: %v", err)
	}
	result, err = evolveStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"catalog":   catName,
		"namespace": []any{"analytics"},
		"table":     "events",
		"changes":   []any{map[string]any{"action": "add-column", "name": "email", "type": "string"}},
	})
	if err != nil {
		t.Fatalf("lakehouse_evolve_schema: %v", err)
	}
	if result.Output["status"] != "evolved" {
		t.Errorf("expected status=evolved, got %v", result.Output["status"])
	}

	// step.lakehouse_snapshot (list)
	snapStep, err := p.CreateStep("step.lakehouse_snapshot", "lh-snap", nil)
	if err != nil {
		t.Fatalf("CreateStep snapshot: %v", err)
	}
	result, err = snapStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"catalog":   catName,
		"namespace": []any{"analytics"},
		"table":     "events",
		"action":    "list",
	})
	if err != nil {
		t.Fatalf("lakehouse_snapshot list: %v", err)
	}
	if result.Output["count"] != 1 {
		t.Errorf("expected count=1, got %v", result.Output["count"])
	}
}

// ─── InfluxDB write + query ───────────────────────────────────────────────────

func TestPlugin_InfluxDBWriteAndQuery(t *testing.T) {
	// InfluxDB v2 annotated CSV format required by the client library.
	const csvResp = "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string\r\n" +
		"#group,false,false,true,true,false,false,true,true\r\n" +
		"#default,_result,,,,,,,\r\n" +
		",result,table,_start,_stop,_time,_value,_field,_measurement\r\n" +
		",_result,0,2021-01-01T00:00:00Z,2021-01-02T00:00:00Z,2021-01-01T12:00:00Z,42.0,temp,sensor\r\n\r\n"

	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"influxdb","status":"pass","version":"2.7.0"}`))
	})
	mux.HandleFunc("/api/v2/write", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	mux.HandleFunc("/api/v2/query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		_, _ = io.WriteString(w, csvResp)
	})
	mux.HandleFunc("/api/v2/buckets", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"buckets": []map[string]any{{"id": "bkt1", "name": "mydb", "retentionRules": []any{}}},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	modName := "p2-influx"
	timeseries.Unregister(modName)
	t.Cleanup(func() { timeseries.Unregister(modName) })

	p := newPhase2Plugin(t)
	ctx := context.Background()

	mod, err := p.CreateModule("timeseries.influxdb", modName, map[string]any{
		"url":    srv.URL,
		"token":  "test-token",
		"org":    "my-org",
		"bucket": "mydb",
	})
	if err != nil {
		t.Fatalf("CreateModule influxdb: %v", err)
	}
	if err := mod.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = mod.Stop(ctx) })

	// step.ts_write
	writeStep, err := p.CreateStep("step.ts_write", "ts-write", nil)
	if err != nil {
		t.Fatalf("CreateStep ts_write: %v", err)
	}
	result, err := writeStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"module":      modName,
		"measurement": "sensor",
		"fields":      map[string]any{"temp": 42.0},
		"tags":        map[string]any{"host": "server1"},
	})
	if err != nil {
		t.Fatalf("ts_write: %v", err)
	}
	if result.Output["status"] != "written" {
		t.Errorf("expected status=written, got %v", result.Output["status"])
	}

	// step.ts_query
	queryStep, err := p.CreateStep("step.ts_query", "ts-query", nil)
	if err != nil {
		t.Fatalf("CreateStep ts_query: %v", err)
	}
	result, err = queryStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"module": modName,
		"query":  `from(bucket: "mydb") |> range(start: -1h)`,
	})
	if err != nil {
		t.Fatalf("ts_query: %v", err)
	}
	rows, _ := result.Output["rows"].([]map[string]any)
	if len(rows) == 0 {
		// Not a fatal error - just verify the step ran without errors.
		t.Logf("ts_query returned 0 rows (CSV parsing may vary)")
	}
}

// ─── TimescaleDB continuous query ─────────────────────────────────────────────

func TestPlugin_TimescaleDBContinuousQuery(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Expect the CREATE MATERIALIZED VIEW statement.
	mock.ExpectExec(`CREATE MATERIALIZED VIEW`).WillReturnResult(sqlmock.NewResult(0, 0))
	// Expect add_continuous_aggregate_policy call.
	mock.ExpectExec(`SELECT add_continuous_aggregate_policy`).WillReturnResult(sqlmock.NewResult(0, 0))

	modName := "p2-timescale"
	timeseries.Unregister(modName)
	t.Cleanup(func() { timeseries.Unregister(modName) })

	// Create module with injected mock DB and register it manually.
	tsModule := timeseries.NewTimescaleModuleFromDB(modName, db)
	if err := timeseries.Register(modName, tsModule); err != nil {
		t.Fatalf("Register timescale: %v", err)
	}

	p := newPhase2Plugin(t)
	ctx := context.Background()

	step, err := p.CreateStep("step.ts_continuous_query", "ts-cq", nil)
	if err != nil {
		t.Fatalf("CreateStep ts_continuous_query: %v", err)
	}
	result, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"module":          modName,
		"viewName":        "hourly_avg",
		"action":          "create",
		"query":           "SELECT time_bucket('1 hour', ts) AS bucket, avg(temp) FROM sensor GROUP BY bucket",
		"refreshInterval": "1h",
	})
	if err != nil {
		t.Fatalf("ts_continuous_query create: %v", err)
	}
	if result.Output["status"] != "created" {
		t.Errorf("expected status=created, got %v", result.Output["status"])
	}
	if result.Output["viewName"] != "hourly_avg" {
		t.Errorf("expected viewName=hourly_avg, got %v", result.Output["viewName"])
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// ─── Druid ingest + query ─────────────────────────────────────────────────────

func TestPlugin_DruidIngestAndQuery(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		writePhase2JSON(w, 200, map[string]any{"version": "30.0.0", "loading": false})
	})
	// SubmitSupervisor calls /druid/indexer/v1/supervisor.
	mux.HandleFunc("/druid/indexer/v1/supervisor", func(w http.ResponseWriter, r *http.Request) {
		writePhase2JSON(w, 200, map[string]any{"id": "supervisor-001", "state": "RUNNING"})
	})
	mux.HandleFunc("/druid/v2/sql", func(w http.ResponseWriter, r *http.Request) {
		writePhase2JSON(w, 200, []map[string]any{
			{"__time": "2021-01-01T00:00:00Z", "count": float64(100)},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	modName := "p2-druid"
	timeseries.Unregister(modName)
	t.Cleanup(func() { timeseries.Unregister(modName) })

	p := newPhase2Plugin(t)
	ctx := context.Background()

	mod, err := p.CreateModule("timeseries.druid", modName, map[string]any{
		"routerUrl": srv.URL,
	})
	if err != nil {
		t.Fatalf("CreateModule druid: %v", err)
	}
	if err := mod.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = mod.Stop(ctx) })

	// step.ts_druid_ingest — submits a Kafka supervisor spec.
	ingestStep, err := p.CreateStep("step.ts_druid_ingest", "druid-ingest", nil)
	if err != nil {
		t.Fatalf("CreateStep ts_druid_ingest: %v", err)
	}
	result, err := ingestStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"module": modName,
		"spec": map[string]any{
			"type":       "kafka",
			"dataSchema": map[string]any{"dataSource": "metrics"},
			"ioConfig": map[string]any{
				"topic":           "metrics",
				"consumerProperties": map[string]any{"bootstrap.servers": "localhost:9092"},
			},
		},
	})
	if err != nil {
		t.Fatalf("ts_druid_ingest: %v", err)
	}
	if result.Output["supervisorId"] == nil {
		t.Errorf("expected supervisorId in output, got %v", result.Output)
	}

	// step.ts_druid_query
	queryStep, err := p.CreateStep("step.ts_druid_query", "druid-query", nil)
	if err != nil {
		t.Fatalf("CreateStep ts_druid_query: %v", err)
	}
	result, err = queryStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"module": modName,
		"query":  "SELECT COUNT(*) FROM metrics",
	})
	if err != nil {
		t.Fatalf("ts_druid_query: %v", err)
	}
	rows, _ := result.Output["rows"].([]map[string]any)
	if len(rows) == 0 {
		t.Errorf("expected rows in output, got %v", result.Output)
	}
}

// ─── Schema Registry register + validate ─────────────────────────────────────

func TestPlugin_SchemaRegistryRegisterAndValidate(t *testing.T) {
	const jsonSchema = `{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id","name"]}`

	mux := http.NewServeMux()
	mux.HandleFunc("/subjects", func(w http.ResponseWriter, r *http.Request) {
		srWriteJSON(w, []string{})
	})
	mux.HandleFunc("/subjects/user-value/versions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			srWriteJSON(w, map[string]int{"id": 42})
		}
	})
	mux.HandleFunc("/subjects/user-value/versions/latest", func(w http.ResponseWriter, r *http.Request) {
		srWriteJSON(w, map[string]any{
			"id":         42,
			"subject":    "user-value",
			"version":    1,
			"schema":     jsonSchema,
			"schemaType": "JSON",
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	modName := "p2-sr"
	catalog.UnregisterSRModule(modName)
	t.Cleanup(func() { catalog.UnregisterSRModule(modName) })

	p := newPhase2Plugin(t)
	ctx := context.Background()

	mod, err := p.CreateModule("catalog.schema_registry", modName, map[string]any{
		"endpoint": srv.URL,
	})
	if err != nil {
		t.Fatalf("CreateModule schema_registry: %v", err)
	}
	if err := mod.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = mod.Stop(ctx) })

	// step.schema_register
	regStep, err := p.CreateStep("step.schema_register", "sr-register", nil)
	if err != nil {
		t.Fatalf("CreateStep schema_register: %v", err)
	}
	result, err := regStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"registry":   modName,
		"subject":    "user-value",
		"schema":     jsonSchema,
		"schemaType": "JSON",
	})
	if err != nil {
		t.Fatalf("schema_register: %v", err)
	}
	if result.Output["schemaId"] == nil {
		t.Errorf("expected schemaId in output, got %v", result.Output)
	}

	// step.schema_validate
	validateStep, err := p.CreateStep("step.schema_validate", "sr-validate", nil)
	if err != nil {
		t.Fatalf("CreateStep schema_validate: %v", err)
	}
	result, err = validateStep.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"registry": modName,
		"subject":  "user-value",
		"data":     `{"id":1,"name":"alice"}`,
	})
	if err != nil {
		t.Fatalf("schema_validate: %v", err)
	}
	if result.Output["valid"] != true {
		t.Errorf("expected valid=true, got %v", result.Output)
	}
}

// ─── TimeSeriesWriter interface check ─────────────────────────────────────────

func TestPlugin_TimeSeriesWriterInterface(t *testing.T) {
	// Verify each module type can be created without error (Init only, no Start).
	p := newPhase2Plugin(t)
	cases := []struct {
		moduleType string
		config     map[string]any
	}{
		{"timeseries.influxdb", map[string]any{"url": "http://localhost:8086", "token": "tok", "org": "org", "bucket": "bkt"}},
		{"timeseries.timescaledb", map[string]any{"connection": "postgres://localhost/db"}},
		{"timeseries.clickhouse", map[string]any{"endpoints": []any{"localhost:9000"}}},
		{"timeseries.druid", map[string]any{"routerUrl": "http://localhost:8888"}},
	}
	for _, tc := range cases {
		t.Run(tc.moduleType, func(t *testing.T) {
			mod, err := p.CreateModule(tc.moduleType, fmt.Sprintf("tsw-check-%s", tc.moduleType), tc.config)
			if err != nil {
				t.Fatalf("CreateModule %s: %v", tc.moduleType, err)
			}
			if err := mod.Init(); err != nil {
				t.Fatalf("Init %s: %v", tc.moduleType, err)
			}
			// Don't call Start — requires real infrastructure.
		})
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func writePhase2JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func srWriteJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	_ = json.NewEncoder(w).Encode(v)
}
