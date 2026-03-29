package internal_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/catalog"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/graph"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/migrate"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/quality"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

func writeP34JSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// ─── all module / step type coverage ─────────────────────────────────────────

func TestPlugin_Phase34_AllModuleTypes(t *testing.T) {
	p := newPlugin(t)
	typeSet := make(map[string]bool)
	for _, typ := range p.ModuleTypes() {
		typeSet[typ] = true
	}
	required := []string{
		"migrate.schema",
		"quality.checks",
		"graph.neo4j",
		"catalog.datahub",
		"catalog.openmetadata",
	}
	for _, req := range required {
		if !typeSet[req] {
			t.Errorf("missing module type %q", req)
		}
	}
}

func TestPlugin_Phase34_AllStepTypes(t *testing.T) {
	p := newPlugin(t)
	typeSet := make(map[string]bool)
	for _, typ := range p.StepTypes() {
		typeSet[typ] = true
	}
	required := []string{
		"step.migrate_plan", "step.migrate_apply", "step.migrate_run",
		"step.migrate_rollback", "step.migrate_status",
		"step.quality_check", "step.quality_schema_validate", "step.quality_profile",
		"step.quality_compare", "step.quality_anomaly",
		"step.quality_dbt_test", "step.quality_soda_check", "step.quality_ge_validate",
		"step.graph_query", "step.graph_write", "step.graph_import",
		"step.graph_extract_entities", "step.graph_link",
		"step.catalog_register", "step.catalog_search", "step.contract_validate",
	}
	for _, req := range required {
		if !typeSet[req] {
			t.Errorf("missing step type %q", req)
		}
	}
}

// ─── migrate lifecycle ────────────────────────────────────────────────────────

func TestPlugin_MigrateLifecycle(t *testing.T) {
	p := newPlugin(t)
	ctx := context.Background()

	mod, err := p.CreateModule("migrate.schema", "m_mig", map[string]any{
		"strategy": "scripted",
	})
	if err != nil {
		t.Fatalf("CreateModule migrate.schema: %v", err)
	}
	if err := mod.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = mod.Stop(ctx) })

	statusStep, err := p.CreateStep("step.migrate_status", "ms1", nil)
	if err != nil {
		t.Fatalf("CreateStep migrate_status: %v", err)
	}
	result, err := execStep(ctx, statusStep, map[string]any{"module": "m_mig"})
	if err != nil {
		t.Fatalf("migrate_status: %v", err)
	}
	if result.Output == nil {
		t.Error("expected non-nil output from migrate_status")
	}
}

func TestPlugin_MigratePlan_EmptyDeclarative(t *testing.T) {
	p := newPlugin(t)
	ctx := context.Background()

	mod, err := p.CreateModule("migrate.schema", "m_plan", map[string]any{
		"strategy": "declarative",
	})
	if err != nil {
		t.Fatalf("CreateModule: %v", err)
	}
	if err := mod.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		_ = mod.Stop(ctx)
		migrate.UnregisterModule("m_plan")
	})

	planStep, err := p.CreateStep("step.migrate_plan", "mp1", nil)
	if err != nil {
		t.Fatalf("CreateStep: %v", err)
	}
	result, err := execStep(ctx, planStep, map[string]any{"module": "m_plan"})
	if err != nil {
		t.Fatalf("migrate_plan: %v", err)
	}
	_ = result
}

// ─── quality checks ───────────────────────────────────────────────────────────

func TestPlugin_QualityChecks(t *testing.T) {
	p := newPlugin(t)
	ctx := context.Background()

	mod, err := p.CreateModule("quality.checks", "q_checks", map[string]any{
		"provider": "builtin",
	})
	if err != nil {
		t.Fatalf("CreateModule quality.checks: %v", err)
	}
	if err := mod.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		_ = mod.Stop(ctx)
		quality.UnregisterChecksModule("q_checks")
	})

	qMod := mod.(*quality.ChecksModule)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	qMod.SetExecutor(db)

	checkStep, err := p.CreateStep("step.quality_check", "qc1", nil)
	if err != nil {
		t.Fatalf("CreateStep quality_check: %v", err)
	}
	result, err := execStep(ctx, checkStep, map[string]any{
		"module": "q_checks",
		"table":  "orders",
		"checks": []any{
			map[string]any{"type": "not_null", "columns": []any{"id"}},
		},
	})
	if err != nil {
		t.Fatalf("quality_check: %v", err)
	}
	if result.Output["passed"] != true {
		t.Errorf("expected passed=true, got %v", result.Output["passed"])
	}
}

func TestPlugin_QualityProfile(t *testing.T) {
	p := newPlugin(t)
	ctx := context.Background()

	mod, err := p.CreateModule("quality.checks", "q_prof", map[string]any{
		"provider": "builtin",
	})
	if err != nil {
		t.Fatalf("CreateModule: %v", err)
	}
	if err := mod.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		_ = mod.Stop(ctx)
		quality.UnregisterChecksModule("q_prof")
	})

	qMod := mod.(*quality.ChecksModule)
	db2, mock2, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { db2.Close() })
	// row count
	mock2.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	// id column: null count, distinct count, min/max, raw values
	mock2.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock2.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock2.ExpectQuery("SELECT MIN").WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
	mock2.ExpectQuery("SELECT id FROM events").WillReturnRows(sqlmock.NewRows([]string{"id"}))
	// ts column: null count, distinct count, min/max, raw values
	mock2.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock2.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock2.ExpectQuery("SELECT MIN").WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
	mock2.ExpectQuery("SELECT ts FROM events").WillReturnRows(sqlmock.NewRows([]string{"ts"}))
	qMod.SetExecutor(db2)

	profileStep, err := p.CreateStep("step.quality_profile", "qp1", nil)
	if err != nil {
		t.Fatalf("CreateStep: %v", err)
	}
	result, err := execStep(ctx, profileStep, map[string]any{
		"module":  "q_prof",
		"table":   "events",
		"columns": []any{"id", "ts"},
	})
	if err != nil {
		t.Fatalf("quality_profile: %v", err)
	}
	if result.Output == nil {
		t.Error("expected non-nil output from quality_profile")
	}
}

// ─── graph steps ──────────────────────────────────────────────────────────────

func TestPlugin_GraphQuery(t *testing.T) {
	p := newPlugin(t)
	ctx := context.Background()

	p34RegisterGraphModule(t, "p34_gmod")

	queryStep, err := p.CreateStep("step.graph_query", "gq1", nil)
	if err != nil {
		t.Fatalf("CreateStep: %v", err)
	}
	result, err := execStep(ctx, queryStep, map[string]any{
		"module": "p34_gmod",
		"cypher": "MATCH (n) RETURN n.name AS name LIMIT 5",
	})
	if err != nil {
		t.Fatalf("graph_query: %v", err)
	}
	if result.Output["count"] == nil {
		t.Error("expected count in output")
	}
}

func TestPlugin_GraphExtractEntities(t *testing.T) {
	p := newPlugin(t)
	ctx := context.Background()

	step, err := p.CreateStep("step.graph_extract_entities", "ge1", nil)
	if err != nil {
		t.Fatalf("CreateStep: %v", err)
	}
	result, err := execStep(ctx, step, map[string]any{
		"text":  "Alice Smith works at Acme Corp in New York. Contact: alice@example.com",
		"types": []any{"person", "org", "location", "email"},
	})
	if err != nil {
		t.Fatalf("graph_extract_entities: %v", err)
	}
	count, _ := result.Output["count"].(int)
	if count == 0 {
		t.Error("expected at least one entity extracted")
	}
}

func TestPlugin_GraphWrite(t *testing.T) {
	p := newPlugin(t)
	ctx := context.Background()

	p34RegisterGraphModule(t, "p34_wmod")

	writeStep, err := p.CreateStep("step.graph_write", "gw1", nil)
	if err != nil {
		t.Fatalf("CreateStep: %v", err)
	}
	result, err := execStep(ctx, writeStep, map[string]any{
		"module": "p34_wmod",
		"nodes": []any{
			map[string]any{"label": "Person", "properties": map[string]any{"name": "Alice"}},
		},
	})
	if err != nil {
		t.Fatalf("graph_write: %v", err)
	}
	_ = result
}

// ─── catalog steps ────────────────────────────────────────────────────────────

func TestPlugin_CatalogRegister_DataHub(t *testing.T) {
	p := newPlugin(t)
	ctx := context.Background()

	mux := http.NewServeMux()
	mux.HandleFunc("/aspects", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("/entities", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	mod, err := p.CreateModule("catalog.datahub", "dh_p34", map[string]any{
		"endpoint": srv.URL,
	})
	if err != nil {
		t.Fatalf("CreateModule: %v", err)
	}
	if err := mod.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		_ = mod.Stop(ctx)
		catalog.UnregisterCatalogModule("dh_p34")
	})

	regStep, err := p.CreateStep("step.catalog_register", "cr_dh", nil)
	if err != nil {
		t.Fatalf("CreateStep: %v", err)
	}
	result, err := execStep(ctx, regStep, map[string]any{
		"catalog": "dh_p34",
		"dataset": "orders",
		"owner":   "alice",
		"tags":    []any{"finance"},
	})
	if err != nil {
		t.Fatalf("catalog_register: %v", err)
	}
	if result.Output["status"] != "registered" {
		t.Errorf("expected status=registered, got %v", result.Output["status"])
	}
}

func TestPlugin_CatalogRegister_OpenMetadata(t *testing.T) {
	p := newPlugin(t)
	ctx := context.Background()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/tables", func(w http.ResponseWriter, _ *http.Request) {
		writeP34JSON(w, 200, map[string]any{"name": "events"})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	mod, err := p.CreateModule("catalog.openmetadata", "om_p34", map[string]any{
		"endpoint": srv.URL,
	})
	if err != nil {
		t.Fatalf("CreateModule: %v", err)
	}
	if err := mod.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		_ = mod.Stop(ctx)
		catalog.UnregisterCatalogModule("om_p34")
	})

	regStep, err := p.CreateStep("step.catalog_register", "cr_om", nil)
	if err != nil {
		t.Fatalf("CreateStep: %v", err)
	}
	result, err := execStep(ctx, regStep, map[string]any{
		"catalog": "om_p34",
		"dataset": "events",
	})
	if err != nil {
		t.Fatalf("catalog_register: %v", err)
	}
	if result.Output["status"] != "registered" {
		t.Errorf("expected status=registered, got %v", result.Output["status"])
	}
}

func TestPlugin_CatalogSearch_DataHub(t *testing.T) {
	p := newPlugin(t)
	ctx := context.Background()

	mux := http.NewServeMux()
	mux.HandleFunc("/entities/v1/search", func(w http.ResponseWriter, _ *http.Request) {
		writeP34JSON(w, 200, map[string]any{
			"numEntities": 2,
			"entities": []any{
				map[string]any{"urn": "urn:li:dataset:users", "name": "users"},
				map[string]any{"urn": "urn:li:dataset:orders", "name": "orders"},
			},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	mod, err := p.CreateModule("catalog.datahub", "dh_srch_p34", map[string]any{
		"endpoint": srv.URL,
	})
	if err != nil {
		t.Fatalf("CreateModule: %v", err)
	}
	if err := mod.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		_ = mod.Stop(ctx)
		catalog.UnregisterCatalogModule("dh_srch_p34")
	})

	searchStep, err := p.CreateStep("step.catalog_search", "cs_p34", nil)
	if err != nil {
		t.Fatalf("CreateStep: %v", err)
	}
	result, err := execStep(ctx, searchStep, map[string]any{
		"catalog": "dh_srch_p34",
		"query":   "orders",
		"limit":   10,
	})
	if err != nil {
		t.Fatalf("catalog_search: %v", err)
	}
	if result.Output["total"] != 2 {
		t.Errorf("expected total=2, got %v", result.Output["total"])
	}
}

// ─── mock helpers ─────────────────────────────────────────────────────────────

// p34MockNeo4jDriver implements graph.Neo4jDriver for integration tests.
type p34MockNeo4jDriver struct{}

func (d *p34MockNeo4jDriver) NewSession(_ context.Context, _ neo4j.SessionConfig) graph.GraphSession {
	return &p34MockSession{}
}
func (d *p34MockNeo4jDriver) VerifyConnectivity(_ context.Context) error { return nil }
func (d *p34MockNeo4jDriver) Close(_ context.Context) error              { return nil }

// p34MockSession implements graph.GraphSession.
type p34MockSession struct{}

func (s *p34MockSession) Run(_ context.Context, _ string, _ map[string]any) (graph.GraphResult, error) {
	return &p34MockResult{}, nil
}
func (s *p34MockSession) Close(_ context.Context) error { return nil }

// p34MockResult implements graph.GraphResult.
type p34MockResult struct{}

func (r *p34MockResult) Next(_ context.Context) bool      { return false }
func (r *p34MockResult) Record() *neo4j.Record            { return nil }
func (r *p34MockResult) Err() error                       { return nil }

// p34RegisterGraphModule registers a pre-wired mock neo4j module.
func p34RegisterGraphModule(t *testing.T, name string) {
	t.Helper()
	graph.UnregisterNeo4jModule(name)
	m := graph.NewNeo4jModuleForTest(name, &p34MockNeo4jDriver{})
	if err := graph.RegisterNeo4jModule(name, m); err != nil {
		t.Fatalf("RegisterNeo4jModule %q: %v", name, err)
	}
	t.Cleanup(func() { graph.UnregisterNeo4jModule(name) })
}

