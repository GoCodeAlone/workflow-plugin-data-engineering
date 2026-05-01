package internal_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/cdc"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// fullPlugin combines all SDK provider interfaces for testing convenience.
type fullPlugin interface {
	Manifest() sdk.PluginManifest
	ModuleTypes() []string
	CreateModule(typeName, name string, config map[string]any) (sdk.ModuleInstance, error)
	StepTypes() []string
	CreateStep(typeName, name string, config map[string]any) (sdk.StepInstance, error)
	TriggerTypes() []string
	CreateTrigger(typeName string, config map[string]any, cb sdk.TriggerCallback) (sdk.TriggerInstance, error)
	ModuleSchemas() []sdk.ModuleSchemaData
}

func newPlugin(t *testing.T) fullPlugin {
	t.Helper()
	p, ok := internal.NewDataEngineeringPlugin("test").(fullPlugin)
	if !ok {
		t.Fatal("plugin does not implement all expected provider interfaces")
	}
	return p
}

// execStep calls step.Execute with only the runtime config populated.
func execStep(ctx context.Context, step sdk.StepInstance, config map[string]any) (*sdk.StepResult, error) {
	return step.Execute(ctx, nil, nil, nil, nil, config)
}

// ─── metadata tests ──────────────────────────────────────────────────────────

func TestPlugin_AllModuleTypes(t *testing.T) {
	p := newPlugin(t)
	types := p.ModuleTypes()
	want := map[string]bool{
		"cdc.source": false, "data.tenancy": false,
		"catalog.iceberg": false, "lakehouse.table": false,
		"timeseries.influxdb": false, "timeseries.timescaledb": false,
		"timeseries.clickhouse": false, "timeseries.questdb": false,
		"timeseries.druid": false, "catalog.schema_registry": false,
	}
	for _, typ := range types {
		want[typ] = true
	}
	for typ, found := range want {
		if !found {
			t.Errorf("missing module type %q in ModuleTypes()", typ)
		}
	}
}

func TestPlugin_AllStepTypes(t *testing.T) {
	p := newPlugin(t)
	types := p.StepTypes()
	want := map[string]bool{
		"step.cdc_start":                  false,
		"step.cdc_stop":                   false,
		"step.cdc_status":                 false,
		"step.cdc_snapshot":               false,
		"step.cdc_schema_history":         false,
		"step.tenant_provision":           false,
		"step.tenant_deprovision":         false,
		"step.tenant_migrate":             false,
		"step.lakehouse_create_table":     false,
		"step.lakehouse_evolve_schema":    false,
		"step.lakehouse_write":            false,
		"step.lakehouse_compact":          false,
		"step.lakehouse_snapshot":         false,
		"step.lakehouse_query":            false,
		"step.lakehouse_expire_snapshots": false,
		// Time-series steps
		"step.ts_write":             false,
		"step.ts_write_batch":       false,
		"step.ts_query":             false,
		"step.ts_downsample":        false,
		"step.ts_retention":         false,
		"step.ts_continuous_query":  false,
		"step.ts_druid_ingest":      false,
		"step.ts_druid_query":       false,
		"step.ts_druid_datasource":  false,
		"step.ts_druid_compact":     false,
		"step.schema_register":      false,
		"step.schema_validate":      false,
	}
	for _, typ := range types {
		want[typ] = true
	}
	for typ, found := range want {
		if !found {
			t.Errorf("missing step type %q in StepTypes()", typ)
		}
	}
	if len(types) != 64 {
		t.Errorf("expected 64 step types, got %d", len(types))
	}
}

func TestPlugin_AllSchemas(t *testing.T) {
	p := newPlugin(t)
	schemas := p.ModuleSchemas()
	if len(schemas) != 15 {
		t.Fatalf("expected 15 schemas, got %d", len(schemas))
	}
	byType := make(map[string]sdk.ModuleSchemaData)
	for _, s := range schemas {
		byType[s.Type] = s
	}
	for _, typ := range []string{
		"cdc.source", "data.tenancy", "catalog.iceberg", "lakehouse.table",
		"timeseries.influxdb", "timeseries.timescaledb", "timeseries.clickhouse",
		"timeseries.questdb", "timeseries.druid", "catalog.schema_registry",
	} {
		s, ok := byType[typ]
		if !ok {
			t.Errorf("missing schema for type %q", typ)
			continue
		}
		if s.Label == "" {
			t.Errorf("%q schema: Label is empty", typ)
		}
		if s.Description == "" {
			t.Errorf("%q schema: Description is empty", typ)
		}
		if len(s.ConfigFields) == 0 {
			t.Errorf("%q schema: no ConfigFields defined", typ)
		}
		// Verify required fields have the Required flag set
		hasRequired := false
		for _, f := range s.ConfigFields {
			if f.Required {
				hasRequired = true
				break
			}
		}
		if !hasRequired {
			t.Errorf("%q schema: no required config fields defined", typ)
		}
	}
}

func TestPlugin_UnknownTypes(t *testing.T) {
	p := newPlugin(t)
	ctx := context.Background()

	if _, err := p.CreateModule("no.such.module", "x", nil); err == nil {
		t.Error("CreateModule with unknown type should return error")
	}
	if _, err := p.CreateStep("no.such.step", "x", nil); err == nil {
		t.Error("CreateStep with unknown type should return error")
	}
	if _, err := p.CreateTrigger("no.such.trigger", nil, func(_ string, _ map[string]any) error { return nil }); err == nil {
		t.Error("CreateTrigger with unknown type should return error")
	}
	_ = ctx
}

// ─── CDC full lifecycle ───────────────────────────────────────────────────────

func TestPlugin_CDCFullLifecycle(t *testing.T) {
	p := newPlugin(t)
	ctx := context.Background()
	sourceID := "integ-cdc-lifecycle"
	cdc.UnregisterSource(sourceID)
	defer cdc.UnregisterSource(sourceID)

	mod, err := p.CreateModule("cdc.source", "lifecycle-cdc", map[string]any{
		"provider":    "memory",
		"source_id":   sourceID,
		"source_type": "postgres",
		"connection":  "postgres://localhost/testdb",
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

	// step.cdc_status
	statusStep, err := p.CreateStep("step.cdc_status", "s-status", nil)
	if err != nil {
		t.Fatalf("CreateStep cdc_status: %v", err)
	}
	result, err := execStep(ctx, statusStep, map[string]any{"source_id": sourceID})
	if err != nil {
		t.Fatalf("cdc_status: %v", err)
	}
	if result.Output["state"] != "running" {
		t.Errorf("expected state=running, got %v", result.Output["state"])
	}

	// step.cdc_snapshot
	snapStep, err := p.CreateStep("step.cdc_snapshot", "s-snap", nil)
	if err != nil {
		t.Fatalf("CreateStep cdc_snapshot: %v", err)
	}
	result, err = execStep(ctx, snapStep, map[string]any{
		"source_id": sourceID,
		"tables":    []any{"public.users", "public.orders"},
	})
	if err != nil {
		t.Fatalf("cdc_snapshot: %v", err)
	}
	if result.Output["action"] != "snapshot_triggered" {
		t.Errorf("expected action=snapshot_triggered, got %v", result.Output["action"])
	}

	// Add schema history and verify cdc_schema_history step
	mp, ok := lookupMemoryProvider(t, sourceID)
	if ok {
		_ = mp.AddSchemaVersion(sourceID, cdc.SchemaVersion{
			Table:     "public.users",
			Version:   1,
			DDL:       "ALTER TABLE users ADD COLUMN email TEXT",
			AppliedAt: "2026-03-28T00:00:00Z",
		})
	}

	schemaStep, err := p.CreateStep("step.cdc_schema_history", "s-schema", nil)
	if err != nil {
		t.Fatalf("CreateStep cdc_schema_history: %v", err)
	}
	result, err = execStep(ctx, schemaStep, map[string]any{
		"source_id": sourceID,
		"table":     "public.users",
	})
	if err != nil {
		t.Fatalf("cdc_schema_history: %v", err)
	}
	if result.Output["count"] != 1 {
		t.Errorf("expected count=1, got %v", result.Output["count"])
	}

	// step.cdc_stop
	stopStep, err := p.CreateStep("step.cdc_stop", "s-stop", nil)
	if err != nil {
		t.Fatalf("CreateStep cdc_stop: %v", err)
	}
	result, err = execStep(ctx, stopStep, map[string]any{"source_id": sourceID})
	if err != nil {
		t.Fatalf("cdc_stop: %v", err)
	}
	if result.Output["action"] != "stopped" {
		t.Errorf("expected action=stopped, got %v", result.Output["action"])
	}

	// Module Stop should be idempotent after step stop.
	if err := mod.Stop(ctx); err != nil {
		// The module's provider may already be disconnected; a not-found error is acceptable.
		if !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "not connected") {
			t.Errorf("module Stop: %v", err)
		}
	}
}

// lookupMemoryProvider returns the MemoryProvider registered for sourceID, if any.
func lookupMemoryProvider(t *testing.T, sourceID string) (*cdc.MemoryProvider, bool) {
	t.Helper()
	provider, err := cdc.LookupSource(sourceID)
	if err != nil {
		return nil, false
	}
	mp, ok := provider.(*cdc.MemoryProvider)
	return mp, ok
}

// ─── CDC trigger ─────────────────────────────────────────────────────────────

func TestPlugin_TriggerCDCEvent(t *testing.T) {
	p := newPlugin(t)
	ctx := context.Background()
	sourceID := "integ-trigger-src"
	cdc.UnregisterSource(sourceID)

	// Start a CDC source module so the trigger can find it.
	mod, err := p.CreateModule("cdc.source", "trig-cdc", map[string]any{
		"provider":    "memory",
		"source_id":   sourceID,
		"source_type": "postgres",
		"connection":  "postgres://localhost/testdb",
	})
	if err != nil {
		t.Fatalf("CreateModule: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		mod.Stop(ctx) //nolint:errcheck
		cdc.UnregisterSource(sourceID)
	}()

	var mu sync.Mutex
	var received []map[string]any
	trig, err := p.CreateTrigger("trigger.cdc", map[string]any{
		"source_id": sourceID,
	}, func(_ string, data map[string]any) error {
		mu.Lock()
		received = append(received, data)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	if err := trig.Start(ctx); err != nil {
		t.Fatalf("trigger Start: %v", err)
	}
	defer trig.Stop(ctx) //nolint:errcheck

	mp, ok := lookupMemoryProvider(t, sourceID)
	if !ok {
		t.Fatal("expected MemoryProvider in registry")
	}
	if err := mp.InjectEvent(sourceID, map[string]any{
		"op":    "INSERT",
		"table": "users",
	}); err != nil {
		t.Fatalf("InjectEvent: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Errorf("expected 1 event, got %d", len(received))
	}
}

// ─── Bento provider ───────────────────────────────────────────────────────────

func TestPlugin_CDCWithBentoProvider(t *testing.T) {
	p := newPlugin(t)

	// Verify Init passes with bento config — Start() is NOT called (no real DB).
	mod, err := p.CreateModule("cdc.source", "bento-cdc", map[string]any{
		"provider":    "bento",
		"source_id":   "integ-bento-src",
		"source_type": "postgres",
		"connection":  "postgres://localhost/testdb",
	})
	if err != nil {
		t.Fatalf("CreateModule bento: %v", err)
	}
	if err := mod.Init(); err != nil {
		t.Fatalf("Init bento: %v", err)
	}
	// Do not call Start — no real database available in CI.
}

// ─── Debezium provider ────────────────────────────────────────────────────────

func TestPlugin_CDCWithDebeziumProvider(t *testing.T) {
	sourceID := "integ-debezium-src"
	connectorName := "workflow-" + sourceID
	cdc.UnregisterSource(sourceID)

	// Set up a mock Kafka Connect REST API server.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/connectors":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]any{"name": connectorName}) //nolint:errcheck
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/status"):
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"connector": map[string]any{"state": "RUNNING"},
				"tasks":     []any{},
			})
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, connectorName):
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	p := newPlugin(t)
	ctx := context.Background()

	mod, err := p.CreateModule("cdc.source", "debezium-cdc", map[string]any{
		"provider":    "debezium",
		"source_id":   sourceID,
		"source_type": "postgres",
		"connection":  srv.URL,
	})
	if err != nil {
		t.Fatalf("CreateModule debezium: %v", err)
	}
	if err := mod.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		mod.Stop(ctx) //nolint:errcheck
		cdc.UnregisterSource(sourceID)
	}()

	// Verify the CDC status step sees a running connector.
	statusStep, err := p.CreateStep("step.cdc_status", "d-status", nil)
	if err != nil {
		t.Fatalf("CreateStep: %v", err)
	}
	result, err := execStep(ctx, statusStep, map[string]any{"source_id": sourceID})
	if err != nil {
		t.Fatalf("cdc_status: %v", err)
	}
	if result.Output["state"] != "running" {
		t.Errorf("expected state=running, got %v", result.Output["state"])
	}
}

// ─── Tenancy lifecycle ────────────────────────────────────────────────────────

func TestPlugin_TenancyFullLifecycle(t *testing.T) {
	p := newPlugin(t)
	ctx := context.Background()

	mod, err := p.CreateModule("data.tenancy", "tmod", map[string]any{
		"strategy":      "schema_per_tenant",
		"schema_prefix": "t_",
	})
	if err != nil {
		t.Fatalf("CreateModule tenancy: %v", err)
	}
	if err := mod.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// step.tenant_provision
	provStep, err := p.CreateStep("step.tenant_provision", "prov", map[string]any{
		"strategy":      "schema_per_tenant",
		"schema_prefix": "t_",
	})
	if err != nil {
		t.Fatalf("CreateStep provision: %v", err)
	}
	result, err := execStep(ctx, provStep, map[string]any{"tenant_id": "acme"})
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	if result.Output["status"] != "provisioned" {
		t.Errorf("expected status=provisioned, got %v", result.Output["status"])
	}
	if result.Output["tenant_id"] != "acme" {
		t.Errorf("expected tenant_id=acme, got %v", result.Output["tenant_id"])
	}

	// step.tenant_deprovision
	deprovStep, err := p.CreateStep("step.tenant_deprovision", "deprov", map[string]any{
		"strategy":      "schema_per_tenant",
		"schema_prefix": "t_",
	})
	if err != nil {
		t.Fatalf("CreateStep deprovision: %v", err)
	}
	result, err = execStep(ctx, deprovStep, map[string]any{
		"tenant_id": "acme",
		"mode":      "archive",
	})
	if err != nil {
		t.Fatalf("deprovision: %v", err)
	}
	if result.Output["status"] != "deprovisioned" {
		t.Errorf("expected status=deprovisioned, got %v", result.Output["status"])
	}

	if err := mod.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ─── Tenant migrate parallel ──────────────────────────────────────────────────

func TestPlugin_TenantMigrateParallel(t *testing.T) {
	p := newPlugin(t)
	ctx := context.Background()

	migrateStep, err := p.CreateStep("step.tenant_migrate", "mig", map[string]any{
		"strategy":      "schema_per_tenant",
		"schema_prefix": "t_",
	})
	if err != nil {
		t.Fatalf("CreateStep migrate: %v", err)
	}

	tenants := make([]any, 10)
	for i := range tenants {
		tenants[i] = "tenant" + string(rune('0'+i))
	}

	result, err := execStep(ctx, migrateStep, map[string]any{
		"tenant_ids":  tenants,
		"parallelism": 3,
	})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if result.Output["status"] != "completed" {
		t.Errorf("expected status=completed, got %v", result.Output["status"])
	}
	count, _ := result.Output["count"].(int)
	if count != 10 {
		t.Errorf("expected count=10, got %v", result.Output["count"])
	}
}

// ─── Binary build test ────────────────────────────────────────────────────────

func TestPlugin_BinaryBuilds(t *testing.T) {
	// Locate the module root by walking up from this test file's directory.
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine test file path")
	}
	moduleRoot := filepath.Dir(filepath.Dir(file)) // internal/ -> repo root
	cmd := exec.Command("go", "build", "-o", os.DevNull, "./cmd/workflow-plugin-data-engineering/...")
	cmd.Dir = moduleRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build failed: %v\n%s", err, string(out))
	}
}

// ─── Strict contract coverage tests ──────────────────────────────────────────

// pluginJSON is the shape we read from plugin.json for contract coverage checks.
type pluginJSON struct {
	StepTypes   []string          `json:"stepTypes"`
	StepSchemas []stepSchemaEntry `json:"stepSchemas"`
}

type stepSchemaEntry struct {
	Type string `json:"type"`
}

// moduleContractsJSON is the shape of plugin.contracts.json.
type moduleContractsJSON struct {
	ModuleContracts  map[string]any `json:"moduleContracts"`
	TriggerContracts map[string]any `json:"triggerContracts"`
}

// repoRoot returns the absolute path to the repository root by locating plugin.json.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine test file path")
	}
	return filepath.Dir(filepath.Dir(file))
}

// TestPlugin_StepSchemaCoverage verifies that every step type advertised in
// StepTypes() has a corresponding entry in the stepSchemas field of plugin.json.
// This ensures strict contract descriptors are present for all steps.
func TestPlugin_StepSchemaCoverage(t *testing.T) {
	root := repoRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "plugin.json"))
	if err != nil {
		t.Fatalf("read plugin.json: %v", err)
	}
	var pj pluginJSON
	if err := json.Unmarshal(data, &pj); err != nil {
		t.Fatalf("parse plugin.json: %v", err)
	}

	// Index step schemas by type.
	schemaIndex := make(map[string]bool, len(pj.StepSchemas))
	for _, s := range pj.StepSchemas {
		schemaIndex[s.Type] = true
	}

	// Every step type in the manifest must have a stepSchema.
	missing := []string{}
	for _, st := range pj.StepTypes {
		if !schemaIndex[st] {
			missing = append(missing, st)
		}
	}
	if len(missing) > 0 {
		t.Errorf("plugin.json stepSchemas missing contract descriptors for %d step type(s):\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}

	// Every stepSchema must have a corresponding advertised step type.
	typeIndex := make(map[string]bool, len(pj.StepTypes))
	for _, st := range pj.StepTypes {
		typeIndex[st] = true
	}
	for _, s := range pj.StepSchemas {
		if !typeIndex[s.Type] {
			t.Errorf("stepSchemas contains orphan entry %q not in stepTypes", s.Type)
		}
	}

	t.Logf("strict contract coverage: %d/%d step types have schema descriptors",
		len(schemaIndex), len(pj.StepTypes))
}

// TestPlugin_ModuleContractCoverage verifies that plugin.contracts.json provides
// field contracts for every module type and trigger type advertised by the plugin.
func TestPlugin_ModuleContractCoverage(t *testing.T) {
	root := repoRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "plugin.contracts.json"))
	if err != nil {
		t.Fatalf("read plugin.contracts.json: %v", err)
	}
	var mc moduleContractsJSON
	if err := json.Unmarshal(data, &mc); err != nil {
		t.Fatalf("parse plugin.contracts.json: %v", err)
	}

	p := newPlugin(t)

	// Every advertised module type must have a contract entry.
	missing := []string{}
	for _, mt := range p.ModuleTypes() {
		if _, ok := mc.ModuleContracts[mt]; !ok {
			missing = append(missing, mt)
		}
	}
	if len(missing) > 0 {
		t.Errorf("plugin.contracts.json moduleContracts missing %d module type(s):\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}

	// Every advertised trigger type must have a contract entry.
	missingTrigger := []string{}
	for _, tt := range p.TriggerTypes() {
		if _, ok := mc.TriggerContracts[tt]; !ok {
			missingTrigger = append(missingTrigger, tt)
		}
	}
	if len(missingTrigger) > 0 {
		t.Errorf("plugin.contracts.json triggerContracts missing %d trigger type(s):\n  %s",
			len(missingTrigger), strings.Join(missingTrigger, "\n  "))
	}

	t.Logf("module contracts: %d/%d; trigger contracts: %d/%d",
		len(mc.ModuleContracts), len(p.ModuleTypes()),
		len(mc.TriggerContracts), len(p.TriggerTypes()))
}
