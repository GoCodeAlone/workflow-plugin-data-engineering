// Package testhelpers provides test utilities for workflow-plugin-data-engineering.
// It re-exports the registry helpers, mock constructors, and types from the
// internal packages so that external test suites (e.g. workflow-scenarios/e2e)
// can perform full lifecycle tests without importing internal packages directly.
package testhelpers

import (
	"context"

	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/catalog"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/cdc"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/graph"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/lakehouse"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/migrate"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/quality"
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/timeseries"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// ─── Plugin factory ───────────────────────────────────────────────────────────

// NewPlugin returns a new data-engineering plugin instance, cast to the full
// provider interface used in tests.
func NewPlugin(version string) sdk.PluginProvider {
	return internal.NewDataEngineeringPlugin(version)
}

// ─── CDC helpers ──────────────────────────────────────────────────────────────

// CDCSchemaVersion re-exports cdc.SchemaVersion.
type CDCSchemaVersion = cdc.SchemaVersion

// CDCUnregisterSource removes a CDC source from the global registry.
func CDCUnregisterSource(sourceID string) { cdc.UnregisterSource(sourceID) }

// CDCLookupMemoryProvider returns the MemoryProvider for sourceID, if registered.
func CDCLookupMemoryProvider(sourceID string) (*cdc.MemoryProvider, bool) {
	p, err := cdc.LookupSource(sourceID)
	if err != nil {
		return nil, false
	}
	mp, ok := p.(*cdc.MemoryProvider)
	return mp, ok
}

// ─── Lakehouse / Iceberg helpers ──────────────────────────────────────────────

// LakehouseUnregisterCatalog removes a catalog from the lakehouse registry.
func LakehouseUnregisterCatalog(name string) { lakehouse.UnregisterCatalog(name) }

// ─── Time-series helpers ──────────────────────────────────────────────────────

// TSUnregister removes a time-series module from the registry.
func TSUnregister(name string) { timeseries.Unregister(name) }

// ─── Graph / Neo4j helpers ────────────────────────────────────────────────────

// Neo4jDriver is the narrow interface over neo4j.DriverWithContext.
type Neo4jDriver = graph.Neo4jDriver

// GraphSession is the narrow interface over neo4j.SessionWithContext.
type GraphSession = graph.GraphSession

// GraphResult is the narrow interface over neo4j.ResultWithContext.
type GraphResult = graph.GraphResult

// Neo4jRecord re-exports neo4j.Record for use in mock implementations.
type Neo4jRecord = neo4j.Record

// Neo4jSessionConfig re-exports neo4j.SessionConfig.
type Neo4jSessionConfig = neo4j.SessionConfig

// NewNeo4jModuleForTest creates a Neo4jModule wired with a mock driver.
func NewNeo4jModuleForTest(name string, driver Neo4jDriver) *graph.Neo4jModule {
	return graph.NewNeo4jModuleForTest(name, driver)
}

// RegisterNeo4jModule registers a pre-wired Neo4j module.
func RegisterNeo4jModule(name string, m *graph.Neo4jModule) error {
	return graph.RegisterNeo4jModule(name, m)
}

// UnregisterNeo4jModule removes a Neo4j module from the registry.
func UnregisterNeo4jModule(name string) { graph.UnregisterNeo4jModule(name) }

// ─── Catalog helpers ──────────────────────────────────────────────────────────

// CatalogUnregisterModule removes a catalog module (DataHub/OpenMetadata) from the registry.
func CatalogUnregisterModule(name string) { catalog.UnregisterCatalogModule(name) }

// ─── Quality helpers ──────────────────────────────────────────────────────────

// QualityUnregisterChecksModule removes a quality.checks module from the registry.
func QualityUnregisterChecksModule(name string) { quality.UnregisterChecksModule(name) }

// ─── Migrate helpers ──────────────────────────────────────────────────────────

// MigrateUnregisterModule removes a migrate.schema module from the registry.
func MigrateUnregisterModule(name string) { migrate.UnregisterModule(name) }

// ─── Compile-time interface checks ───────────────────────────────────────────
// These ensure external callers can implement the narrow driver interfaces.

var (
	_ Neo4jDriver = (*noopNeo4jDriver)(nil)
	_ GraphSession = (*noopGraphSession)(nil)
	_ GraphResult  = (*noopGraphResult)(nil)
)

// noopNeo4jDriver is an unexported stub that satisfies Neo4jDriver for compile-time checks.
type noopNeo4jDriver struct{}

func (d *noopNeo4jDriver) NewSession(_ context.Context, _ Neo4jSessionConfig) GraphSession {
	return &noopGraphSession{}
}
func (d *noopNeo4jDriver) VerifyConnectivity(_ context.Context) error { return nil }
func (d *noopNeo4jDriver) Close(_ context.Context) error              { return nil }

type noopGraphSession struct{}

func (s *noopGraphSession) Run(_ context.Context, _ string, _ map[string]any) (GraphResult, error) {
	return &noopGraphResult{}, nil
}
func (s *noopGraphSession) Close(_ context.Context) error { return nil }

type noopGraphResult struct{}

func (r *noopGraphResult) Next(_ context.Context) bool    { return false }
func (r *noopGraphResult) Record() *neo4j.Record          { return nil }
func (r *noopGraphResult) Err() error                     { return nil }
