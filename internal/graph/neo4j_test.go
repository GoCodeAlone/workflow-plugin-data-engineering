package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// -- Mock driver (implements Neo4jDriver) --

type mockNeo4jDriver struct {
	verifyErr   error
	closeErr    error
	sessionFunc func(ctx context.Context, cfg neo4j.SessionConfig) GraphSession
}

func (m *mockNeo4jDriver) NewSession(ctx context.Context, cfg neo4j.SessionConfig) GraphSession {
	if m.sessionFunc != nil {
		return m.sessionFunc(ctx, cfg)
	}
	return &mockSession{}
}

func (m *mockNeo4jDriver) VerifyConnectivity(_ context.Context) error {
	return m.verifyErr
}

func (m *mockNeo4jDriver) Close(_ context.Context) error {
	return m.closeErr
}

// -- Mock session (implements GraphSession) --

type mockSession struct {
	runFunc func(ctx context.Context, cypher string, params map[string]any) (GraphResult, error)
}

func (s *mockSession) Run(ctx context.Context, cypher string, params map[string]any) (GraphResult, error) {
	if s.runFunc != nil {
		return s.runFunc(ctx, cypher, params)
	}
	return &mockResult{}, nil
}

func (s *mockSession) Close(_ context.Context) error { return nil }

// -- Mock result (implements GraphResult) --

type mockResult struct {
	records []*neo4j.Record
	pos     int
	err     error
}

func (r *mockResult) Next(_ context.Context) bool {
	if r.pos < len(r.records) {
		r.pos++
		return true
	}
	return false
}

func (r *mockResult) Record() *neo4j.Record {
	if r.pos == 0 || r.pos > len(r.records) {
		return nil
	}
	return r.records[r.pos-1]
}

func (r *mockResult) Err() error { return r.err }

// makeRecord builds a neo4j.Record with given keys and values.
func makeRecord(keys []string, values []any) *neo4j.Record {
	return &neo4j.Record{Keys: keys, Values: values}
}

// -- Helpers --

func makeNeo4jModule(t *testing.T, mockDriver *mockNeo4jDriver) *Neo4jModule {
	t.Helper()
	m := &Neo4jModule{
		name: "test_graph",
		config: Neo4jConfig{
			URI:      "bolt://localhost:7687",
			Database: "neo4j",
		},
		newDriver: func(_ context.Context, _ Neo4jConfig) (Neo4jDriver, error) {
			return mockDriver, nil
		},
	}
	return m
}

// -- Tests --

func TestNeo4jModule_Init_Start(t *testing.T) {
	mock := &mockNeo4jDriver{}
	m := makeNeo4jModule(t, mock)
	t.Cleanup(func() { UnregisterNeo4jModule(m.name) })

	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	m.mu.RLock()
	if m.driver == nil {
		t.Error("driver should be set after Start")
	}
	m.mu.RUnlock()
}

func TestNeo4jModule_InvalidConfig(t *testing.T) {
	_, err := NewNeo4jModule("bad", map[string]any{})
	if err == nil {
		t.Fatal("expected error for missing uri")
	}
}

func TestNeo4jModule_Stop(t *testing.T) {
	mock := &mockNeo4jDriver{}
	m := makeNeo4jModule(t, mock)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := m.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	m.mu.RLock()
	if m.driver != nil {
		t.Error("driver should be nil after Stop")
	}
	m.mu.RUnlock()
}

func TestNeo4jModule_ExecuteCypher(t *testing.T) {
	ctx := context.Background()

	record := makeRecord([]string{"name"}, []any{"Alice"})
	mock := &mockNeo4jDriver{
		sessionFunc: func(_ context.Context, _ neo4j.SessionConfig) GraphSession {
			return &mockSession{
				runFunc: func(_ context.Context, _ string, _ map[string]any) (GraphResult, error) {
					return &mockResult{records: []*neo4j.Record{record}}, nil
				},
			}
		},
	}
	m := makeNeo4jModule(t, mock)
	m.mu.Lock()
	m.driver = mock
	m.mu.Unlock()

	rows, err := m.ExecuteCypher(ctx, "MATCH (n:Person) RETURN n.name AS name", nil)
	if err != nil {
		t.Fatalf("ExecuteCypher: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0]["name"] != "Alice" {
		t.Errorf("expected Alice, got %v", rows[0]["name"])
	}
}

func TestNeo4jModule_ExecuteCypher_NotStarted(t *testing.T) {
	m := &Neo4jModule{
		name:   "not_started",
		config: Neo4jConfig{URI: "bolt://x:7687"},
	}
	_, err := m.ExecuteCypher(context.Background(), "RETURN 1", nil)
	if err == nil {
		t.Fatal("expected error when driver is nil")
	}
}

func TestNeo4jModule_Start_ConnectivityError(t *testing.T) {
	mock := &mockNeo4jDriver{
		verifyErr: errors.New("connection refused"),
	}
	m := makeNeo4jModule(t, mock)
	err := m.Start(context.Background())
	if err == nil {
		t.Fatal("expected connectivity error")
	}
}
