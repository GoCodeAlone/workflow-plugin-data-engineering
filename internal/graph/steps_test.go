package graph

import (
	"context"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// captureSession captures the cypher + params of the last Run call.
type captureSession struct {
	lastCypher string
	lastParams map[string]any
	result     GraphResult
	runErr     error
}

func (s *captureSession) Run(_ context.Context, cypher string, params map[string]any) (GraphResult, error) {
	s.lastCypher = cypher
	s.lastParams = params
	if s.runErr != nil {
		return nil, s.runErr
	}
	if s.result != nil {
		return s.result, nil
	}
	return &mockResult{}, nil
}

func (s *captureSession) Close(_ context.Context) error { return nil }

// buildCaptureDriver returns a Neo4jDriver whose sessions use the given captureSession.
func buildCaptureDriver(sess *captureSession) *mockNeo4jDriver {
	return &mockNeo4jDriver{
		sessionFunc: func(_ context.Context, _ neo4j.SessionConfig) GraphSession {
			return sess
		},
	}
}

// registerModule registers a module with the given mock driver and returns cleanup func.
func registerModule(t *testing.T, name string, driver *mockNeo4jDriver) *Neo4jModule {
	t.Helper()
	m := &Neo4jModule{
		name:   name,
		config: Neo4jConfig{URI: "bolt://x:7687", Database: "neo4j"},
		driver: driver,
	}
	if err := RegisterNeo4jModule(name, m); err != nil {
		t.Fatalf("RegisterNeo4jModule: %v", err)
	}
	t.Cleanup(func() { UnregisterNeo4jModule(name) })
	return m
}

// -- Tests: step.graph_query --

func TestGraphQuery(t *testing.T) {
	ctx := context.Background()
	record := makeRecord([]string{"n"}, []any{"foo"})
	sess := &captureSession{result: &mockResult{records: []*neo4j.Record{record}}}
	registerModule(t, "qmod", buildCaptureDriver(sess))

	step, _ := NewGraphQueryStep("q1", nil)
	result, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"module": "qmod",
		"cypher": "MATCH (n) RETURN n",
		"params": map[string]any{"limit": 10},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["count"] != 1 {
		t.Errorf("expected count=1, got %v", result.Output["count"])
	}
	if sess.lastCypher != "MATCH (n) RETURN n" {
		t.Errorf("unexpected cypher: %q", sess.lastCypher)
	}
	if sess.lastParams["limit"] != 10 {
		t.Errorf("expected limit=10 in params")
	}
}

func TestGraphQuery_MissingModule(t *testing.T) {
	step, _ := NewGraphQueryStep("q_bad", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"cypher": "MATCH (n) RETURN n",
	})
	if err == nil {
		t.Fatal("expected error for missing module")
	}
}

func TestGraphQuery_MissingCypher(t *testing.T) {
	sess := &captureSession{}
	registerModule(t, "qmod2", buildCaptureDriver(sess))
	step, _ := NewGraphQueryStep("q_nocypher", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "qmod2",
	})
	if err == nil {
		t.Fatal("expected error for missing cypher")
	}
}

// -- Tests: step.graph_write --

func TestGraphWrite_Nodes(t *testing.T) {
	ctx := context.Background()
	sess := &captureSession{}
	registerModule(t, "wmod", buildCaptureDriver(sess))

	step, _ := NewGraphWriteStep("w1", nil)
	result, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"module": "wmod",
		"nodes": []any{
			map[string]any{
				"label":      "Person",
				"properties": map[string]any{"name": "Alice"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	_ = result

	// Verify MERGE Cypher was generated
	if !strings.Contains(sess.lastCypher, "MERGE") {
		t.Errorf("expected MERGE in cypher, got: %q", sess.lastCypher)
	}
	if !strings.Contains(sess.lastCypher, "Person") {
		t.Errorf("expected Person label in cypher, got: %q", sess.lastCypher)
	}
}

func TestGraphWrite_Relationships(t *testing.T) {
	ctx := context.Background()
	sess := &captureSession{}
	registerModule(t, "rmod", buildCaptureDriver(sess))

	step, _ := NewGraphWriteStep("w2", nil)
	_, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"module": "rmod",
		"relationships": []any{
			map[string]any{
				"from":       "Person",
				"to":         "Company",
				"type":       "WORKS_AT",
				"properties": map[string]any{"since": "2020"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if !strings.Contains(sess.lastCypher, "WORKS_AT") {
		t.Errorf("expected WORKS_AT in cypher, got: %q", sess.lastCypher)
	}
	if !strings.Contains(sess.lastCypher, "MERGE") {
		t.Errorf("expected MERGE in cypher, got: %q", sess.lastCypher)
	}
}

func TestGraphWrite_MissingModule(t *testing.T) {
	step, _ := NewGraphWriteStep("w_bad", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"nodes": []any{},
	})
	if err == nil {
		t.Fatal("expected error for missing module")
	}
}

// -- Tests: step.graph_import --

func TestGraphImport_Batch(t *testing.T) {
	ctx := context.Background()
	sess := &captureSession{}
	registerModule(t, "imod", buildCaptureDriver(sess))

	step, _ := NewGraphImportStep("i1", nil)
	result, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"module": "imod",
		"source": []any{
			map[string]any{"user_name": "Alice", "user_age": 30},
			map[string]any{"user_name": "Bob", "user_age": 25},
		},
		"mapping": map[string]any{
			"nodeLabel": "Person",
			"properties": map[string]any{
				"name": "user_name",
				"age":  "user_age",
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["nodeLabel"] != "Person" {
		t.Errorf("expected nodeLabel=Person, got %v", result.Output["nodeLabel"])
	}

	// Verify UNWIND pattern
	if !strings.Contains(sess.lastCypher, "UNWIND") {
		t.Errorf("expected UNWIND in cypher, got: %q", sess.lastCypher)
	}
	if !strings.Contains(sess.lastCypher, "MERGE") {
		t.Errorf("expected MERGE in cypher, got: %q", sess.lastCypher)
	}
	if !strings.Contains(sess.lastCypher, "Person") {
		t.Errorf("expected Person label in cypher, got: %q", sess.lastCypher)
	}

	// Verify rows param is set
	rows, ok := sess.lastParams["rows"].([]any)
	if !ok || len(rows) != 2 {
		t.Errorf("expected 2 rows in params, got %v", sess.lastParams["rows"])
	}
}

func TestGraphImport_EmptySource(t *testing.T) {
	sess := &captureSession{}
	registerModule(t, "imod2", buildCaptureDriver(sess))

	step, _ := NewGraphImportStep("i2", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "imod2",
		"source": []any{},
		"mapping": map[string]any{
			"nodeLabel": "Person",
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["imported"] != 0 {
		t.Errorf("expected imported=0, got %v", result.Output["imported"])
	}
}
