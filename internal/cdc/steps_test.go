package cdc

import (
	"context"
	"testing"
)

// setupTestSource creates and registers a MemoryProvider for use in step tests.
// Returns the provider and a cleanup function.
func setupTestSource(t *testing.T, sourceID string) (*MemoryProvider, func()) {
	t.Helper()
	ctx := context.Background()
	p := NewMemoryProvider()
	cfg := SourceConfig{
		Provider:   "memory",
		SourceID:   sourceID,
		SourceType: "postgres",
		Connection: "postgres://localhost/testdb",
	}
	if err := p.Connect(ctx, cfg); err != nil {
		t.Fatalf("setupTestSource: connect: %v", err)
	}
	if err := RegisterSource(sourceID, p); err != nil {
		t.Fatalf("setupTestSource: register: %v", err)
	}
	cleanup := func() {
		UnregisterSource(sourceID)
		_ = p.Disconnect(ctx, sourceID)
	}
	return p, cleanup
}

func TestStatusStep_Execute(t *testing.T) {
	sourceID := "test-status-src"
	_, cleanup := setupTestSource(t, sourceID)
	defer cleanup()

	step, err := NewStatusStep("my-status", nil)
	if err != nil {
		t.Fatalf("NewStatusStep: %v", err)
	}

	ctx := context.Background()
	result, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{"source_id": sourceID})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["state"] != "running" {
		t.Errorf("expected state=running, got %v", result.Output["state"])
	}
	if result.Output["source_id"] != sourceID {
		t.Errorf("expected source_id=%q, got %v", sourceID, result.Output["source_id"])
	}
}

func TestStatusStep_MissingSourceID(t *testing.T) {
	step, _ := NewStatusStep("my-status", nil)
	ctx := context.Background()
	_, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{})
	if err == nil {
		t.Fatal("expected error for missing source_id")
	}
}

func TestStatusStep_UnknownSource(t *testing.T) {
	step, _ := NewStatusStep("my-status", nil)
	ctx := context.Background()
	_, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{"source_id": "no-such-source"})
	if err == nil {
		t.Fatal("expected error for unknown source")
	}
}

func TestStopStep_Execute(t *testing.T) {
	sourceID := "test-stop-src"
	_, cleanup := setupTestSource(t, sourceID)
	// Don't defer cleanup — the stop step removes the source.
	_ = cleanup

	step, err := NewStopStep("my-stop", nil)
	if err != nil {
		t.Fatalf("NewStopStep: %v", err)
	}

	ctx := context.Background()
	result, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{"source_id": sourceID})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["action"] != "stopped" {
		t.Errorf("expected action=stopped, got %v", result.Output["action"])
	}

	// Source should be gone from registry.
	_, lookupErr := LookupSource(sourceID)
	if lookupErr == nil {
		t.Fatal("expected source to be deregistered after stop")
	}
}

func TestStopStep_UnknownSource(t *testing.T) {
	step, _ := NewStopStep("my-stop", nil)
	ctx := context.Background()
	_, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{"source_id": "no-such"})
	if err == nil {
		t.Fatal("expected error for unknown source")
	}
}

func TestSnapshotStep_Execute(t *testing.T) {
	sourceID := "test-snap-src"
	_, cleanup := setupTestSource(t, sourceID)
	defer cleanup()

	step, err := NewSnapshotStep("my-snap", nil)
	if err != nil {
		t.Fatalf("NewSnapshotStep: %v", err)
	}

	tables := []any{"public.users", "public.orders"}
	ctx := context.Background()
	result, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"source_id": sourceID,
		"tables":    tables,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["action"] != "snapshot_triggered" {
		t.Errorf("expected action=snapshot_triggered, got %v", result.Output["action"])
	}
}

func TestSchemaHistoryStep_Execute(t *testing.T) {
	sourceID := "test-schema-src"
	p, cleanup := setupTestSource(t, sourceID)
	defer cleanup()

	// Add a schema version.
	if err := p.AddSchemaVersion(sourceID, SchemaVersion{
		Table:     "public.users",
		Version:   1,
		DDL:       "ALTER TABLE users ADD COLUMN email TEXT",
		AppliedAt: "2026-03-28T00:00:00Z",
	}); err != nil {
		t.Fatalf("AddSchemaVersion: %v", err)
	}

	step, err := NewSchemaHistoryStep("my-schema", nil)
	if err != nil {
		t.Fatalf("NewSchemaHistoryStep: %v", err)
	}

	ctx := context.Background()
	result, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"source_id": sourceID,
		"table":     "public.users",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.Output["count"] != 1 {
		t.Errorf("expected count=1, got %v", result.Output["count"])
	}
	history, _ := result.Output["history"].([]map[string]any)
	if len(history) != 1 {
		t.Fatalf("expected 1 history entry, got %d", len(history))
	}
	if history[0]["ddl"] != "ALTER TABLE users ADD COLUMN email TEXT" {
		t.Errorf("unexpected DDL: %v", history[0]["ddl"])
	}
}

func TestSchemaHistoryStep_MissingTable(t *testing.T) {
	sourceID := "test-schema-missing-table"
	_, cleanup := setupTestSource(t, sourceID)
	defer cleanup()

	step, _ := NewSchemaHistoryStep("my-schema", nil)
	ctx := context.Background()
	_, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{"source_id": sourceID})
	if err == nil {
		t.Fatal("expected error for missing table")
	}
}

func TestStartStep_ExistingSource(t *testing.T) {
	sourceID := "test-start-existing"
	_, cleanup := setupTestSource(t, sourceID)
	defer cleanup()

	step, err := NewStartStep("my-start", nil)
	if err != nil {
		t.Fatalf("NewStartStep: %v", err)
	}

	ctx := context.Background()
	result, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{"source_id": sourceID})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["action"] != "started" {
		t.Errorf("expected action=started, got %v", result.Output["action"])
	}
}

func TestStartStep_MissingSourceID(t *testing.T) {
	step, _ := NewStartStep("my-start", nil)
	ctx := context.Background()
	_, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{})
	if err == nil {
		t.Fatal("expected error for missing source_id")
	}
}
