package cdc

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestTrigger_StartStop(t *testing.T) {
	sourceID := "trigger-test-src"
	p, cleanup := setupTestSource(t, sourceID)
	defer cleanup()

	var mu sync.Mutex
	var calls []string

	trigger, err := NewTrigger(map[string]any{
		"source_id": sourceID,
	}, func(action string, data map[string]any) error {
		mu.Lock()
		calls = append(calls, action)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("NewTrigger: %v", err)
	}

	ctx := context.Background()
	if err := trigger.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Inject an event — should reach the callback.
	if err := p.InjectEvent(sourceID, map[string]any{
		"op":    "INSERT",
		"table": "users",
	}); err != nil {
		t.Fatalf("InjectEvent: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(calls)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	if len(calls) != 1 {
		t.Errorf("expected 1 callback, got %d", len(calls))
	}
	if len(calls) > 0 && calls[0] != "cdc.change" {
		t.Errorf("expected action=cdc.change, got %q", calls[0])
	}
	mu.Unlock()

	if err := trigger.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestTrigger_TableFilter(t *testing.T) {
	sourceID := "trigger-filter-src"
	p, cleanup := setupTestSource(t, sourceID)
	defer cleanup()

	var mu sync.Mutex
	var received []map[string]any

	trigger, _ := NewTrigger(map[string]any{
		"source_id": sourceID,
		"tables":    []any{"public.orders"},
	}, func(action string, data map[string]any) error {
		mu.Lock()
		received = append(received, data)
		mu.Unlock()
		return nil
	})

	ctx := context.Background()
	if err := trigger.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer trigger.Stop(ctx) //nolint:errcheck

	// Inject event for users (should be filtered out).
	_ = p.InjectEvent(sourceID, map[string]any{"op": "INSERT", "table": "public.users"})
	// Inject event for orders (should pass through).
	_ = p.InjectEvent(sourceID, map[string]any{"op": "INSERT", "table": "public.orders"})

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

	// Give extra time to ensure the users event doesn't slip through.
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Errorf("expected 1 event (orders only), got %d", len(received))
	}
	if len(received) > 0 {
		if received[0]["table"] != "public.orders" {
			t.Errorf("expected table=public.orders, got %v", received[0]["table"])
		}
	}
}

func TestTrigger_ActionFilter(t *testing.T) {
	sourceID := "trigger-action-src"
	p, cleanup := setupTestSource(t, sourceID)
	defer cleanup()

	var mu sync.Mutex
	var received []map[string]any

	trigger, _ := NewTrigger(map[string]any{
		"source_id": sourceID,
		"actions":   []any{"DELETE"},
	}, func(_ string, data map[string]any) error {
		mu.Lock()
		received = append(received, data)
		mu.Unlock()
		return nil
	})

	ctx := context.Background()
	if err := trigger.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer trigger.Stop(ctx) //nolint:errcheck

	_ = p.InjectEvent(sourceID, map[string]any{"op": "INSERT", "table": "users"}) // filtered
	_ = p.InjectEvent(sourceID, map[string]any{"op": "UPDATE", "table": "users"}) // filtered
	_ = p.InjectEvent(sourceID, map[string]any{"op": "DELETE", "table": "users"}) // passes

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

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Errorf("expected 1 DELETE event, got %d", len(received))
	}
}

func TestTrigger_MissingSourceID(t *testing.T) {
	_, err := NewTrigger(map[string]any{}, func(_ string, _ map[string]any) error { return nil })
	if err == nil {
		t.Fatal("expected error for missing source_id")
	}
}

func TestTrigger_SourceNotRunning(t *testing.T) {
	trigger, _ := NewTrigger(map[string]any{
		"source_id": "not-registered-src",
	}, func(_ string, _ map[string]any) error { return nil })

	ctx := context.Background()
	err := trigger.Start(ctx)
	if err == nil {
		t.Fatal("expected error when source module is not running")
	}
}

func TestTrigger_DoubleStart(t *testing.T) {
	sourceID := "trigger-double-src"
	_, cleanup := setupTestSource(t, sourceID)
	defer cleanup()

	trigger, _ := NewTrigger(map[string]any{
		"source_id": sourceID,
	}, func(_ string, _ map[string]any) error { return nil })

	ctx := context.Background()
	if err := trigger.Start(ctx); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := trigger.Start(ctx); err == nil {
		t.Fatal("expected error on double Start")
	}
	trigger.Stop(ctx) //nolint:errcheck
}

func TestTrigger_StopIdempotent(t *testing.T) {
	sourceID := "trigger-stop-idem-src"
	_, cleanup := setupTestSource(t, sourceID)
	defer cleanup()

	trigger, _ := NewTrigger(map[string]any{
		"source_id": sourceID,
	}, func(_ string, _ map[string]any) error { return nil })

	ctx := context.Background()
	trigger.Start(ctx) //nolint:errcheck

	// Stop twice should not panic or error.
	if err := trigger.Stop(ctx); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if err := trigger.Stop(ctx); err != nil {
		t.Fatalf("second Stop (idempotent): %v", err)
	}
}

func TestRegistry_RegisterLookupUnregister(t *testing.T) {
	p := NewMemoryProvider()
	sourceID := "reg-test-src"

	if err := RegisterSource(sourceID, p); err != nil {
		t.Fatalf("RegisterSource: %v", err)
	}

	found, err := LookupSource(sourceID)
	if err != nil {
		t.Fatalf("LookupSource: %v", err)
	}
	if found != p {
		t.Error("LookupSource returned wrong provider")
	}

	// Duplicate registration must fail.
	if err := RegisterSource(sourceID, p); err == nil {
		t.Fatal("expected error on duplicate RegisterSource")
	}

	UnregisterSource(sourceID)

	_, err = LookupSource(sourceID)
	if err == nil {
		t.Fatal("expected error after UnregisterSource")
	}
}
