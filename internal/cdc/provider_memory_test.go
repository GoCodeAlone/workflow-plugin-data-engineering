package cdc

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMemoryProvider_ConnectDisconnect(t *testing.T) {
	p := NewMemoryProvider()
	ctx := context.Background()
	cfg := SourceConfig{
		Provider:   "memory",
		SourceID:   "test-source",
		SourceType: "postgres",
		Connection: "postgres://localhost/testdb",
	}

	if err := p.Connect(ctx, cfg); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Duplicate connect must fail.
	if err := p.Connect(ctx, cfg); err == nil {
		t.Fatal("expected error on duplicate Connect")
	}

	status, err := p.Status(ctx, cfg.SourceID)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.State != "running" {
		t.Errorf("expected state=running, got %q", status.State)
	}
	if status.Provider != "memory" {
		t.Errorf("expected provider=memory, got %q", status.Provider)
	}

	if err := p.Disconnect(ctx, cfg.SourceID); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	// Status after disconnect returns not_found.
	status, err = p.Status(ctx, cfg.SourceID)
	if err != nil {
		t.Fatalf("Status after disconnect: %v", err)
	}
	if status.State != "not_found" {
		t.Errorf("expected state=not_found after disconnect, got %q", status.State)
	}
}

func TestMemoryProvider_DisconnectNotFound(t *testing.T) {
	p := NewMemoryProvider()
	ctx := context.Background()
	if err := p.Disconnect(ctx, "nonexistent"); err == nil {
		t.Fatal("expected error when disconnecting unknown source")
	}
}

func TestMemoryProvider_InjectEvent(t *testing.T) {
	p := NewMemoryProvider()
	ctx := context.Background()
	cfg := SourceConfig{
		Provider:   "memory",
		SourceID:   "src1",
		SourceType: "postgres",
		Connection: "postgres://localhost/testdb",
	}

	if err := p.Connect(ctx, cfg); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer p.Disconnect(ctx, cfg.SourceID) //nolint:errcheck

	var mu sync.Mutex
	var received []map[string]any

	if err := p.RegisterEventHandler(cfg.SourceID, func(sourceID string, event map[string]any) error {
		if sourceID != cfg.SourceID {
			t.Errorf("unexpected sourceID %q", sourceID)
		}
		mu.Lock()
		received = append(received, event)
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("RegisterEventHandler: %v", err)
	}

	// Inject two events.
	events := []map[string]any{
		{"op": "INSERT", "table": "users", "after": map[string]any{"id": 1}},
		{"op": "UPDATE", "table": "users", "after": map[string]any{"id": 1, "name": "alice"}},
	}
	for _, e := range events {
		if err := p.InjectEvent(cfg.SourceID, e); err != nil {
			t.Fatalf("InjectEvent: %v", err)
		}
	}

	// Wait for events to be dispatched.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 2 {
		t.Fatalf("expected 2 events, got %d", len(received))
	}
	if received[0]["op"] != "INSERT" {
		t.Errorf("expected first event op=INSERT, got %v", received[0]["op"])
	}
}

func TestMemoryProvider_Snapshot(t *testing.T) {
	p := NewMemoryProvider()
	ctx := context.Background()
	cfg := SourceConfig{
		Provider:   "memory",
		SourceID:   "src2",
		SourceType: "postgres",
		Connection: "postgres://localhost/testdb",
	}

	if err := p.Connect(ctx, cfg); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer p.Disconnect(ctx, cfg.SourceID) //nolint:errcheck

	var received []map[string]any
	var mu sync.Mutex

	if err := p.RegisterEventHandler(cfg.SourceID, func(_ string, event map[string]any) error {
		mu.Lock()
		received = append(received, event)
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("RegisterEventHandler: %v", err)
	}

	tables := []string{"public.users", "public.orders"}
	if err := p.Snapshot(ctx, cfg.SourceID, tables); err != nil {
		t.Fatalf("Snapshot: %v", err)
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
	if len(received) == 0 {
		t.Fatal("expected snapshot event, got none")
	}
	if received[0]["type"] != "snapshot_started" {
		t.Errorf("expected snapshot_started event, got type=%v", received[0]["type"])
	}
}

func TestMemoryProvider_SchemaHistory(t *testing.T) {
	p := NewMemoryProvider()
	ctx := context.Background()
	cfg := SourceConfig{
		Provider:   "memory",
		SourceID:   "src3",
		SourceType: "postgres",
		Connection: "postgres://localhost/testdb",
	}

	if err := p.Connect(ctx, cfg); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer p.Disconnect(ctx, cfg.SourceID) //nolint:errcheck

	sv := SchemaVersion{
		Table:     "public.users",
		Version:   1,
		DDL:       "ALTER TABLE users ADD COLUMN email TEXT",
		AppliedAt: "2026-03-28T00:00:00Z",
	}
	if err := p.AddSchemaVersion(cfg.SourceID, sv); err != nil {
		t.Fatalf("AddSchemaVersion: %v", err)
	}

	// Add a version for a different table.
	if err := p.AddSchemaVersion(cfg.SourceID, SchemaVersion{
		Table: "public.orders", Version: 1, DDL: "CREATE TABLE orders (...)",
	}); err != nil {
		t.Fatalf("AddSchemaVersion (orders): %v", err)
	}

	history, err := p.SchemaHistory(ctx, cfg.SourceID, "public.users")
	if err != nil {
		t.Fatalf("SchemaHistory: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("expected 1 schema version, got %d", len(history))
	}
	if history[0].DDL != sv.DDL {
		t.Errorf("expected DDL=%q, got %q", sv.DDL, history[0].DDL)
	}
}

func TestMemoryProvider_StatusNotFound(t *testing.T) {
	p := NewMemoryProvider()
	ctx := context.Background()
	status, err := p.Status(ctx, "unknown")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.State != "not_found" {
		t.Errorf("expected not_found, got %q", status.State)
	}
}

func TestMemoryProvider_ConcurrentInject(t *testing.T) {
	p := NewMemoryProvider()
	ctx := context.Background()
	cfg := SourceConfig{
		Provider: "memory",
		SourceID: "concurrent-src",
	}
	if err := p.Connect(ctx, cfg); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer p.Disconnect(ctx, cfg.SourceID) //nolint:errcheck

	var count int64
	var mu sync.Mutex

	_ = p.RegisterEventHandler(cfg.SourceID, func(_ string, _ map[string]any) error {
		mu.Lock()
		count++
		mu.Unlock()
		return nil
	})

	var wg sync.WaitGroup
	const n = 50
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = p.InjectEvent(cfg.SourceID, map[string]any{"i": i})
		}(i)
	}
	wg.Wait()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		c := count
		mu.Unlock()
		if c >= n {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if count != n {
		t.Errorf("expected %d events, got %d", n, count)
	}
}
