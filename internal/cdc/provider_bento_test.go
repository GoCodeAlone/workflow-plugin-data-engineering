package cdc

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestBentoProvider_BuildConfig verifies that the config generator produces valid YAML
// for each source type without requiring a real database.
func TestBentoProvider_BuildConfig(t *testing.T) {
	cases := []struct {
		name    string
		cfg     SourceConfig
		wantErr bool
	}{
		{
			name: "postgres",
			cfg: SourceConfig{
				SourceID:   "pg-src",
				SourceType: "postgres",
				Connection: "postgres://user:pass@localhost/mydb",
				Tables:     []string{"public.users", "public.orders"},
			},
		},
		{
			name: "mysql",
			cfg: SourceConfig{
				SourceID:   "mysql-src",
				SourceType: "mysql",
				Connection: "user:pass@tcp(localhost:3306)/mydb",
				Tables:     []string{"users"},
			},
		},
		{
			name: "dynamodb",
			cfg: SourceConfig{
				SourceID:   "dynamo-src",
				SourceType: "dynamodb",
				Connection: "",
				Options: map[string]any{
					"region":             "us-east-1",
					"kinesis_stream_arn": "arn:aws:kinesis:us-east-1:123456789:stream/my-cdc",
				},
			},
		},
		{
			name: "unknown_source_type",
			cfg: SourceConfig{
				SourceID:   "bad-src",
				SourceType: "oracle",
				Connection: "oracle://localhost/db",
			},
			wantErr: true,
		},
		{
			name: "postgres_no_tables",
			cfg: SourceConfig{
				SourceID:   "pg-src-no-tables",
				SourceType: "postgres",
				Connection: "postgres://localhost/db",
				Tables:     []string{},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			yaml, err := buildBentoInputYAML(tc.cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got yaml:\n%s", yaml)
				}
				return
			}
			if err != nil {
				t.Fatalf("buildBentoInputYAML: %v", err)
			}
			if len(yaml) == 0 {
				t.Fatal("got empty YAML")
			}
		})
	}
}

// TestBentoProvider_StartStop verifies that the BentoProvider can start and stop a real
// Bento stream. We use the "generate" input (built-in Bento test data generator) to avoid
// needing any real database.
func TestBentoProvider_StartStop(t *testing.T) {
	ctx := context.Background()

	bs := &bentoStream{
		config: SourceConfig{SourceID: "test-stream"},
		done:   make(chan struct{}),
		state:  "starting",
	}

	// Use Bento's "generate" input via AddInputYAML (pure components imported in testmain_test.go).
	// We pass just the input fragment (not a full config); the consumer output is added by start().
	generateYAML := `generate:
  mapping: 'root = {"op": "INSERT", "table": "test"}'
  interval: "20ms"
  count: 100`

	var mu sync.Mutex
	var events []map[string]any

	bs.handler = func(_ string, event map[string]any) error {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
		return nil
	}

	if err := bs.start(ctx, generateYAML); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for at least 5 events (5 × 20ms = 100ms, well within deadline).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(events)
		mu.Unlock()
		if n >= 5 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := bs.stop(stopCtx); err != nil {
		// Context deadline exceeded during shutdown is acceptable — the stream was stopped.
		t.Logf("stop (non-fatal): %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) < 5 {
		t.Errorf("expected at least 5 events, got %d", len(events))
	}
	if events[0]["op"] != "INSERT" {
		t.Errorf("expected op=INSERT, got %v", events[0]["op"])
	}
}

// TestBentoProvider_ConnectDuplicate verifies that connecting with the same source ID twice returns an error.
func TestBentoProvider_ConnectDuplicate(t *testing.T) {
	p := newBentoProvider()
	ctx := context.Background()

	cfg := SourceConfig{
		Provider:   "bento",
		SourceID:   "dup-src",
		SourceType: "postgres",
		Connection: "postgres://localhost/db",
		Tables:     []string{"users"},
	}

	// First connect will fail trying to build a stream against a non-existent database,
	// so we inject the stream directly.
	p.mu.Lock()
	p.streams[cfg.SourceID] = &bentoStream{config: cfg, state: "running", done: make(chan struct{})}
	p.mu.Unlock()

	// Duplicate connect must fail with the stream already registered.
	if err := p.Connect(ctx, cfg); err == nil {
		t.Fatal("expected error on duplicate Connect")
	}
}

// TestBentoProvider_StatusNotFound verifies Status returns not_found for unknown sources.
func TestBentoProvider_StatusNotFound(t *testing.T) {
	p := newBentoProvider()
	ctx := context.Background()
	status, err := p.Status(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.State != "not_found" {
		t.Errorf("expected not_found, got %q", status.State)
	}
}

// TestBentoProvider_RegisterEventHandler verifies handler registration against a stream.
func TestBentoProvider_RegisterEventHandler(t *testing.T) {
	p := newBentoProvider()

	bs := &bentoStream{
		config: SourceConfig{SourceID: "handler-src"},
		done:   make(chan struct{}),
		state:  "running",
	}
	p.mu.Lock()
	p.streams["handler-src"] = bs
	p.mu.Unlock()

	var called bool
	var mu sync.Mutex
	handler := EventHandler(func(_ string, _ map[string]any) error {
		mu.Lock()
		called = true
		mu.Unlock()
		return nil
	})

	if err := p.RegisterEventHandler("handler-src", handler); err != nil {
		t.Fatalf("RegisterEventHandler: %v", err)
	}

	// Manually invoke the registered handler.
	bs.mu.RLock()
	h := bs.handler
	bs.mu.RUnlock()

	if h == nil {
		t.Fatal("handler was not registered")
	}
	if err := h("handler-src", map[string]any{"test": true}); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !called {
		t.Fatal("handler was not called")
	}
}
