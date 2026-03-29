package cdc

import (
	"context"
	"testing"
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

// TestBentoProvider_Connect_GeneratesConfig verifies that Connect generates YAML
// and stores the config for delegation.
func TestBentoProvider_Connect_GeneratesConfig(t *testing.T) {
	p := newBentoProvider()
	ctx := context.Background()

	cfg := SourceConfig{
		SourceID:   "pg-test",
		SourceType: "postgres",
		Connection: "postgres://localhost/mydb",
		Tables:     []string{"public.events"},
		Options:    map[string]any{"bento_module": "cdc_bento_stream"},
	}

	if err := p.Connect(ctx, cfg); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	yaml, err := p.ConfigYAML("pg-test")
	if err != nil {
		t.Fatalf("ConfigYAML: %v", err)
	}
	if len(yaml) == 0 {
		t.Error("expected non-empty YAML after Connect")
	}

	// Verify bento_module reference is stored.
	p.mu.RLock()
	entry := p.configs["pg-test"]
	p.mu.RUnlock()
	if entry.bentoModule != "cdc_bento_stream" {
		t.Errorf("bentoModule: got %q, want %q", entry.bentoModule, "cdc_bento_stream")
	}
}

// TestBentoProvider_Status_Configured verifies Status returns "configured" after Connect.
func TestBentoProvider_Status_Configured(t *testing.T) {
	p := newBentoProvider()
	ctx := context.Background()

	cfg := SourceConfig{
		SourceID:   "status-test",
		SourceType: "postgres",
		Connection: "postgres://localhost/db",
	}
	if err := p.Connect(ctx, cfg); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	status, err := p.Status(ctx, "status-test")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.State != "configured" {
		t.Errorf("State: got %q, want %q", status.State, "configured")
	}
	if status.Provider != "bento" {
		t.Errorf("Provider: got %q, want %q", status.Provider, "bento")
	}
}

// TestBentoProvider_Disconnect verifies Disconnect removes the configuration.
func TestBentoProvider_Disconnect(t *testing.T) {
	p := newBentoProvider()
	ctx := context.Background()

	cfg := SourceConfig{
		SourceID:   "disc-test",
		SourceType: "mysql",
		Connection: "user:pass@tcp(localhost:3306)/db",
	}
	if err := p.Connect(ctx, cfg); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := p.Disconnect(ctx, "disc-test"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	// Status should return not_found after disconnect.
	status, err := p.Status(ctx, "disc-test")
	if err != nil {
		t.Fatalf("Status after Disconnect: %v", err)
	}
	if status.State != "not_found" {
		t.Errorf("State after Disconnect: got %q, want %q", status.State, "not_found")
	}

	// Second disconnect should error.
	if err := p.Disconnect(ctx, "disc-test"); err == nil {
		t.Error("expected error on second Disconnect, got nil")
	}
}

// TestBentoProvider_Snapshot_RegeneratesConfig verifies Snapshot regenerates YAML
// with updated tables.
func TestBentoProvider_Snapshot_RegeneratesConfig(t *testing.T) {
	p := newBentoProvider()
	ctx := context.Background()

	cfg := SourceConfig{
		SourceID:   "snap-test",
		SourceType: "postgres",
		Connection: "postgres://localhost/db",
		Tables:     []string{"public.users"},
	}
	if err := p.Connect(ctx, cfg); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	origYAML, _ := p.ConfigYAML("snap-test")

	// Snapshot with new tables should regenerate the YAML.
	if err := p.Snapshot(ctx, "snap-test", []string{"public.orders"}); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	newYAML, _ := p.ConfigYAML("snap-test")
	if newYAML == origYAML {
		t.Error("expected YAML to change after Snapshot with different tables")
	}

	// Status should be "snapshot".
	status, _ := p.Status(ctx, "snap-test")
	if status.State != "snapshot" {
		t.Errorf("State after Snapshot: got %q, want %q", status.State, "snapshot")
	}
}

// TestBentoProvider_ConnectDuplicate verifies that connecting with the same source ID
// twice returns an error.
func TestBentoProvider_ConnectDuplicate(t *testing.T) {
	p := newBentoProvider()
	ctx := context.Background()

	cfg := SourceConfig{
		SourceID:   "dup-src",
		SourceType: "postgres",
		Connection: "postgres://localhost/db",
		Tables:     []string{"users"},
	}

	if err := p.Connect(ctx, cfg); err != nil {
		t.Fatalf("first Connect: %v", err)
	}
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
