package cdc

import (
	"context"
	"testing"
)

func TestSourceModule_InitValidation(t *testing.T) {
	cases := []struct {
		name    string
		config  map[string]any
		wantErr bool
	}{
		{
			name: "valid",
			config: map[string]any{
				"provider":    "memory",
				"source_id":   "src1",
				"source_type": "postgres",
				"connection":  "postgres://localhost/db",
			},
		},
		{
			name: "missing_source_id",
			config: map[string]any{
				"provider":   "memory",
				"connection": "postgres://localhost/db",
			},
			wantErr: true,
		},
		{
			name: "missing_connection",
			config: map[string]any{
				"provider":  "memory",
				"source_id": "src1",
			},
			wantErr: true,
		},
		{
			name: "unknown_provider",
			config: map[string]any{
				"provider":   "unknown",
				"source_id":  "src1",
				"connection": "x",
			},
			wantErr: true,
		},
		{
			name:    "missing_provider",
			config:  map[string]any{"source_id": "src1", "connection": "x"},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := NewSourceModule(tc.name, tc.config)
			if tc.wantErr {
				if err == nil && m != nil {
					// NewSourceModule may succeed but Init may fail.
					if initErr := m.Init(); initErr == nil {
						t.Fatal("expected error")
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if initErr := m.Init(); initErr != nil {
				t.Fatalf("Init: %v", initErr)
			}
		})
	}
}

func TestSourceModule_StartStop_Memory(t *testing.T) {
	ctx := context.Background()
	sourceID := "module-start-stop-src"

	// Ensure the source isn't already registered from a previous test.
	UnregisterSource(sourceID)

	m, err := NewSourceModule("test-module", map[string]any{
		"provider":    "memory",
		"source_id":   sourceID,
		"source_type": "postgres",
		"connection":  "postgres://localhost/testdb",
	})
	if err != nil {
		t.Fatalf("NewSourceModule: %v", err)
	}

	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Source should be registered.
	_, err = LookupSource(sourceID)
	if err != nil {
		t.Fatalf("LookupSource after Start: %v", err)
	}

	if err := m.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Source should be deregistered.
	_, err = LookupSource(sourceID)
	if err == nil {
		t.Fatal("expected source to be deregistered after Stop")
	}
}

func TestSourceModule_DuplicateStart(t *testing.T) {
	ctx := context.Background()
	sourceID := "module-dup-src"
	UnregisterSource(sourceID)

	m, _ := NewSourceModule("dup", map[string]any{
		"provider":    "memory",
		"source_id":   sourceID,
		"source_type": "postgres",
		"connection":  "postgres://localhost/db",
	})

	if err := m.Start(ctx); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	defer m.Stop(ctx) //nolint:errcheck

	// Second start should fail (provider already connected, registry conflict).
	if err := m.Start(ctx); err == nil {
		t.Fatal("expected error on duplicate Start")
	}
}
