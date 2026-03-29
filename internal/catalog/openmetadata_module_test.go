package catalog

import (
	"context"
	"testing"
)

func TestOpenMetadataModule_Init_Start(t *testing.T) {
	mod, err := NewOpenMetadataModule("om_test", map[string]any{
		"endpoint": "http://localhost:8585",
		"token":    "om-token",
	})
	if err != nil {
		t.Fatalf("NewOpenMetadataModule: %v", err)
	}
	t.Cleanup(func() { UnregisterCatalogModule("om_test") })

	if err := mod.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := mod.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	omMod := mod.(*OpenMetadataModule)
	if omMod.Client() == nil {
		t.Error("expected client to be set after Start")
	}
}

func TestOpenMetadataModule_InvalidConfig(t *testing.T) {
	_, err := NewOpenMetadataModule("bad_om", map[string]any{})
	if err == nil {
		t.Fatal("expected error for missing endpoint")
	}
}

func TestOpenMetadataModule_Stop(t *testing.T) {
	mod, _ := NewOpenMetadataModule("om_stop", map[string]any{"endpoint": "http://localhost:8585"})
	if err := mod.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := mod.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	omMod := mod.(*OpenMetadataModule)
	if omMod.Client() != nil {
		t.Error("expected client to be nil after Stop")
	}
}
