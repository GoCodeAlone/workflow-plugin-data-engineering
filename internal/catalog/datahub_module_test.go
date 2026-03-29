package catalog

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDataHubModule_Init_Start(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()

	mod, err := NewDataHubModule("dh_test", map[string]any{
		"endpoint": srv.URL,
		"token":    "test-token",
	})
	if err != nil {
		t.Fatalf("NewDataHubModule: %v", err)
	}
	t.Cleanup(func() { UnregisterCatalogModule("dh_test") })

	if err := mod.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := mod.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	dhMod := mod.(*DataHubModule)
	if dhMod.Client() == nil {
		t.Error("expected client to be set after Start")
	}
}

func TestDataHubModule_InvalidConfig(t *testing.T) {
	_, err := NewDataHubModule("bad_dh", map[string]any{})
	if err == nil {
		t.Fatal("expected error for missing endpoint")
	}
}

func TestDataHubModule_Stop(t *testing.T) {
	mod, _ := NewDataHubModule("dh_stop", map[string]any{"endpoint": "http://localhost:8080"})
	if err := mod.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := mod.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	dhMod := mod.(*DataHubModule)
	if dhMod.Client() != nil {
		t.Error("expected client to be nil after Stop")
	}
}
