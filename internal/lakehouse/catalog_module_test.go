package lakehouse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCatalogModule_Init(t *testing.T) {
	cases := []struct {
		name    string
		config  map[string]any
		wantErr bool
	}{
		{
			name: "valid",
			config: map[string]any{
				"endpoint":   "https://catalog.example.com/v1",
				"credential": "token123",
			},
		},
		{
			name:    "missing endpoint",
			config:  map[string]any{"credential": "token"},
			wantErr: true,
		},
		{
			name:    "empty config",
			config:  map[string]any{},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := NewCatalogModule("test-catalog", tc.config)
			if tc.wantErr {
				if err == nil && m != nil {
					if initErr := m.Init(); initErr == nil {
						t.Fatal("expected Init error")
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("NewCatalogModule: %v", err)
			}
			if initErr := m.Init(); initErr != nil {
				t.Fatalf("Init: %v", initErr)
			}
		})
	}
}

func TestCatalogModule_Start_PingsCatalog(t *testing.T) {
	pinged := false
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/config", func(w http.ResponseWriter, r *http.Request) {
		pinged = true
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]string{},
			"overrides": map[string]string{},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	m, err := NewCatalogModule("ping-catalog", map[string]any{
		"endpoint":   srv.URL + "/v1",
		"credential": "tok",
	})
	if err != nil {
		t.Fatalf("NewCatalogModule: %v", err)
	}
	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Stop(ctx) //nolint:errcheck

	if !pinged {
		t.Error("expected catalog to be pinged on Start")
	}

	// Client() should be accessible.
	cm := m.(*CatalogModule)
	if cm.Client() == nil {
		t.Error("expected non-nil client after Start")
	}

	// Should be registered in the catalog registry.
	_, lookupErr := LookupCatalog("ping-catalog")
	if lookupErr != nil {
		t.Errorf("LookupCatalog after Start: %v", lookupErr)
	}
}

func TestCatalogModule_Start_DeregistersOnStop(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]string{}, "overrides": map[string]string{},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	m, _ := NewCatalogModule("dereg-catalog", map[string]any{
		"endpoint": srv.URL + "/v1",
	})
	ctx := context.Background()
	_ = m.Start(ctx)

	if _, err := LookupCatalog("dereg-catalog"); err != nil {
		t.Fatalf("should be registered after Start: %v", err)
	}

	_ = m.Stop(ctx)
	if _, err := LookupCatalog("dereg-catalog"); err == nil {
		t.Error("expected catalog to be deregistered after Stop")
	}
}

func TestCatalogModule_InvalidConfig(t *testing.T) {
	// Empty endpoint should fail.
	m, err := NewCatalogModule("bad", map[string]any{})
	if err == nil {
		if initErr := m.Init(); initErr == nil {
			t.Fatal("expected error for missing endpoint")
		}
	}
}
