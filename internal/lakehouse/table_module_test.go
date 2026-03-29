package lakehouse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func setupCatalogForTableTest(t *testing.T, mux *http.ServeMux) string {
	t.Helper()
	// Ensure config endpoint is always handled.
	mux.HandleFunc("GET /v1/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]string{}, "overrides": map[string]string{},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	catName := "tbl-cat-" + t.Name()
	client, err := NewIcebergCatalogClient(IcebergClientConfig{Endpoint: srv.URL + "/v1"})
	if err != nil {
		t.Fatalf("NewIcebergCatalogClient: %v", err)
	}
	if err := RegisterCatalog(catName, client); err != nil {
		t.Fatalf("RegisterCatalog: %v", err)
	}
	t.Cleanup(func() { UnregisterCatalog(catName) })
	return catName
}

func TestTableModule_CreatesOnStart(t *testing.T) {
	created := false
	mux := http.NewServeMux()
	// TableExists → 404
	mux.HandleFunc("HEAD /v1/namespaces/{namespace}/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	})
	// CreateTable → 200
	mux.HandleFunc("POST /v1/namespaces/{namespace}/tables", func(w http.ResponseWriter, r *http.Request) {
		created = true
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"metadata": map[string]any{
				"format-version":    2,
				"table-uuid":        "new-table-uuid",
				"location":          "s3://bucket/analytics/users",
				"current-schema-id": 0,
				"schemas":           []any{},
				"properties":        map[string]string{},
			},
		})
	})

	catName := setupCatalogForTableTest(t, mux)

	m, err := NewTableModule("users-table", map[string]any{
		"catalog":   catName,
		"namespace": []any{"analytics"},
		"table":     "users",
		"schema": map[string]any{
			"fields": []any{
				map[string]any{"name": "id", "type": "long", "required": true},
				map[string]any{"name": "email", "type": "string", "required": true},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewTableModule: %v", err)
	}

	ctx := context.Background()
	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Stop(ctx) //nolint:errcheck

	if !created {
		t.Error("expected table to be created on Start when it doesn't exist")
	}
}

func TestTableModule_VerifiesSchema(t *testing.T) {
	loaded := false
	mux := http.NewServeMux()
	// TableExists → 200 (already exists)
	mux.HandleFunc("HEAD /v1/namespaces/{namespace}/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	// LoadTable → existing metadata
	mux.HandleFunc("GET /v1/namespaces/{namespace}/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		loaded = true
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"metadata": map[string]any{
				"format-version":    2,
				"table-uuid":        "existing-uuid",
				"location":          "s3://bucket",
				"current-schema-id": 0,
				"schemas": []any{
					map[string]any{
						"schema-id": 0,
						"type":      "struct",
						"fields": []any{
							map[string]any{"id": 1, "name": "id", "type": "long", "required": true},
						},
					},
				},
				"properties": map[string]string{},
			},
		})
	})

	catName := setupCatalogForTableTest(t, mux)

	m, err := NewTableModule("events-table", map[string]any{
		"catalog":   catName,
		"namespace": []any{"analytics"},
		"table":     "events",
		"schema": map[string]any{
			"fields": []any{
				map[string]any{"name": "id", "type": "long", "required": true},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewTableModule: %v", err)
	}

	ctx := context.Background()
	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Stop(ctx) //nolint:errcheck

	if !loaded {
		t.Error("expected LoadTable to be called when table exists")
	}
}

func TestTableModule_MissingCatalog(t *testing.T) {
	// Reference a catalog that hasn't been registered.
	m, err := NewTableModule("bad-table", map[string]any{
		"catalog":   "nonexistent-catalog",
		"namespace": []any{"analytics"},
		"table":     "users",
	})
	if err != nil {
		t.Fatalf("NewTableModule: %v", err)
	}

	ctx := context.Background()
	if err := m.Init(); err != nil {
		t.Fatal("Init should succeed (lazy catalog lookup)")
	}
	// Start should fail because catalog is not registered.
	if err := m.Start(ctx); err == nil {
		t.Fatal("expected Start to fail for unregistered catalog")
	}
}
