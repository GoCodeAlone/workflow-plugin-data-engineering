package lakehouse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// helpers

func newTestClient(t *testing.T, mux *http.ServeMux) (IcebergCatalogClient, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	client, err := NewIcebergCatalogClient(IcebergClientConfig{
		Endpoint: srv.URL + "/v1",
		Token:    "test-token",
	})
	if err != nil {
		t.Fatalf("NewIcebergCatalogClient: %v", err)
	}
	return client, srv
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func TestIcebergClient_GetConfig(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/config", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{
			"defaults":  map[string]string{"warehouse": "s3://bucket"},
			"overrides": map[string]string{},
		})
	})

	client, _ := newTestClient(t, mux)
	cfg, err := client.GetConfig(context.Background())
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if cfg.Defaults["warehouse"] != "s3://bucket" {
		t.Errorf("unexpected defaults: %v", cfg.Defaults)
	}
}

func TestIcebergClient_ListNamespaces(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{
			"namespaces": [][]string{{"analytics"}, {"analytics", "raw"}},
		})
	})

	client, _ := newTestClient(t, mux)
	ns, err := client.ListNamespaces(context.Background(), "")
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	if len(ns) != 2 {
		t.Fatalf("want 2 namespaces, got %d", len(ns))
	}
	if ns[0][0] != "analytics" {
		t.Errorf("unexpected namespace: %v", ns[0])
	}
}

func TestIcebergClient_CreateNamespace(t *testing.T) {
	var got map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		writeJSON(w, 200, map[string]any{
			"namespace":  []string{"analytics"},
			"properties": map[string]string{"owner": "alice"},
		})
	})

	client, _ := newTestClient(t, mux)
	err := client.CreateNamespace(context.Background(), Namespace{"analytics"}, map[string]string{"owner": "alice"})
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	props, _ := got["properties"].(map[string]any)
	if props["owner"] != "alice" {
		t.Errorf("unexpected body: %v", got)
	}
}

func TestIcebergClient_LoadNamespace(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/namespaces/{namespace}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{
			"namespace":  []string{"analytics"},
			"properties": map[string]string{"location": "s3://bucket/analytics"},
		})
	})

	client, _ := newTestClient(t, mux)
	info, err := client.LoadNamespace(context.Background(), Namespace{"analytics"})
	if err != nil {
		t.Fatalf("LoadNamespace: %v", err)
	}
	if info.Properties["location"] != "s3://bucket/analytics" {
		t.Errorf("unexpected properties: %v", info.Properties)
	}
}

func TestIcebergClient_DropNamespace(t *testing.T) {
	dropped := false
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /v1/namespaces/{namespace}", func(w http.ResponseWriter, r *http.Request) {
		dropped = true
		w.WriteHeader(204)
	})

	client, _ := newTestClient(t, mux)
	err := client.DropNamespace(context.Background(), Namespace{"analytics"})
	if err != nil {
		t.Fatalf("DropNamespace: %v", err)
	}
	if !dropped {
		t.Error("expected DELETE to be called")
	}
}

func TestIcebergClient_ListTables(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/namespaces/{namespace}/tables", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{
			"identifiers": []map[string]any{
				{"namespace": []string{"analytics"}, "name": "users"},
				{"namespace": []string{"analytics"}, "name": "events"},
			},
		})
	})

	client, _ := newTestClient(t, mux)
	tables, err := client.ListTables(context.Background(), Namespace{"analytics"})
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("want 2 tables, got %d", len(tables))
	}
	if tables[0].Name != "users" {
		t.Errorf("unexpected table name: %s", tables[0].Name)
	}
}

func TestIcebergClient_CreateTable(t *testing.T) {
	var reqBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/namespaces/{namespace}/tables", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&reqBody)
		writeJSON(w, 200, map[string]any{
			"metadata": map[string]any{
				"format-version":    2,
				"table-uuid":        "abc-123",
				"location":          "s3://bucket/analytics/users",
				"current-schema-id": 0,
				"schemas": []map[string]any{
					{
						"schema-id": 0,
						"type":      "struct",
						"fields": []map[string]any{
							{"id": 1, "name": "id", "type": "long", "required": true},
						},
					},
				},
				"properties": map[string]string{},
			},
		})
	})

	client, _ := newTestClient(t, mux)
	req := CreateTableRequest{
		Name: "users",
		Schema: Schema{
			SchemaID: 0,
			Type:     "struct",
			Fields: []SchemaField{
				{ID: 1, Name: "id", Type: "long", Required: true},
			},
		},
	}
	meta, err := client.CreateTable(context.Background(), Namespace{"analytics"}, req)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if meta.TableUUID != "abc-123" {
		t.Errorf("unexpected UUID: %s", meta.TableUUID)
	}
	if reqBody["name"] != "users" {
		t.Errorf("unexpected request body: %v", reqBody)
	}
}

func TestIcebergClient_LoadTable(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/namespaces/{namespace}/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		snapshotID := int64(1000)
		writeJSON(w, 200, map[string]any{
			"metadata": map[string]any{
				"format-version":      2,
				"table-uuid":          "def-456",
				"location":            "s3://bucket/analytics/events",
				"current-schema-id":   1,
				"current-snapshot-id": snapshotID,
				"schemas":             []any{},
				"snapshots": []map[string]any{
					{
						"snapshot-id":   int64(1000),
						"timestamp-ms":  int64(1700000000000),
						"manifest-list": "s3://bucket/metadata/snap-1000.avro",
						"summary":       map[string]string{"operation": "append"},
					},
				},
				"properties": map[string]string{},
			},
		})
	})

	client, _ := newTestClient(t, mux)
	meta, err := client.LoadTable(context.Background(), TableIdentifier{Namespace: Namespace{"analytics"}, Name: "events"})
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	if meta.TableUUID != "def-456" {
		t.Errorf("unexpected UUID: %s", meta.TableUUID)
	}
	if meta.CurrentSnapshotID == nil || *meta.CurrentSnapshotID != 1000 {
		t.Errorf("unexpected snapshot ID: %v", meta.CurrentSnapshotID)
	}
	if len(meta.Snapshots) != 1 {
		t.Errorf("expected 1 snapshot, got %d", len(meta.Snapshots))
	}
}

func TestIcebergClient_UpdateTable(t *testing.T) {
	var reqBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/namespaces/{namespace}/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&reqBody)
		writeJSON(w, 200, map[string]any{
			"metadata": map[string]any{
				"format-version":    2,
				"table-uuid":        "def-456",
				"location":          "s3://bucket/analytics/events",
				"current-schema-id": 2,
				"schemas":           []any{},
				"properties":        map[string]string{},
			},
		})
	})

	client, _ := newTestClient(t, mux)
	updates := []TableUpdate{{Action: "add-column"}}
	reqs := []TableRequirement{{Type: "assert-current-schema-id"}}
	meta, err := client.UpdateTable(context.Background(), TableIdentifier{Namespace: Namespace{"analytics"}, Name: "events"}, updates, reqs)
	if err != nil {
		t.Fatalf("UpdateTable: %v", err)
	}
	if meta.CurrentSchemaID != 2 {
		t.Errorf("unexpected schema ID: %d", meta.CurrentSchemaID)
	}
	updates2, _ := reqBody["updates"].([]any)
	if len(updates2) != 1 {
		t.Errorf("expected 1 update in request, got %v", reqBody["updates"])
	}
}

func TestIcebergClient_DropTable(t *testing.T) {
	var purgeParam string
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /v1/namespaces/{namespace}/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		purgeParam = r.URL.Query().Get("purgeRequested")
		w.WriteHeader(204)
	})

	client, _ := newTestClient(t, mux)
	err := client.DropTable(context.Background(), TableIdentifier{Namespace: Namespace{"analytics"}, Name: "users"}, true)
	if err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	if purgeParam != "true" {
		t.Errorf("expected purgeRequested=true, got %q", purgeParam)
	}
}

func TestIcebergClient_TableExists(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("HEAD /v1/namespaces/{namespace}/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		tbl := r.PathValue("table")
		if tbl == "existing" {
			w.WriteHeader(200)
		} else {
			w.WriteHeader(404)
		}
	})

	client, _ := newTestClient(t, mux)

	exists, err := client.TableExists(context.Background(), TableIdentifier{Namespace: Namespace{"analytics"}, Name: "existing"})
	if err != nil {
		t.Fatalf("TableExists (existing): %v", err)
	}
	if !exists {
		t.Error("expected table to exist")
	}

	exists, err = client.TableExists(context.Background(), TableIdentifier{Namespace: Namespace{"analytics"}, Name: "missing"})
	if err != nil {
		t.Fatalf("TableExists (missing): %v", err)
	}
	if exists {
		t.Error("expected table not to exist")
	}
}

func TestIcebergClient_ListSnapshots(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/namespaces/{namespace}/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{
			"metadata": map[string]any{
				"format-version":    2,
				"table-uuid":        "snap-table",
				"location":          "s3://bucket",
				"current-schema-id": 0,
				"schemas":           []any{},
				"snapshots": []map[string]any{
					{"snapshot-id": int64(1), "timestamp-ms": int64(1000), "manifest-list": "s3://a", "summary": map[string]string{"operation": "append"}},
					{"snapshot-id": int64(2), "timestamp-ms": int64(2000), "manifest-list": "s3://b", "summary": map[string]string{"operation": "overwrite"}},
				},
				"properties": map[string]string{},
			},
		})
	})

	client, _ := newTestClient(t, mux)
	snaps, err := client.ListSnapshots(context.Background(), TableIdentifier{Namespace: Namespace{"analytics"}, Name: "users"})
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(snaps))
	}
	if snaps[1].SnapshotID != 2 {
		t.Errorf("unexpected snapshot ID: %d", snaps[1].SnapshotID)
	}
}

func TestIcebergClient_ErrorHandling(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/namespaces/{namespace}", func(w http.ResponseWriter, r *http.Request) {
		ns := r.PathValue("namespace")
		switch {
		case strings.Contains(ns, "notfound"):
			writeJSON(w, 404, map[string]any{
				"error": map[string]any{
					"message": "Namespace does not exist",
					"type":    "NoSuchNamespaceException",
					"code":    404,
				},
			})
		case strings.Contains(ns, "conflict"):
			writeJSON(w, 409, map[string]any{
				"error": map[string]any{
					"message": "Namespace already exists",
					"type":    "AlreadyExistsException",
					"code":    409,
				},
			})
		default:
			w.WriteHeader(500)
		}
	})

	client, _ := newTestClient(t, mux)

	_, err := client.LoadNamespace(context.Background(), Namespace{"notfound"})
	if err == nil {
		t.Fatal("expected error for 404")
	}
	if !strings.Contains(err.Error(), "NoSuchNamespaceException") {
		t.Errorf("expected error type in message, got: %v", err)
	}

	_, err = client.LoadNamespace(context.Background(), Namespace{"conflict"})
	if err == nil {
		t.Fatal("expected error for 409")
	}
	if !strings.Contains(err.Error(), "409") {
		t.Errorf("expected status code in error, got: %v", err)
	}

	_, err = client.LoadNamespace(context.Background(), Namespace{"broken"})
	if err == nil {
		t.Fatal("expected error for 500")
	}
}

func TestIcebergClient_Authentication(t *testing.T) {
	var authHeader string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/config", func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		writeJSON(w, 200, map[string]any{
			"defaults":  map[string]string{},
			"overrides": map[string]string{},
		})
	})

	client, _ := newTestClient(t, mux)
	_, err := client.GetConfig(context.Background())
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if authHeader != "Bearer test-token" {
		t.Errorf("expected Bearer auth header, got: %q", authHeader)
	}
}

func TestIcebergClient_UpdateNamespaceProperties(t *testing.T) {
	var reqBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/namespaces/{namespace}/properties", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&reqBody)
		writeJSON(w, 200, map[string]any{
			"removed": []string{},
			"updated": []string{"owner"},
			"missing": []string{},
		})
	})

	client, _ := newTestClient(t, mux)
	err := client.UpdateNamespaceProperties(
		context.Background(),
		Namespace{"analytics"},
		map[string]string{"owner": "bob"},
		map[string]string{"old_key": ""},
	)
	if err != nil {
		t.Fatalf("UpdateNamespaceProperties: %v", err)
	}
	updates, _ := reqBody["updates"].(map[string]any)
	if updates["owner"] != "bob" {
		t.Errorf("unexpected updates in request: %v", reqBody)
	}
}
