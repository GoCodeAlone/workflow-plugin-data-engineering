package catalog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func writeOMJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func TestOpenMetadataClient_GetTable(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/tables/name/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "wrong method", http.StatusMethodNotAllowed)
			return
		}
		writeOMJSON(w, 200, map[string]any{
			"id":                 "tbl-uuid",
			"name":               "users",
			"fullyQualifiedName": "db.schema.users",
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewOpenMetadataClient(srv.URL, "", 0)
	tbl, err := client.GetTable(context.Background(), "db.schema.users")
	if err != nil {
		t.Fatalf("GetTable: %v", err)
	}
	if tbl.Name != "users" {
		t.Errorf("expected name=users, got %q", tbl.Name)
	}
}

func TestOpenMetadataClient_SearchTables(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/search/query", func(w http.ResponseWriter, r *http.Request) {
		writeOMJSON(w, 200, map[string]any{
			"hits": map[string]any{
				"total": map[string]any{"value": 1},
				"hits": []any{
					map[string]any{
						"_source": map[string]any{
							"name": "orders",
						},
					},
				},
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewOpenMetadataClient(srv.URL, "", 0)
	result, err := client.SearchTables(context.Background(), "orders", 10)
	if err != nil {
		t.Fatalf("SearchTables: %v", err)
	}
	if result.Total != 1 {
		t.Errorf("expected total=1, got %d", result.Total)
	}
	if len(result.Tables) != 1 {
		t.Errorf("expected 1 table, got %d", len(result.Tables))
	}
}

func TestOpenMetadataClient_CreateOrUpdateTable(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/tables", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "wrong method", http.StatusMethodNotAllowed)
			return
		}
		writeOMJSON(w, 200, map[string]any{"name": "events"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewOpenMetadataClient(srv.URL, "", 0)
	err := client.CreateOrUpdateTable(context.Background(), OMTable{Name: "events"})
	if err != nil {
		t.Fatalf("CreateOrUpdateTable: %v", err)
	}
}

func TestOpenMetadataClient_AddTag(t *testing.T) {
	calls := map[string]int{}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/tables/name/", func(w http.ResponseWriter, r *http.Request) {
		calls["get"]++
		writeOMJSON(w, 200, map[string]any{"id": "tbl-1", "name": "users"})
	})
	mux.HandleFunc("/api/v1/tables/tbl-1/tags", func(w http.ResponseWriter, r *http.Request) {
		calls["tag"]++
		w.WriteHeader(200)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewOpenMetadataClient(srv.URL, "", 0)
	err := client.AddTag(context.Background(), "db.schema.users", "PII")
	if err != nil {
		t.Fatalf("AddTag: %v", err)
	}
	if calls["get"] != 1 || calls["tag"] != 1 {
		t.Errorf("expected 1 GET and 1 tag call, got %v", calls)
	}
}

func TestOpenMetadataClient_SetOwner(t *testing.T) {
	callCount := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/tables/name/", func(w http.ResponseWriter, r *http.Request) {
		writeOMJSON(w, 200, map[string]any{"id": "tbl-2", "name": "orders"})
	})
	mux.HandleFunc("/api/v1/tables", func(w http.ResponseWriter, r *http.Request) {
		callCount++
		writeOMJSON(w, 200, map[string]any{"name": "orders"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewOpenMetadataClient(srv.URL, "", 0)
	err := client.SetOwner(context.Background(), "db.schema.orders", "alice")
	if err != nil {
		t.Fatalf("SetOwner: %v", err)
	}
	if callCount != 1 {
		t.Errorf("expected 1 PUT call for SetOwner, got %d", callCount)
	}
}

func TestOpenMetadataClient_GetLineage(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/lineage/table/name/", func(w http.ResponseWriter, r *http.Request) {
		writeOMJSON(w, 200, map[string]any{
			"entity": map[string]any{"fullyQualifiedName": "db.schema.orders"},
			"nodes": []any{
				map[string]any{"fullyQualifiedName": "db.schema.users"},
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewOpenMetadataClient(srv.URL, "", 0)
	result, err := client.GetLineage(context.Background(), "db.schema.orders")
	if err != nil {
		t.Fatalf("GetLineage: %v", err)
	}
	if result.FQN != "db.schema.orders" {
		t.Errorf("expected FQN=db.schema.orders, got %q", result.FQN)
	}
	if len(result.Upstream) != 1 {
		t.Errorf("expected 1 upstream, got %d", len(result.Upstream))
	}
}

func TestOpenMetadataClient_HTTPError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/tables/name/", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewOpenMetadataClient(srv.URL, "", 0)
	_, err := client.GetTable(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for 404")
	}
}

func TestOpenMetadataClient_AuthHeader(t *testing.T) {
	var gotAuth string
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/tables", func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		writeOMJSON(w, 200, map[string]any{})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewOpenMetadataClient(srv.URL, "om-token", 0)
	_ = client.CreateOrUpdateTable(context.Background(), OMTable{Name: "t"})
	if gotAuth != "Bearer om-token" {
		t.Errorf("expected Bearer om-token, got %q", gotAuth)
	}
}
