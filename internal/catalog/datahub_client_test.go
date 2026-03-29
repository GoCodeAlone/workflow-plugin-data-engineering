package catalog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newDHTestServer(mux *http.ServeMux) (*httptest.Server, func()) {
	srv := httptest.NewServer(mux)
	return srv, srv.Close
}

func writeDHJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func TestDataHubClient_GetDataset(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/entities/v1/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "wrong method", http.StatusMethodNotAllowed)
			return
		}
		writeDHJSON(w, 200, map[string]any{
			"urn": "urn:li:dataset:mydb.users",
			"entity": map[string]any{
				"datasetProperties": map[string]any{"name": "users"},
				"dataPlatformInstance": map[string]any{"platform": "mysql"},
			},
		})
	})
	srv, close := newDHTestServer(mux)
	defer close()

	client := NewDataHubClient(srv.URL, "", 0)
	ds, err := client.GetDataset(context.Background(), "urn:li:dataset:mydb.users")
	if err != nil {
		t.Fatalf("GetDataset: %v", err)
	}
	if ds.Name != "users" {
		t.Errorf("expected name=users, got %q", ds.Name)
	}
	if ds.Platform != "mysql" {
		t.Errorf("expected platform=mysql, got %q", ds.Platform)
	}
}

func TestDataHubClient_SearchDatasets(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/entities/v1/search", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "wrong method", http.StatusMethodNotAllowed)
			return
		}
		writeDHJSON(w, 200, map[string]any{
			"numEntities": 2,
			"from":        0,
			"entities": []any{
				map[string]any{"urn": "urn:li:dataset:users", "name": "users"},
				map[string]any{"urn": "urn:li:dataset:orders", "name": "orders"},
			},
		})
	})
	srv, close := newDHTestServer(mux)
	defer close()

	client := NewDataHubClient(srv.URL, "token123", 0)
	result, err := client.SearchDatasets(context.Background(), "user", 0, 10)
	if err != nil {
		t.Fatalf("SearchDatasets: %v", err)
	}
	if result.Total != 2 {
		t.Errorf("expected total=2, got %d", result.Total)
	}
	if len(result.Entities) != 2 {
		t.Errorf("expected 2 entities, got %d", len(result.Entities))
	}
}

func TestDataHubClient_EmitMetadata(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/aspects", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	srv, close := newDHTestServer(mux)
	defer close()

	client := NewDataHubClient(srv.URL, "", 0)
	err := client.EmitMetadata(context.Background(), []MetadataProposal{
		{
			EntityURN:  "urn:li:dataset:test",
			AspectName: "datasetProperties",
			Aspect:     map[string]any{"name": "test"},
		},
	})
	if err != nil {
		t.Fatalf("EmitMetadata: %v", err)
	}
}

func TestDataHubClient_AddTag(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/entities", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	srv, close := newDHTestServer(mux)
	defer close()

	client := NewDataHubClient(srv.URL, "", 0)
	err := client.AddTag(context.Background(), "urn:li:dataset:test", "pii")
	if err != nil {
		t.Fatalf("AddTag: %v", err)
	}
}

func TestDataHubClient_AddGlossaryTerm(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/entities", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	srv, close := newDHTestServer(mux)
	defer close()

	client := NewDataHubClient(srv.URL, "", 0)
	err := client.AddGlossaryTerm(context.Background(), "urn:li:dataset:test", "CustomerData")
	if err != nil {
		t.Fatalf("AddGlossaryTerm: %v", err)
	}
}

func TestDataHubClient_SetOwner(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/entities", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	srv, close := newDHTestServer(mux)
	defer close()

	client := NewDataHubClient(srv.URL, "", 0)
	err := client.SetOwner(context.Background(), "urn:li:dataset:test", "urn:li:corpuser:alice", "USER")
	if err != nil {
		t.Fatalf("SetOwner: %v", err)
	}
}

func TestDataHubClient_GetLineage(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/relationships", func(w http.ResponseWriter, r *http.Request) {
		writeDHJSON(w, 200, map[string]any{
			"entities": []any{
				map[string]any{"urn": "urn:li:dataset:upstream", "name": "upstream"},
			},
		})
	})
	srv, close := newDHTestServer(mux)
	defer close()

	client := NewDataHubClient(srv.URL, "", 0)
	result, err := client.GetLineage(context.Background(), "urn:li:dataset:test", "UPSTREAM")
	if err != nil {
		t.Fatalf("GetLineage: %v", err)
	}
	if len(result.Entities) != 1 {
		t.Errorf("expected 1 entity, got %d", len(result.Entities))
	}
}

func TestDataHubClient_HTTPError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/entities/v1/", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	})
	srv, close := newDHTestServer(mux)
	defer close()

	client := NewDataHubClient(srv.URL, "", 0)
	_, err := client.GetDataset(context.Background(), "urn:li:dataset:missing")
	if err == nil {
		t.Fatal("expected error for 404")
	}
}

func TestDataHubClient_AuthHeader(t *testing.T) {
	var gotAuth string
	mux := http.NewServeMux()
	mux.HandleFunc("/aspects", func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(200)
	})
	srv, close := newDHTestServer(mux)
	defer close()

	client := NewDataHubClient(srv.URL, "mytoken", 0)
	_ = client.EmitMetadata(context.Background(), []MetadataProposal{})
	if gotAuth != "Bearer mytoken" {
		t.Errorf("expected Bearer mytoken, got %q", gotAuth)
	}
}
