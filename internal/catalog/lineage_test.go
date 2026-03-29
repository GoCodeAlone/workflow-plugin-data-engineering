package catalog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// -- DataHub lineage tests --

func TestCatalogLineage_DataHub(t *testing.T) {
	var setLineageCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/relationships", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "wrong method", http.StatusMethodNotAllowed)
			return
		}
		setLineageCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mod := &DataHubModule{
		name:   "dh_lineage",
		client: NewDataHubClient(srv.URL, "tok", 0),
	}
	if err := RegisterCatalogModule("dh_lineage", mod); err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { UnregisterCatalogModule("dh_lineage") })

	step, _ := NewCatalogLineageStep("s1", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"catalog":  "dh_lineage",
		"pipeline": "etl_pipeline",
		"upstream": []any{
			map[string]any{"dataset": "raw_events", "platform": "mysql"},
		},
		"downstream": []any{
			map[string]any{"dataset": "fact_events", "platform": "bigquery"},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "recorded" {
		t.Errorf("expected status=recorded, got %v", result.Output["status"])
	}
	if result.Output["upstreamCount"] != 1 {
		t.Errorf("expected upstreamCount=1, got %v", result.Output["upstreamCount"])
	}
	if result.Output["downstreamCount"] != 1 {
		t.Errorf("expected downstreamCount=1, got %v", result.Output["downstreamCount"])
	}
	if setLineageCalls.Load() != 1 {
		t.Errorf("expected 1 SetLineage call, got %d", setLineageCalls.Load())
	}
}

func TestCatalogLineage_OpenMetadata(t *testing.T) {
	var addEdgeCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/lineage", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "wrong method", http.StatusMethodNotAllowed)
			return
		}
		addEdgeCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mod := &OpenMetadataModule{
		name:   "om_lineage",
		client: NewOpenMetadataClient(srv.URL, "tok", 0),
	}
	if err := RegisterCatalogModule("om_lineage", mod); err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { UnregisterCatalogModule("om_lineage") })

	step, _ := NewCatalogLineageStep("s2", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"catalog": "om_lineage",
		"upstream": []any{
			map[string]any{"dataset": "db.raw", "platform": "postgres"},
		},
		"downstream": []any{
			map[string]any{"dataset": "db.processed", "platform": "postgres"},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "recorded" {
		t.Errorf("expected status=recorded, got %v", result.Output["status"])
	}
	if addEdgeCalls.Load() != 1 {
		t.Errorf("expected 1 AddLineageEdge call, got %d", addEdgeCalls.Load())
	}
}

func TestCatalogLineageQuery(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/relationships", func(w http.ResponseWriter, r *http.Request) {
		writeDHJSON(w, 200, map[string]any{
			"entities": []any{
				map[string]any{"urn": "urn:li:dataset:upstream_table", "name": "upstream_table", "platform": "postgres"},
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mod := &DataHubModule{
		name:   "dh_query",
		client: NewDataHubClient(srv.URL, "", 0),
	}
	if err := RegisterCatalogModule("dh_query", mod); err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { UnregisterCatalogModule("dh_query") })

	step, _ := NewCatalogLineageQueryStep("q1", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"catalog":   "dh_query",
		"dataset":   "urn:li:dataset:fact_events",
		"direction": "upstream",
		"depth":     1,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	nodes, ok := result.Output["nodes"].([]map[string]any)
	if !ok {
		t.Fatalf("expected nodes to be []map[string]any, got %T", result.Output["nodes"])
	}
	if len(nodes) != 1 {
		t.Errorf("expected 1 node, got %d", len(nodes))
	}
}

func TestLineage_BothDirections(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/lineage/table/name/", func(w http.ResponseWriter, r *http.Request) {
		writeOMJSON(w, 200, map[string]any{
			"entity": map[string]any{"fullyQualifiedName": "db.orders"},
			"nodes": []any{
				map[string]any{"fullyQualifiedName": "db.users"},
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mod := &OpenMetadataModule{
		name:   "om_both",
		client: NewOpenMetadataClient(srv.URL, "", 0),
	}
	if err := RegisterCatalogModule("om_both", mod); err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { UnregisterCatalogModule("om_both") })

	step, _ := NewCatalogLineageQueryStep("q2", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"catalog":   "om_both",
		"dataset":   "db.orders",
		"direction": "both",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["nodes"] == nil {
		t.Error("expected nodes in output")
	}
}

func TestCatalogLineage_DataHub_SetLineage_Payload(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/relationships", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewDataHubClient(srv.URL, "", 0)
	err := client.SetLineage(context.Background(),
		"urn:li:dataset:(urn:li:dataPlatform:mysql,raw,PROD)",
		"urn:li:dataset:(urn:li:dataPlatform:bigquery,fact,PROD)",
	)
	if err != nil {
		t.Fatalf("SetLineage: %v", err)
	}
	if gotBody["upstreamUrns"] == nil {
		t.Error("expected upstreamUrns in body")
	}
	if gotBody["downstreamUrns"] == nil {
		t.Error("expected downstreamUrns in body")
	}
}

func TestCatalogLineage_MissingCatalog(t *testing.T) {
	step, _ := NewCatalogLineageStep("s_bad", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"catalog": "nonexistent",
	})
	if err == nil {
		t.Fatal("expected error for nonexistent catalog")
	}
}

func TestCatalogLineageQuery_MissingDataset(t *testing.T) {
	step, _ := NewCatalogLineageQueryStep("q_bad", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"catalog": "some_catalog",
	})
	if err == nil {
		t.Fatal("expected error for missing dataset")
	}
}
