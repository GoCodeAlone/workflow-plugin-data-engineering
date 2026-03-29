package lakehouse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// setupStepCatalog registers a mock catalog for step tests.
func setupStepCatalog(t *testing.T, mux *http.ServeMux) string {
	t.Helper()
	mux.HandleFunc("GET /v1/config", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{
			"defaults": map[string]string{}, "overrides": map[string]string{},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	name := "step-cat-" + t.Name()
	client, err := NewIcebergCatalogClient(IcebergClientConfig{Endpoint: srv.URL + "/v1"})
	if err != nil {
		t.Fatalf("NewIcebergCatalogClient: %v", err)
	}
	if err := RegisterCatalog(name, client); err != nil {
		t.Fatalf("RegisterCatalog: %v", err)
	}
	t.Cleanup(func() { UnregisterCatalog(name) })
	return name
}

func execStep(t *testing.T, step sdk.StepInstance, config map[string]any) map[string]any {
	t.Helper()
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, config)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	return result.Output
}

func TestLakehouseCreateTable(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/namespaces/{namespace}/tables", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{
			"metadata": map[string]any{
				"format-version":    2,
				"table-uuid":        "create-uuid",
				"location":          "s3://bucket/ns/tbl",
				"current-schema-id": 0,
				"schemas":           []any{},
				"properties":        map[string]string{},
			},
		})
	})
	catName := setupStepCatalog(t, mux)

	step, err := NewCreateTableStep("create-tbl", nil)
	if err != nil {
		t.Fatalf("NewCreateTableStep: %v", err)
	}

	out := execStep(t, step, map[string]any{
		"catalog":   catName,
		"namespace": []any{"analytics"},
		"table":     "orders",
		"schema": map[string]any{
			"fields": []any{
				map[string]any{"name": "id", "type": "long", "required": true},
			},
		},
	})

	if out["status"] != "created" {
		t.Errorf("unexpected status: %v", out["status"])
	}
	if out["tableUUID"] != "create-uuid" {
		t.Errorf("unexpected tableUUID: %v", out["tableUUID"])
	}
}

func TestLakehouseEvolveSchema_AddColumn(t *testing.T) {
	var reqBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/namespaces/{namespace}/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&reqBody)
		writeJSON(w, 200, map[string]any{
			"metadata": map[string]any{
				"format-version":    2,
				"table-uuid":        "evolve-uuid",
				"location":          "s3://bucket",
				"current-schema-id": 1,
				"schemas":           []any{},
				"properties":        map[string]string{},
			},
		})
	})
	catName := setupStepCatalog(t, mux)

	step, err := NewEvolveSchemaStep("evolve", nil)
	if err != nil {
		t.Fatalf("NewEvolveSchemaStep: %v", err)
	}

	out := execStep(t, step, map[string]any{
		"catalog":   catName,
		"namespace": []any{"analytics"},
		"table":     "users",
		"changes": []any{
			map[string]any{"action": "add-column", "name": "phone", "type": "string", "doc": "Phone number"},
		},
	})

	if out["status"] != "evolved" {
		t.Errorf("unexpected status: %v", out["status"])
	}
	if out["schemaId"] == nil {
		t.Error("expected schemaId in output")
	}

	updates, _ := reqBody["updates"].([]any)
	if len(updates) == 0 {
		t.Errorf("expected updates in request body, got: %v", reqBody)
	}
}

func TestLakehouseWrite_Append(t *testing.T) {
	var reqBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/namespaces/{namespace}/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&reqBody)
		snapshotID := int64(9001)
		writeJSON(w, 200, map[string]any{
			"metadata": map[string]any{
				"format-version":      2,
				"table-uuid":          "write-uuid",
				"location":            "s3://bucket",
				"current-schema-id":   0,
				"current-snapshot-id": snapshotID,
				"schemas":             []any{},
				"properties":          map[string]string{},
			},
		})
	})
	catName := setupStepCatalog(t, mux)

	step, err := NewWriteStep("write", nil)
	if err != nil {
		t.Fatalf("NewWriteStep: %v", err)
	}

	out := execStep(t, step, map[string]any{
		"catalog":   catName,
		"namespace": []any{"analytics"},
		"table":     "events",
		"mode":      "append",
		"data": []any{
			map[string]any{"id": 1, "name": "alice"},
			map[string]any{"id": 2, "name": "bob"},
		},
	})

	if out["status"] != "written" {
		t.Errorf("unexpected status: %v", out["status"])
	}
	if out["recordCount"] != 2 {
		t.Errorf("unexpected recordCount: %v", out["recordCount"])
	}
	if out["snapshotId"] != int64(9001) {
		t.Errorf("unexpected snapshotId: %v", out["snapshotId"])
	}
}

func TestLakehouseCompact(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/namespaces/{namespace}/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{
			"metadata": map[string]any{
				"format-version":    2,
				"table-uuid":        "compact-uuid",
				"location":          "s3://bucket",
				"current-schema-id": 0,
				"schemas":           []any{},
				"properties":        map[string]string{},
			},
		})
	})
	catName := setupStepCatalog(t, mux)

	step, err := NewCompactStep("compact", nil)
	if err != nil {
		t.Fatalf("NewCompactStep: %v", err)
	}

	out := execStep(t, step, map[string]any{
		"catalog":        catName,
		"namespace":      []any{"analytics"},
		"table":          "events",
		"targetFileSize": "256MB",
	})

	if out["status"] != "compaction_requested" {
		t.Errorf("unexpected status: %v", out["status"])
	}
	if out["table"] != "events" {
		t.Errorf("unexpected table: %v", out["table"])
	}
}

func TestLakehouseSnapshot_List(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/namespaces/{namespace}/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{
			"metadata": map[string]any{
				"format-version":    2,
				"table-uuid":        "snap-uuid",
				"location":          "s3://bucket",
				"current-schema-id": 0,
				"schemas":           []any{},
				"snapshots": []any{
					map[string]any{"snapshot-id": int64(1), "timestamp-ms": int64(1000), "manifest-list": "s3://a", "summary": map[string]string{}},
					map[string]any{"snapshot-id": int64(2), "timestamp-ms": int64(2000), "manifest-list": "s3://b", "summary": map[string]string{}},
				},
				"properties": map[string]string{},
			},
		})
	})
	catName := setupStepCatalog(t, mux)

	step, err := NewSnapshotStep("snap", nil)
	if err != nil {
		t.Fatalf("NewSnapshotStep: %v", err)
	}

	out := execStep(t, step, map[string]any{
		"catalog":   catName,
		"namespace": []any{"analytics"},
		"table":     "users",
		"action":    "list",
	})

	snaps, _ := out["snapshots"].([]Snapshot)
	if len(snaps) != 2 {
		t.Errorf("expected 2 snapshots, got type=%T val=%v", out["snapshots"], out["snapshots"])
	}
}

func TestLakehouseSnapshot_Rollback(t *testing.T) {
	var reqBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/namespaces/{namespace}/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&reqBody)
		writeJSON(w, 200, map[string]any{
			"metadata": map[string]any{
				"format-version":    2,
				"table-uuid":        "rollback-uuid",
				"location":          "s3://bucket",
				"current-schema-id": 0,
				"schemas":           []any{},
				"properties":        map[string]string{},
			},
		})
	})
	catName := setupStepCatalog(t, mux)

	step, err := NewSnapshotStep("snap", nil)
	if err != nil {
		t.Fatalf("NewSnapshotStep: %v", err)
	}

	out := execStep(t, step, map[string]any{
		"catalog":    catName,
		"namespace":  []any{"analytics"},
		"table":      "users",
		"action":     "rollback",
		"snapshotId": int64(42),
	})

	if out["status"] != "rolled_back" {
		t.Errorf("unexpected status: %v", out["status"])
	}
}

func TestLakehouseQuery(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/namespaces/{namespace}/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{
			"metadata": map[string]any{
				"format-version":      2,
				"table-uuid":          "query-uuid",
				"location":            "s3://bucket",
				"current-schema-id":   0,
				"current-snapshot-id": int64(55),
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
	catName := setupStepCatalog(t, mux)

	step, err := NewQueryStep("query", nil)
	if err != nil {
		t.Fatalf("NewQueryStep: %v", err)
	}

	out := execStep(t, step, map[string]any{
		"catalog":   catName,
		"namespace": []any{"analytics"},
		"table":     "users",
		"limit":     10,
	})

	if out["currentSnapshotId"] == nil {
		t.Error("expected currentSnapshotId in output")
	}
	if out["schema"] == nil {
		t.Error("expected schema in output")
	}
}

func TestLakehouseExpireSnapshots(t *testing.T) {
	var reqBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/namespaces/{namespace}/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&reqBody)
		writeJSON(w, 200, map[string]any{
			"metadata": map[string]any{
				"format-version":    2,
				"table-uuid":        "expire-uuid",
				"location":          "s3://bucket",
				"current-schema-id": 0,
				"schemas":           []any{},
				"properties":        map[string]string{},
			},
		})
	})
	catName := setupStepCatalog(t, mux)

	step, err := NewExpireSnapshotsStep("expire", nil)
	if err != nil {
		t.Fatalf("NewExpireSnapshotsStep: %v", err)
	}

	out := execStep(t, step, map[string]any{
		"catalog":   catName,
		"namespace": []any{"analytics"},
		"table":     "users",
		"olderThan": "168h",
	})

	if out["status"] != "expired" {
		t.Errorf("unexpected status: %v", out["status"])
	}
}
