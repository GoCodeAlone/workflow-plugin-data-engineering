package httpclient_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/httpclient"
)

func TestClient_Do_JSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "bad content-type", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	c := httpclient.New(srv.URL, httpclient.AuthConfig{}, 5*time.Second)
	resp, err := c.Do(context.Background(), http.MethodPost, "/", map[string]string{"key": "val"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d", resp.StatusCode)
	}
}

func TestClient_Do_BearerAuth(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := httpclient.New(srv.URL, httpclient.AuthConfig{Type: "bearer", Token: "mytoken"}, 5*time.Second)
	resp, err := c.Do(context.Background(), http.MethodGet, "/", nil)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()
	if gotAuth != "Bearer mytoken" {
		t.Errorf("Authorization: got %q, want %q", gotAuth, "Bearer mytoken")
	}
}

func TestClient_Do_BasicAuth(t *testing.T) {
	var gotUser, gotPass string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, _ = r.BasicAuth()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := httpclient.New(srv.URL, httpclient.AuthConfig{Type: "basic", Username: "admin", Password: "secret"}, 5*time.Second)
	resp, err := c.Do(context.Background(), http.MethodGet, "/", nil)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()
	if gotUser != "admin" || gotPass != "secret" {
		t.Errorf("basic auth: got user=%q pass=%q", gotUser, gotPass)
	}
}

func TestClient_Do_ErrorResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	c := httpclient.New(srv.URL, httpclient.AuthConfig{}, 5*time.Second)
	err := c.DoJSON(context.Background(), http.MethodGet, "/missing", nil, nil)
	if err == nil {
		t.Fatal("expected error for 404, got nil")
	}
}

func TestClient_DoJSON(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload{Name: "alice", Age: 30})
	}))
	defer srv.Close()

	c := httpclient.New(srv.URL, httpclient.AuthConfig{}, 5*time.Second)
	var out payload
	if err := c.DoJSON(context.Background(), http.MethodGet, "/", nil, &out); err != nil {
		t.Fatalf("DoJSON: %v", err)
	}
	if out.Name != "alice" || out.Age != 30 {
		t.Errorf("DoJSON result: got %+v", out)
	}
}
