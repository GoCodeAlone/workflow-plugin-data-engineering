package catalog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// srTestServer is a minimal mock Schema Registry server.
type srTestServer struct {
	mux *http.ServeMux
	srv *httptest.Server
}

func newSRTestServer() *srTestServer {
	mux := http.NewServeMux()
	s := &srTestServer{mux: mux}
	s.srv = httptest.NewServer(mux)
	return s
}

func (s *srTestServer) close() { s.srv.Close() }

func (s *srTestServer) handle(path string, fn http.HandlerFunc) {
	s.mux.HandleFunc(path, fn)
}

func (s *srTestServer) client() SchemaRegistryClient {
	return NewSchemaRegistryClient(s.srv.URL, "", "", 10*time.Second)
}

func srJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	_ = json.NewEncoder(w).Encode(v)
}

// --- Client Tests ---

func TestSchemaRegistryClient_ListSubjects(t *testing.T) {
	srv := newSRTestServer()
	defer srv.close()

	srv.handle("/subjects", func(w http.ResponseWriter, _ *http.Request) {
		srJSON(w, []string{"user-value", "order-value", "payment-key"})
	})

	subjects, err := srv.client().ListSubjects(context.Background())
	if err != nil {
		t.Fatalf("ListSubjects: %v", err)
	}
	if len(subjects) != 3 {
		t.Errorf("expected 3 subjects, got %d", len(subjects))
	}
	if subjects[0] != "user-value" {
		t.Errorf("subjects[0]: got %q", subjects[0])
	}
}

func TestSchemaRegistryClient_RegisterSchema_JSON(t *testing.T) {
	srv := newSRTestServer()
	defer srv.close()

	var registered SchemaDefinition
	srv.handle("/subjects/user-value/versions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			_ = json.NewDecoder(r.Body).Decode(&registered)
			srJSON(w, map[string]int{"id": 42})
		}
	})

	schema := SchemaDefinition{
		Schema: `{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id","name"]}`,
		SchemaType: "JSON",
	}
	id, err := srv.client().RegisterSchema(context.Background(), "user-value", schema)
	if err != nil {
		t.Fatalf("RegisterSchema: %v", err)
	}
	if id != 42 {
		t.Errorf("id: got %d, want 42", id)
	}
	if registered.SchemaType != "JSON" {
		t.Errorf("registered schemaType: got %q", registered.SchemaType)
	}
}

func TestSchemaRegistryClient_RegisterSchema_Avro(t *testing.T) {
	srv := newSRTestServer()
	defer srv.close()

	srv.handle("/subjects/user-avro/versions", func(w http.ResponseWriter, _ *http.Request) {
		srJSON(w, map[string]int{"id": 7})
	})

	avroSchema := `{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"email","type":"string"}]}`
	id, err := srv.client().RegisterSchema(context.Background(), "user-avro", SchemaDefinition{
		Schema:     avroSchema,
		SchemaType: "AVRO",
	})
	if err != nil {
		t.Fatalf("RegisterSchema Avro: %v", err)
	}
	if id != 7 {
		t.Errorf("id: got %d", id)
	}
}

func TestSchemaRegistryClient_GetLatestSchema(t *testing.T) {
	srv := newSRTestServer()
	defer srv.close()

	srv.handle("/subjects/user-value/versions/latest", func(w http.ResponseWriter, _ *http.Request) {
		srJSON(w, SchemaInfo{
			Subject:    "user-value",
			Version:    3,
			ID:         42,
			Schema:     `{"type":"object"}`,
			SchemaType: "JSON",
		})
	})

	info, err := srv.client().GetLatestSchema(context.Background(), "user-value")
	if err != nil {
		t.Fatalf("GetLatestSchema: %v", err)
	}
	if info.Version != 3 {
		t.Errorf("version: got %d", info.Version)
	}
	if info.ID != 42 {
		t.Errorf("id: got %d", info.ID)
	}
}

func TestSchemaRegistryClient_CheckCompatibility_Pass(t *testing.T) {
	srv := newSRTestServer()
	defer srv.close()

	srv.handle("/compatibility/subjects/user-value/versions/latest", func(w http.ResponseWriter, _ *http.Request) {
		srJSON(w, map[string]bool{"is_compatible": true})
	})

	ok, err := srv.client().CheckCompatibility(context.Background(), "user-value", SchemaDefinition{
		Schema: `{"type":"object"}`,
	})
	if err != nil {
		t.Fatalf("CheckCompatibility: %v", err)
	}
	if !ok {
		t.Error("expected compatible=true")
	}
}

func TestSchemaRegistryClient_CheckCompatibility_Fail(t *testing.T) {
	srv := newSRTestServer()
	defer srv.close()

	srv.handle("/compatibility/subjects/user-value/versions/latest", func(w http.ResponseWriter, _ *http.Request) {
		srJSON(w, map[string]bool{"is_compatible": false})
	})

	ok, err := srv.client().CheckCompatibility(context.Background(), "user-value", SchemaDefinition{
		Schema: `{"type":"array"}`,
	})
	if err != nil {
		t.Fatalf("CheckCompatibility: %v", err)
	}
	if ok {
		t.Error("expected compatible=false")
	}
}

func TestSchemaRegistryClient_SetCompatibilityLevel(t *testing.T) {
	srv := newSRTestServer()
	defer srv.close()

	var received map[string]string
	srv.handle("/config/user-value", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			_ = json.NewDecoder(r.Body).Decode(&received)
			srJSON(w, map[string]string{"compatibility": "BACKWARD"})
		}
	})

	err := srv.client().SetCompatibilityLevel(context.Background(), "user-value", "BACKWARD")
	if err != nil {
		t.Fatalf("SetCompatibilityLevel: %v", err)
	}
	if received["compatibility"] != "BACKWARD" {
		t.Errorf("compatibility: got %q", received["compatibility"])
	}
}

func TestSchemaRegistryClient_DeleteSubject(t *testing.T) {
	srv := newSRTestServer()
	defer srv.close()

	srv.handle("/subjects/old-topic", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			srJSON(w, []int{1, 2, 3})
		}
	})

	versions, err := srv.client().DeleteSubject(context.Background(), "old-topic", false)
	if err != nil {
		t.Fatalf("DeleteSubject: %v", err)
	}
	if len(versions) != 3 {
		t.Errorf("expected 3 versions, got %d", len(versions))
	}
}

func TestSchemaRegistryClient_AuthHeader(t *testing.T) {
	srv := newSRTestServer()
	defer srv.close()

	var authHeader string
	srv.handle("/subjects", func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		srJSON(w, []string{})
	})

	client := NewSchemaRegistryClient(srv.srv.URL, "user1", "secret", 10*time.Second)
	_, err := client.ListSubjects(context.Background())
	if err != nil {
		t.Fatalf("ListSubjects: %v", err)
	}
	if authHeader == "" {
		t.Error("expected Authorization header to be set")
	}
	if len(authHeader) < 6 || authHeader[:5] != "Basic" {
		t.Errorf("expected Basic auth header, got %q", authHeader)
	}
}

// --- Module Tests ---

func TestSchemaRegistryModule_Init_Start(t *testing.T) {
	srv := newSRTestServer()
	defer srv.close()

	srv.handle("/subjects", func(w http.ResponseWriter, _ *http.Request) {
		srJSON(w, []string{})
	})
	srv.handle("/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			srJSON(w, map[string]string{"compatibility": "BACKWARD"})
		}
	})

	m := &SchemaRegistryModule{
		name: "sr-test",
		config: SchemaRegistryConfig{
			Endpoint:             srv.srv.URL,
			DefaultCompatibility: "BACKWARD",
		},
		newClient: func(cfg SchemaRegistryConfig) SchemaRegistryClient {
			return NewSchemaRegistryClient(cfg.Endpoint, cfg.Username, cfg.Password, 10*time.Second)
		},
	}

	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Should be registered.
	if _, err := LookupSRModule("sr-test"); err != nil {
		t.Errorf("LookupSRModule after Start: %v", err)
	}

	_ = m.Stop(context.Background())
	if _, err := LookupSRModule("sr-test"); err == nil {
		t.Error("expected lookup to fail after Stop")
	}
}

func TestSchemaRegistryModule_Init_MissingEndpoint(t *testing.T) {
	m := &SchemaRegistryModule{name: "sr", config: SchemaRegistryConfig{}}
	if err := m.Init(); err == nil {
		t.Fatal("expected error for missing endpoint")
	}
}

// --- Step Tests ---

func TestSchemaRegisterStep(t *testing.T) {
	srv := newSRTestServer()
	defer srv.close()

	srv.handle("/subjects", func(w http.ResponseWriter, _ *http.Request) {
		srJSON(w, []string{})
	})
	srv.handle("/subjects/user-value/versions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			srJSON(w, map[string]int{"id": 10})
		}
	})
	srv.handle("/subjects/user-value/versions/latest", func(w http.ResponseWriter, _ *http.Request) {
		srJSON(w, SchemaInfo{Subject: "user-value", Version: 1, ID: 10})
	})

	m := &SchemaRegistryModule{
		name:   "sr-reg-test",
		config: SchemaRegistryConfig{Endpoint: srv.srv.URL},
		newClient: func(cfg SchemaRegistryConfig) SchemaRegistryClient {
			return NewSchemaRegistryClient(cfg.Endpoint, cfg.Username, cfg.Password, 10*time.Second)
		},
	}
	_ = m.Start(context.Background())
	defer m.Stop(context.Background())

	step, _ := NewSchemaRegisterStep("reg", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"registry":   "sr-reg-test",
		"subject":    "user-value",
		"schema":     `{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}`,
		"schemaType": "JSON",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["schemaId"] != 10 {
		t.Errorf("schemaId: got %v", result.Output["schemaId"])
	}
	if result.Output["subject"] != "user-value" {
		t.Errorf("subject: got %v", result.Output["subject"])
	}
	if result.Output["version"] != 1 {
		t.Errorf("version: got %v", result.Output["version"])
	}
}

func TestSchemaValidateStep_Valid(t *testing.T) {
	srv := newSRTestServer()
	defer srv.close()

	srv.handle("/subjects", func(w http.ResponseWriter, _ *http.Request) {
		srJSON(w, []string{})
	})
	srv.handle("/subjects/user-value/versions/latest", func(w http.ResponseWriter, _ *http.Request) {
		srJSON(w, SchemaInfo{
			Subject:    "user-value",
			Version:    1,
			ID:         10,
			Schema:     `{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id","name"]}`,
			SchemaType: "JSON",
		})
	})

	m := &SchemaRegistryModule{
		name:   "sr-valid-test",
		config: SchemaRegistryConfig{Endpoint: srv.srv.URL},
		newClient: func(cfg SchemaRegistryConfig) SchemaRegistryClient {
			return NewSchemaRegistryClient(cfg.Endpoint, cfg.Username, cfg.Password, 10*time.Second)
		},
	}
	_ = m.Start(context.Background())
	defer m.Stop(context.Background())

	step, _ := NewSchemaValidateStep("val", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"registry": "sr-valid-test",
		"subject":  "user-value",
		"data":     `{"id":1,"name":"Alice"}`,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["valid"] != true {
		t.Errorf("valid: got %v", result.Output["valid"])
	}
}

func TestSchemaValidateStep_Invalid(t *testing.T) {
	srv := newSRTestServer()
	defer srv.close()

	srv.handle("/subjects", func(w http.ResponseWriter, _ *http.Request) {
		srJSON(w, []string{})
	})
	srv.handle("/subjects/user-value/versions/latest", func(w http.ResponseWriter, _ *http.Request) {
		srJSON(w, SchemaInfo{
			Subject:    "user-value",
			Version:    1,
			ID:         10,
			Schema:     `{"type":"object","required":["id","name"]}`,
			SchemaType: "JSON",
		})
	})

	m := &SchemaRegistryModule{
		name:   "sr-invalid-test",
		config: SchemaRegistryConfig{Endpoint: srv.srv.URL},
		newClient: func(cfg SchemaRegistryConfig) SchemaRegistryClient {
			return NewSchemaRegistryClient(cfg.Endpoint, cfg.Username, cfg.Password, 10*time.Second)
		},
	}
	_ = m.Start(context.Background())
	defer m.Stop(context.Background())

	step, _ := NewSchemaValidateStep("val", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"registry": "sr-invalid-test",
		"subject":  "user-value",
		"data":     `{"id":1}`, // missing "name"
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["valid"] != false {
		t.Errorf("valid: expected false, got %v", result.Output["valid"])
	}
	errs, _ := result.Output["errors"].([]string)
	if len(errs) == 0 {
		t.Error("expected validation errors")
	}
}

func TestNewSchemaRegistryModule_InvalidConfig(t *testing.T) {
	_, err := NewSchemaRegistryModule("sr", map[string]any{
		"timeout": "notaduration",
	})
	if err == nil {
		t.Fatal("expected error for invalid timeout")
	}
}

func TestSchemaRegistryClient_GetSchema(t *testing.T) {
	srv := newSRTestServer()
	defer srv.close()

	srv.handle("/schemas/ids/42", func(w http.ResponseWriter, _ *http.Request) {
		srJSON(w, SchemaDefinition{Schema: `{"type":"object"}`, SchemaType: "JSON"})
	})

	def, err := srv.client().GetSchema(context.Background(), 42)
	if err != nil {
		t.Fatalf("GetSchema: %v", err)
	}
	if def.SchemaType != "JSON" {
		t.Errorf("schemaType: got %q", def.SchemaType)
	}
}

func TestSchemaRegistryClient_GetSchemaByVersion(t *testing.T) {
	srv := newSRTestServer()
	defer srv.close()

	srv.handle("/subjects/user-value/versions/2", func(w http.ResponseWriter, _ *http.Request) {
		srJSON(w, SchemaInfo{Subject: "user-value", Version: 2, ID: 5, Schema: `{"type":"object"}`})
	})

	info, err := srv.client().GetSchemaByVersion(context.Background(), "user-value", "2")
	if err != nil {
		t.Fatalf("GetSchemaByVersion: %v", err)
	}
	if info.Version != 2 {
		t.Errorf("version: got %d", info.Version)
	}
}

func TestSchemaValidateStep_Avro(t *testing.T) {
	avroSchema := `{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"email","type":"string"}]}`

	client := NewSchemaRegistryClient("http://unused", "", "", 0) // direct validation — no HTTP needed
	def := SchemaDefinition{Schema: avroSchema, SchemaType: "AVRO"}

	// Valid data.
	if err := client.ValidateSchema(context.Background(), def, []byte(`{"id":1,"email":"a@b.com"}`)); err != nil {
		t.Errorf("valid avro: unexpected error: %v", err)
	}

	// Missing field.
	if err := client.ValidateSchema(context.Background(), def, []byte(`{"id":1}`)); err == nil {
		t.Error("missing email field: expected error")
	}
}

func TestSchemaRegistryModule_NewModule_ParsesAuth(t *testing.T) {
	srv := newSRTestServer()
	defer srv.close()

	_, err := NewSchemaRegistryModule("sr", map[string]any{
		"endpoint": "http://schema-registry:8081",
		"auth": map[string]any{
			"username": "user",
			"password": "pass",
		},
		"timeout": "30s",
	})
	if err != nil {
		t.Fatalf("NewSchemaRegistryModule: %v", err)
	}
	_ = time.Second // use time import
}
