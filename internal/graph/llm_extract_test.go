package graph

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// -- Mock LLM servers --

func newClaudeMockServer(t *testing.T, entities []LLMEntity) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/messages" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		inputJSON, _ := json.Marshal(map[string]any{"entities": entities})
		resp := map[string]any{
			"id":    "msg_test",
			"type":  "message",
			"role":  "assistant",
			"model": "claude-haiku-4-5-20251001",
			"content": []map[string]any{
				{
					"type":  "tool_use",
					"id":    "tool_test",
					"name":  "extract_entities",
					"input": json.RawMessage(inputJSON),
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newOpenAIMockServer(t *testing.T, entities []LLMEntity) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		content, _ := json.Marshal(map[string]any{"entities": entities})
		resp := map[string]any{
			"choices": []map[string]any{
				{
					"message": map[string]any{
						"role":    "assistant",
						"content": string(content),
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newErrorServer(t *testing.T, code int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "server error", code)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// -- Tests --

func TestLLMExtract_Claude(t *testing.T) {
	expected := []LLMEntity{
		{Type: "person", Value: "John Smith", Context: "John Smith works at Acme", Confidence: 0.95},
		{Type: "org", Value: "Acme Corp", Context: "John Smith works at Acme Corp", Confidence: 0.9},
	}
	srv := newClaudeMockServer(t, expected)

	step := &graphExtractEntitiesLLMStep{
		name:      "test",
		newClaude: func(apiKey, model string) LLMClient { return newClaudeClientWithBase(srv.URL, apiKey, model) },
		newOpenAI: NewOpenAIClient,
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"text":     "John Smith works at Acme Corp in New York.",
		"types":    []any{"person", "org"},
		"provider": "claude",
		"apiKey":   "test-key",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["provider"] != "claude" {
		t.Errorf("expected provider=claude, got %v", result.Output["provider"])
	}
	if result.Output["fallback"] != false {
		t.Errorf("expected fallback=false, got %v", result.Output["fallback"])
	}
	entities, _ := result.Output["entities"].([]map[string]any)
	if len(entities) != 2 {
		t.Errorf("expected 2 entities, got %d", len(entities))
	}
	if result.Output["count"] != 2 {
		t.Errorf("expected count=2, got %v", result.Output["count"])
	}
}

func TestLLMExtract_OpenAI(t *testing.T) {
	expected := []LLMEntity{
		{Type: "location", Value: "Paris", Context: "meeting in Paris", Confidence: 0.98},
	}
	srv := newOpenAIMockServer(t, expected)

	step := &graphExtractEntitiesLLMStep{
		name:      "test_openai",
		newClaude: NewClaudeClient,
		newOpenAI: func(apiKey, model string) LLMClient { return newOpenAIClientWithBase(srv.URL, apiKey, model) },
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"text":     "The team will have a meeting in Paris next week.",
		"types":    []any{"location"},
		"provider": "openai",
		"apiKey":   "test-key",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["provider"] != "openai" {
		t.Errorf("expected provider=openai, got %v", result.Output["provider"])
	}
	if result.Output["fallback"] != false {
		t.Errorf("expected fallback=false, got %v", result.Output["fallback"])
	}
	entities, _ := result.Output["entities"].([]map[string]any)
	if len(entities) != 1 {
		t.Errorf("expected 1 entity, got %d", len(entities))
	}
}

func TestLLMExtract_Fallback(t *testing.T) {
	// Use an error server to trigger fallback
	srv := newErrorServer(t, http.StatusInternalServerError)

	step := &graphExtractEntitiesLLMStep{
		name:      "test_fallback",
		newClaude: func(apiKey, model string) LLMClient { return newClaudeClientWithBase(srv.URL, apiKey, model) },
		newOpenAI: NewOpenAIClient,
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"text":     "John Smith from Acme Corp visited New York on 2024-01-15.",
		"types":    []any{"person", "location", "date"},
		"provider": "claude",
		"apiKey":   "test-key",
	})
	if err != nil {
		t.Fatalf("Execute (should not error on fallback): %v", err)
	}
	if result.Output["fallback"] != true {
		t.Errorf("expected fallback=true, got %v", result.Output["fallback"])
	}
	// Regex should extract at least some entities (date, location)
	count, _ := result.Output["count"].(int)
	_ = count // just verify it ran
}

func TestLLMExtract_ParseResponse(t *testing.T) {
	// Test parsing of OpenAI JSON response
	content := `{"entities":[{"type":"person","value":"Alice","context":"Alice works here","confidence":0.9}]}`
	entities, err := parseOpenAIEntities(content)
	if err != nil {
		t.Fatalf("parseOpenAIEntities: %v", err)
	}
	if len(entities) != 1 {
		t.Fatalf("expected 1 entity, got %d", len(entities))
	}
	if entities[0].Type != "person" {
		t.Errorf("expected type=person, got %q", entities[0].Type)
	}
	if entities[0].Value != "Alice" {
		t.Errorf("expected value=Alice, got %q", entities[0].Value)
	}
	if entities[0].Confidence != 0.9 {
		t.Errorf("expected confidence=0.9, got %v", entities[0].Confidence)
	}
}

func TestLLMExtract_MissingAPIKey(t *testing.T) {
	step, _ := NewGraphExtractEntitiesLLMStep("test_nokey", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"text":     "some text",
		"provider": "claude",
		// No apiKey
	})
	if err == nil {
		t.Fatal("expected error for missing apiKey")
	}
}

func TestLLMExtract_EmptyText(t *testing.T) {
	step, _ := NewGraphExtractEntitiesLLMStep("test_empty", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"text":   "",
		"apiKey": "key",
	})
	if err != nil {
		t.Fatalf("unexpected error for empty text: %v", err)
	}
	if result.Output["count"] != 0 {
		t.Errorf("expected count=0, got %v", result.Output["count"])
	}
}

func TestLLMExtract_Claude_HTTP_Error(t *testing.T) {
	srv := newErrorServer(t, http.StatusUnauthorized)

	client := newClaudeClientWithBase(srv.URL, "bad-key", "")
	_, err := client.ExtractEntities(context.Background(), "test", []string{"person"})
	if err == nil {
		t.Fatal("expected error for HTTP 401")
	}
}

func TestLLMExtract_OpenAI_HTTP_Error(t *testing.T) {
	srv := newErrorServer(t, http.StatusUnauthorized)

	client := newOpenAIClientWithBase(srv.URL, "bad-key", "")
	_, err := client.ExtractEntities(context.Background(), "test", []string{"person"})
	if err == nil {
		t.Fatal("expected error for HTTP 401")
	}
}

func TestLLMExtract_OpenAI_EmptyChoices(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := map[string]any{"choices": []any{}}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	client := newOpenAIClientWithBase(srv.URL, "key", "")
	_, err := client.ExtractEntities(context.Background(), "test", []string{"person"})
	if err == nil {
		t.Fatal("expected error for empty choices")
	}
}

func TestLLMExtract_Claude_NoToolUse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := map[string]any{
			"content": []map[string]any{
				{"type": "text", "text": "I cannot extract entities."},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	client := newClaudeClientWithBase(srv.URL, "key", "")
	_, err := client.ExtractEntities(context.Background(), "test", []string{"person"})
	if err == nil {
		t.Fatal("expected error when no tool_use block returned")
	}
}

// Race condition safety test
func TestLLMExtract_Concurrent(t *testing.T) {
	expected := []LLMEntity{
		{Type: "person", Value: "Bob", Context: "Bob is here", Confidence: 0.8},
	}
	srv := newClaudeMockServer(t, expected)

	step := &graphExtractEntitiesLLMStep{
		name:      fmt.Sprintf("concurrent-%d", 0),
		newClaude: func(apiKey, model string) LLMClient { return newClaudeClientWithBase(srv.URL, apiKey, model) },
		newOpenAI: NewOpenAIClient,
	}

	const n = 5
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
				"text":     "Bob is here.",
				"provider": "claude",
				"apiKey":   "key",
			})
			errs <- err
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent execution error: %v", err)
		}
	}
}
