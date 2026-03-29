package graph

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/httpclient"
)

// LLMEntity is an entity extracted by an LLM.
type LLMEntity struct {
	Type       string  `json:"type"`
	Value      string  `json:"value"`
	Context    string  `json:"context"`
	Confidence float64 `json:"confidence"`
}

// LLMClient is the interface for LLM-based entity extraction.
type LLMClient interface {
	ExtractEntities(ctx context.Context, text string, types []string) ([]LLMEntity, error)
}

// -- Claude client --

// claudeClient uses the Anthropic Messages API with tool_use for structured output.
type claudeClient struct {
	client *httpclient.Client
	model  string
}

// NewClaudeClient creates an LLM client backed by the Anthropic Messages API.
func NewClaudeClient(apiKey, model string) LLMClient {
	if model == "" {
		model = "claude-haiku-4-5-20251001"
	}
	return &claudeClient{
		client: httpclient.New("https://api.anthropic.com", httpclient.AuthConfig{
			Type:  "bearer",
			Token: apiKey,
		}, 30*time.Second),
		model: model,
	}
}

// newClaudeClientWithBase creates a claudeClient with a custom base URL (for testing).
func newClaudeClientWithBase(baseURL, apiKey, model string) LLMClient {
	if model == "" {
		model = "claude-haiku-4-5-20251001"
	}
	return &claudeClient{
		client: httpclient.New(baseURL, httpclient.AuthConfig{
			Type:  "bearer",
			Token: apiKey,
		}, 30*time.Second),
		model: model,
	}
}

func (c *claudeClient) ExtractEntities(ctx context.Context, text string, types []string) ([]LLMEntity, error) {
	prompt := buildExtractionPrompt(text, types)

	body := map[string]any{
		"model":      c.model,
		"max_tokens": 1024,
		"tools": []map[string]any{
			{
				"name":        "extract_entities",
				"description": "Extract named entities from text and return structured JSON",
				"input_schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"entities": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"type":       map[string]any{"type": "string"},
									"value":      map[string]any{"type": "string"},
									"context":    map[string]any{"type": "string"},
									"confidence": map[string]any{"type": "number"},
								},
								"required": []string{"type", "value"},
							},
						},
					},
					"required": []string{"entities"},
				},
			},
		},
		"messages": []map[string]any{
			{"role": "user", "content": prompt},
		},
	}

	var resp struct {
		Content []struct {
			Type  string          `json:"type"`
			Input json.RawMessage `json:"input"`
		} `json:"content"`
	}

	// Add Anthropic-version header via custom Do + manual header
	httpResp, err := c.client.Do(ctx, "POST", "/v1/messages", body)
	if err != nil {
		return nil, fmt.Errorf("claude: request: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode >= 400 {
		return nil, fmt.Errorf("claude: HTTP %d", httpResp.StatusCode)
	}

	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		return nil, fmt.Errorf("claude: decode response: %w", err)
	}

	for _, block := range resp.Content {
		if block.Type == "tool_use" {
			var input struct {
				Entities []LLMEntity `json:"entities"`
			}
			if err := json.Unmarshal(block.Input, &input); err != nil {
				return nil, fmt.Errorf("claude: parse tool_use input: %w", err)
			}
			return input.Entities, nil
		}
	}

	return nil, fmt.Errorf("claude: no tool_use block in response")
}

// -- OpenAI client --

// openaiClient uses the OpenAI Chat Completions API with JSON mode.
type openaiClient struct {
	client *httpclient.Client
	model  string
}

// NewOpenAIClient creates an LLM client backed by the OpenAI Chat Completions API.
func NewOpenAIClient(apiKey, model string) LLMClient {
	if model == "" {
		model = "gpt-4o-mini"
	}
	return &openaiClient{
		client: httpclient.New("https://api.openai.com", httpclient.AuthConfig{
			Type:  "bearer",
			Token: apiKey,
		}, 30*time.Second),
		model: model,
	}
}

// newOpenAIClientWithBase creates an openaiClient with a custom base URL (for testing).
func newOpenAIClientWithBase(baseURL, apiKey, model string) LLMClient {
	if model == "" {
		model = "gpt-4o-mini"
	}
	return &openaiClient{
		client: httpclient.New(baseURL, httpclient.AuthConfig{
			Type:  "bearer",
			Token: apiKey,
		}, 30*time.Second),
		model: model,
	}
}

func (c *openaiClient) ExtractEntities(ctx context.Context, text string, types []string) ([]LLMEntity, error) {
	prompt := buildExtractionPrompt(text, types) +
		` Return a JSON object with key "entities" containing an array of {type, value, context, confidence}.`

	body := map[string]any{
		"model":           c.model,
		"response_format": map[string]any{"type": "json_object"},
		"messages": []map[string]any{
			{"role": "user", "content": prompt},
		},
	}

	var resp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}

	if err := c.client.DoJSON(ctx, "POST", "/v1/chat/completions", body, &resp); err != nil {
		return nil, fmt.Errorf("openai: %w", err)
	}

	if len(resp.Choices) == 0 {
		return nil, fmt.Errorf("openai: empty choices in response")
	}

	return parseOpenAIEntities(resp.Choices[0].Message.Content)
}

func parseOpenAIEntities(content string) ([]LLMEntity, error) {
	var result struct {
		Entities []LLMEntity `json:"entities"`
	}
	if err := json.Unmarshal([]byte(content), &result); err != nil {
		return nil, fmt.Errorf("openai: parse response JSON: %w", err)
	}
	return result.Entities, nil
}

// buildExtractionPrompt builds the entity extraction prompt.
func buildExtractionPrompt(text string, types []string) string {
	typesStr := strings.Join(types, ", ")
	if typesStr == "" {
		typesStr = "person, organization, location, product, concept"
	}
	return fmt.Sprintf(
		"Extract entities of types [%s] from the following text. "+
			"For each entity return: type, value (the entity text), context (surrounding sentence), confidence (0-1).\n\nText: %s",
		typesStr, text,
	)
}
