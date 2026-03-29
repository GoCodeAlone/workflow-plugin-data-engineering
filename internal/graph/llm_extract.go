package graph

import (
	"context"
	"fmt"
	"log"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// -- step.graph_extract_entities_llm --

type graphExtractEntitiesLLMStep struct {
	name      string
	newClaude func(apiKey, model string) LLMClient
	newOpenAI func(apiKey, model string) LLMClient
}

// NewGraphExtractEntitiesLLMStep creates a new step.graph_extract_entities_llm instance.
func NewGraphExtractEntitiesLLMStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &graphExtractEntitiesLLMStep{
		name:      name,
		newClaude: NewClaudeClient,
		newOpenAI: NewOpenAIClient,
	}, nil
}

func (s *graphExtractEntitiesLLMStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	text, _ := config["text"].(string)
	if text == "" {
		return &sdk.StepResult{Output: map[string]any{
			"entities": []map[string]any{},
			"count":    0,
			"provider": "",
			"fallback": false,
		}}, nil
	}

	provider, _ := config["provider"].(string)
	if provider == "" {
		provider = "claude"
	}
	apiKey, _ := config["apiKey"].(string)
	model, _ := config["model"].(string)

	var types []string
	if rawTypes, ok := config["types"].([]any); ok {
		for _, t := range rawTypes {
			if ts, ok := t.(string); ok {
				types = append(types, ts)
			}
		}
	}
	if len(types) == 0 {
		types = []string{"person", "org", "location", "product", "concept"}
	}

	if apiKey == "" {
		return nil, fmt.Errorf("step.graph_extract_entities_llm %q: apiKey is required", s.name)
	}

	// Create LLM client
	var llmClient LLMClient
	switch provider {
	case "openai":
		llmClient = s.newOpenAI(apiKey, model)
	default:
		llmClient = s.newClaude(apiKey, model)
		provider = "claude"
	}

	// Attempt LLM extraction
	llmEntities, err := llmClient.ExtractEntities(ctx, text, types)
	useFallback := false
	if err != nil {
		log.Printf("step.graph_extract_entities_llm %q: LLM extraction failed (%v), falling back to regex", s.name, err)
		useFallback = true
	}

	if useFallback {
		// Fall back to regex-based extraction
		wantTypes := map[string]bool{}
		for _, t := range types {
			wantTypes[t] = true
		}
		patterns := buildPatterns(wantTypes, config)
		regexEntities := extractEntities(text, patterns)

		entities := make([]map[string]any, 0, len(regexEntities))
		for _, e := range regexEntities {
			entities = append(entities, map[string]any{
				"type":       e.Type,
				"value":      e.Value,
				"context":    "",
				"confidence": 0.5,
			})
		}
		return &sdk.StepResult{Output: map[string]any{
			"entities": entities,
			"count":    len(entities),
			"provider": provider,
			"fallback": true,
		}}, nil
	}

	// Convert LLMEntity to map
	entities := make([]map[string]any, 0, len(llmEntities))
	for _, e := range llmEntities {
		entities = append(entities, map[string]any{
			"type":       e.Type,
			"value":      e.Value,
			"context":    e.Context,
			"confidence": e.Confidence,
		})
	}

	return &sdk.StepResult{Output: map[string]any{
		"entities": entities,
		"count":    len(entities),
		"provider": provider,
		"fallback": false,
	}}, nil
}
