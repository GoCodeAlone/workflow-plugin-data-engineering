package graph

import (
	"context"
	"strings"
	"testing"
)

func TestExtractEntities_Person(t *testing.T) {
	step, _ := NewGraphExtractEntitiesStep("e1", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"text":  "John Smith joined the company last year.",
		"types": []any{"person"},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	entities := toEntities(t, result.Output["entities"])
	if !hasEntityType(entities, "person") {
		t.Errorf("expected person entity, got: %v", entities)
	}
}

func TestExtractEntities_Org(t *testing.T) {
	step, _ := NewGraphExtractEntitiesStep("e2", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"text":  "She works at Acme Corp and previously at TechVentures LLC.",
		"types": []any{"org"},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	entities := toEntities(t, result.Output["entities"])
	if !hasEntityType(entities, "org") {
		t.Errorf("expected org entity, got: %v", entities)
	}
}

func TestExtractEntities_Location(t *testing.T) {
	step, _ := NewGraphExtractEntitiesStep("e3", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"text":  "The conference was held in New York and San Francisco.",
		"types": []any{"location"},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	entities := toEntities(t, result.Output["entities"])
	if !hasEntityType(entities, "location") {
		t.Errorf("expected location entity, got: %v", entities)
	}
	if !hasEntityValue(entities, "New York") && !hasEntityValue(entities, "San Francisco") {
		t.Errorf("expected New York or San Francisco, got: %v", entities)
	}
}

func TestExtractEntities_Date(t *testing.T) {
	step, _ := NewGraphExtractEntitiesStep("e4", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"text":  "The deadline is 2024-12-31 and the meeting is on 01/15/2025.",
		"types": []any{"date"},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	entities := toEntities(t, result.Output["entities"])
	if !hasEntityType(entities, "date") {
		t.Errorf("expected date entity, got: %v", entities)
	}
}

func TestExtractEntities_Email(t *testing.T) {
	step, _ := NewGraphExtractEntitiesStep("e5", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"text":  "Contact us at support@example.com or sales@company.org.",
		"types": []any{"email"},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	entities := toEntities(t, result.Output["entities"])
	if len(entities) < 1 {
		t.Fatalf("expected at least 1 email entity, got 0")
	}
	if !hasEntityType(entities, "email") {
		t.Errorf("expected email type, got: %v", entities)
	}
	if !hasEntityValue(entities, "support@example.com") {
		t.Errorf("expected support@example.com in entities")
	}
}

func TestExtractEntities_Mixed(t *testing.T) {
	step, _ := NewGraphExtractEntitiesStep("e6", nil)
	text := "Jane Doe (jane@example.com) from Acme Corp met in London on 2024-03-15."
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"text":  text,
		"types": []any{"email", "date", "location"},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	entities := toEntities(t, result.Output["entities"])

	types := map[string]bool{}
	for _, e := range entities {
		types[e.Type] = true
	}
	if !types["email"] {
		t.Errorf("expected email entity in: %v", entities)
	}
	if !types["date"] {
		t.Errorf("expected date entity in: %v", entities)
	}
	if !types["location"] {
		t.Errorf("expected location entity in: %v", entities)
	}
}

func TestExtractEntities_CustomPattern(t *testing.T) {
	step, _ := NewGraphExtractEntitiesStep("e7", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"text":  "Order #12345 was shipped with tracking SKU-ABC-789.",
		"types": []any{"order_id"},
		"patterns": []any{
			map[string]any{
				"type":    "order_id",
				"pattern": `#\d+`,
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	entities := toEntities(t, result.Output["entities"])
	if !hasEntityType(entities, "order_id") {
		t.Errorf("expected order_id entity, got: %v", entities)
	}
	if !hasEntityValue(entities, "#12345") {
		t.Errorf("expected #12345 entity, got: %v", entities)
	}
}

func TestExtractEntities_EmptyText(t *testing.T) {
	step, _ := NewGraphExtractEntitiesStep("e8", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"text": "",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["count"] != 0 {
		t.Errorf("expected count=0 for empty text")
	}
}

// -- Tests: step.graph_link --

func TestGraphLink(t *testing.T) {
	ctx := context.Background()
	sess := &captureSession{}
	registerModule(t, "lmod", buildCaptureDriver(sess))

	step, _ := NewGraphLinkStep("l1", nil)
	result, err := step.Execute(ctx, nil, nil, nil, nil, map[string]any{
		"module": "lmod",
		"from":   map[string]any{"label": "Person", "key": "alice"},
		"to":     map[string]any{"label": "Company", "key": "acme"},
		"type":   "WORKS_AT",
		"properties": map[string]any{
			"since": "2020",
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	_ = result

	if !strings.Contains(sess.lastCypher, "MERGE") {
		t.Errorf("expected MERGE in cypher, got: %q", sess.lastCypher)
	}
	if !strings.Contains(sess.lastCypher, "WORKS_AT") {
		t.Errorf("expected WORKS_AT in cypher, got: %q", sess.lastCypher)
	}
	if !strings.Contains(sess.lastCypher, "Person") {
		t.Errorf("expected Person in cypher, got: %q", sess.lastCypher)
	}
}

func TestGraphLink_MissingModule(t *testing.T) {
	step, _ := NewGraphLinkStep("l_bad", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"from": map[string]any{"label": "A"},
		"to":   map[string]any{"label": "B"},
		"type": "REL",
	})
	if err == nil {
		t.Fatal("expected error for missing module")
	}
}

func TestGraphLink_MissingType(t *testing.T) {
	sess := &captureSession{}
	registerModule(t, "lmod2", buildCaptureDriver(sess))
	step, _ := NewGraphLinkStep("l_notype", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"module": "lmod2",
		"from":   map[string]any{"label": "A"},
		"to":     map[string]any{"label": "B"},
	})
	if err == nil {
		t.Fatal("expected error for missing type")
	}
}

// -- Helpers --

func toEntities(t *testing.T, raw any) []Entity {
	t.Helper()
	if raw == nil {
		return nil
	}
	switch v := raw.(type) {
	case []Entity:
		return v
	case []any:
		result := make([]Entity, 0, len(v))
		for _, item := range v {
			if e, ok := item.(Entity); ok {
				result = append(result, e)
			}
		}
		return result
	default:
		t.Fatalf("unexpected entities type: %T", raw)
		return nil
	}
}

func hasEntityType(entities []Entity, typ string) bool {
	for _, e := range entities {
		if e.Type == typ {
			return true
		}
	}
	return false
}

func hasEntityValue(entities []Entity, value string) bool {
	for _, e := range entities {
		if strings.Contains(e.Value, value) || e.Value == value {
			return true
		}
	}
	return false
}
