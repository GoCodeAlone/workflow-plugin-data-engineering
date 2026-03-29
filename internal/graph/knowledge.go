package graph

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// Entity represents an extracted entity from text.
type Entity struct {
	Type  string `json:"type"`
	Value string `json:"value"`
	Start int    `json:"start"`
	End   int    `json:"end"`
}

// entityPattern pairs a type name with its compiled regex.
type entityPattern struct {
	entityType string
	re         *regexp.Regexp
}

// defaultPatterns are the built-in entity extraction regexes.
var defaultPatterns = []entityPattern{
	{
		entityType: "email",
		re:         regexp.MustCompile(`[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`),
	},
	{
		entityType: "date",
		re: regexp.MustCompile(
			`(?:\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{2,4}|\d{1,2}\s+(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{4})`,
		),
	},
	{
		entityType: "org",
		re: regexp.MustCompile(
			`(?:[A-Z][a-zA-Z&\s]*\s+(?:Inc|LLC|Corp|Ltd|Co|Group|Holdings|Technologies|Solutions|Services|International|Enterprises)\.?|[A-Z]{2,}(?:\s+[A-Z]{2,})*)`,
		),
	},
	{
		entityType: "location",
		re: regexp.MustCompile(
			`(?:New York|Los Angeles|San Francisco|Chicago|London|Paris|Berlin|Tokyo|Beijing|Sydney|Toronto|Mumbai|Dubai|Singapore|Boston|Seattle|Austin|Denver|Atlanta|Miami|Phoenix|Dallas|Houston|Washington|Philadelphia|Portland|Minneapolis)`,
		),
	},
	{
		entityType: "person",
		re:         regexp.MustCompile(`(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})`),
	},
}

// -- step.graph_extract_entities --

type graphExtractEntitiesStep struct {
	name string
}

// NewGraphExtractEntitiesStep creates a new step.graph_extract_entities instance.
func NewGraphExtractEntitiesStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &graphExtractEntitiesStep{name: name}, nil
}

func (s *graphExtractEntitiesStep) Execute(_ context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	text, _ := config["text"].(string)
	if text == "" {
		return &sdk.StepResult{Output: map[string]any{"entities": []Entity{}, "count": 0}}, nil
	}

	// Determine which entity types to extract
	wantTypes := map[string]bool{}
	if types, ok := config["types"].([]any); ok && len(types) > 0 {
		for _, t := range types {
			if ts, ok := t.(string); ok {
				wantTypes[ts] = true
			}
		}
	} else {
		// Default: all types
		for _, p := range defaultPatterns {
			wantTypes[p.entityType] = true
		}
	}

	// Build pattern list: defaults filtered by wantTypes + custom patterns
	patterns := buildPatterns(wantTypes, config)

	entities := extractEntities(text, patterns)

	return &sdk.StepResult{
		Output: map[string]any{
			"entities": entities,
			"count":    len(entities),
		},
	}, nil
}

// buildPatterns returns patterns for the requested types plus any custom patterns.
func buildPatterns(wantTypes map[string]bool, config map[string]any) []entityPattern {
	var patterns []entityPattern

	// Add built-in patterns in a deterministic order (email > date > org > location > person)
	// so higher-specificity types match first.
	typeOrder := []string{"email", "date", "org", "location", "person"}
	defaultByType := map[string]entityPattern{}
	for _, p := range defaultPatterns {
		defaultByType[p.entityType] = p
	}
	for _, t := range typeOrder {
		if wantTypes[t] {
			if p, ok := defaultByType[t]; ok {
				patterns = append(patterns, p)
			}
		}
	}

	// Add custom patterns
	if customPatterns, ok := config["patterns"].([]any); ok {
		for _, cp := range customPatterns {
			cpMap, ok := cp.(map[string]any)
			if !ok {
				continue
			}
			entityType, _ := cpMap["type"].(string)
			pattern, _ := cpMap["pattern"].(string)
			if entityType == "" || pattern == "" {
				continue
			}
			re, err := regexp.Compile(pattern)
			if err != nil {
				continue
			}
			patterns = append(patterns, entityPattern{entityType: entityType, re: re})
		}
	}
	return patterns
}

// extractEntities finds all matches for each pattern and deduplicates overlapping spans.
func extractEntities(text string, patterns []entityPattern) []Entity {
	// Track which byte ranges are already claimed (to avoid double-counting overlaps)
	used := make([]bool, len(text))
	var entities []Entity

	for _, p := range patterns {
		locs := p.re.FindAllStringIndex(text, -1)
		for _, loc := range locs {
			start, end := loc[0], loc[1]
			// Skip if any byte in this span is already used
			overlap := false
			for i := start; i < end; i++ {
				if used[i] {
					overlap = true
					break
				}
			}
			if overlap {
				continue
			}
			value := strings.TrimSpace(text[start:end])
			if value == "" {
				continue
			}
			for i := start; i < end; i++ {
				used[i] = true
			}
			entities = append(entities, Entity{
				Type:  p.entityType,
				Value: value,
				Start: start,
				End:   end,
			})
		}
	}
	return entities
}

// -- step.graph_link --

type graphLinkStep struct {
	name string
}

// NewGraphLinkStep creates a new step.graph_link instance.
func NewGraphLinkStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &graphLinkStep{name: name}, nil
}

func (s *graphLinkStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	moduleName, _ := config["module"].(string)
	if moduleName == "" {
		return nil, fmt.Errorf("step.graph_link %q: module is required", s.name)
	}

	fromCfg, _ := config["from"].(map[string]any)
	toCfg, _ := config["to"].(map[string]any)
	relType, _ := config["type"].(string)

	if fromCfg == nil || toCfg == nil {
		return nil, fmt.Errorf("step.graph_link %q: from and to are required", s.name)
	}
	if relType == "" {
		return nil, fmt.Errorf("step.graph_link %q: type is required", s.name)
	}

	fromLabel, _ := fromCfg["label"].(string)
	fromKey, _ := fromCfg["key"].(string)
	toLabel, _ := toCfg["label"].(string)
	toKey, _ := toCfg["key"].(string)

	if fromLabel == "" || toLabel == "" {
		return nil, fmt.Errorf("step.graph_link %q: from.label and to.label are required", s.name)
	}

	props, _ := config["properties"].(map[string]any)

	mod, err := LookupNeo4jModule(moduleName)
	if err != nil {
		return nil, fmt.Errorf("step.graph_link %q: %w", s.name, err)
	}

	cypher, params, err := buildLinkCypher(fromLabel, fromKey, toLabel, toKey, relType, props)
	if err != nil {
		return nil, fmt.Errorf("step.graph_link %q: %w", s.name, err)
	}
	rows, err := mod.ExecuteCypher(ctx, cypher, params)
	if err != nil {
		return nil, fmt.Errorf("step.graph_link %q: %w", s.name, err)
	}

	linked := 0
	if len(rows) > 0 {
		if v, ok := rows[0]["linked"].(int64); ok {
			linked = int(v)
		}
	}

	return &sdk.StepResult{
		Output: map[string]any{"linked": linked},
	}, nil
}

// buildLinkCypher builds a MATCH + MERGE Cypher for creating relationships.
func buildLinkCypher(fromLabel, fromKey, toLabel, toKey, relType string, props map[string]any) (string, map[string]any, error) {
	for _, pair := range [][2]string{
		{fromLabel, "from label"}, {toLabel, "to label"}, {relType, "relationship type"},
	} {
		if err := validateCypherIdent(pair[0], pair[1]); err != nil {
			return "", nil, err
		}
	}
	for k := range props {
		if err := validateCypherIdent(k, "property key"); err != nil {
			return "", nil, err
		}
	}
	params := make(map[string]any)
	var matchClauses []string

	if fromKey != "" {
		matchClauses = append(matchClauses, fmt.Sprintf("MATCH (a:%s {key: $fromKey})", fromLabel))
		params["fromKey"] = fromKey
	} else {
		matchClauses = append(matchClauses, fmt.Sprintf("MATCH (a:%s)", fromLabel))
	}
	if toKey != "" {
		matchClauses = append(matchClauses, fmt.Sprintf("MATCH (b:%s {key: $toKey})", toLabel))
		params["toKey"] = toKey
	} else {
		matchClauses = append(matchClauses, fmt.Sprintf("MATCH (b:%s)", toLabel))
	}

	var setParts []string
	for k, v := range props {
		paramKey := "rprop_" + k
		params[paramKey] = v
		setParts = append(setParts, fmt.Sprintf("r.%s = $%s", k, paramKey))
	}

	var cypher string
	if len(setParts) > 0 {
		cypher = fmt.Sprintf("%s MERGE (a)-[r:%s]->(b) SET %s RETURN count(r) AS linked",
			strings.Join(matchClauses, " "), relType, strings.Join(setParts, ", "))
	} else {
		cypher = fmt.Sprintf("%s MERGE (a)-[r:%s]->(b) RETURN count(r) AS linked",
			strings.Join(matchClauses, " "), relType)
	}
	return cypher, params, nil
}
