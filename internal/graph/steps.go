package graph

import (
	"context"
	"fmt"
	"strings"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// -- step.graph_query --

type graphQueryStep struct {
	name string
}

// NewGraphQueryStep creates a new step.graph_query instance.
func NewGraphQueryStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &graphQueryStep{name: name}, nil
}

func (s *graphQueryStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	moduleName, _ := config["module"].(string)
	if moduleName == "" {
		return nil, fmt.Errorf("step.graph_query %q: module is required", s.name)
	}
	cypher, _ := config["cypher"].(string)
	if cypher == "" {
		return nil, fmt.Errorf("step.graph_query %q: cypher is required", s.name)
	}
	params, _ := config["params"].(map[string]any)

	mod, err := LookupNeo4jModule(moduleName)
	if err != nil {
		return nil, fmt.Errorf("step.graph_query %q: %w", s.name, err)
	}

	rows, err := mod.ExecuteCypher(ctx, cypher, params)
	if err != nil {
		return nil, fmt.Errorf("step.graph_query %q: %w", s.name, err)
	}
	return &sdk.StepResult{
		Output: map[string]any{
			"rows":  rows,
			"count": len(rows),
		},
	}, nil
}

// -- step.graph_write --

type graphWriteStep struct {
	name string
}

// NewGraphWriteStep creates a new step.graph_write instance.
func NewGraphWriteStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &graphWriteStep{name: name}, nil
}

func (s *graphWriteStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	moduleName, _ := config["module"].(string)
	if moduleName == "" {
		return nil, fmt.Errorf("step.graph_write %q: module is required", s.name)
	}

	mod, err := LookupNeo4jModule(moduleName)
	if err != nil {
		return nil, fmt.Errorf("step.graph_write %q: %w", s.name, err)
	}

	nodesCreated := 0
	relsCreated := 0

	// Process nodes
	if nodes, ok := config["nodes"].([]any); ok {
		for _, n := range nodes {
			nodeMap, ok := n.(map[string]any)
			if !ok {
				continue
			}
			label, _ := nodeMap["label"].(string)
			if label == "" {
				continue
			}
			props, _ := nodeMap["properties"].(map[string]any)

			cypher, params := buildNodeMergeCypher(label, props)
			rows, err := mod.ExecuteCypher(ctx, cypher, params)
			if err != nil {
				return nil, fmt.Errorf("step.graph_write %q: node merge: %w", s.name, err)
			}
			if len(rows) > 0 {
				if v, ok := rows[0]["nodesCreated"].(int64); ok {
					nodesCreated += int(v)
				}
			}
		}
	}

	// Process relationships
	if rels, ok := config["relationships"].([]any); ok {
		for _, r := range rels {
			relMap, ok := r.(map[string]any)
			if !ok {
				continue
			}
			fromLabel, _ := relMap["from"].(string)
			toLabel, _ := relMap["to"].(string)
			relType, _ := relMap["type"].(string)
			if fromLabel == "" || toLabel == "" || relType == "" {
				continue
			}
			props, _ := relMap["properties"].(map[string]any)

			cypher, params := buildRelMergeCypher(fromLabel, toLabel, relType, props)
			rows, err := mod.ExecuteCypher(ctx, cypher, params)
			if err != nil {
				return nil, fmt.Errorf("step.graph_write %q: relationship merge: %w", s.name, err)
			}
			if len(rows) > 0 {
				if v, ok := rows[0]["relationshipsCreated"].(int64); ok {
					relsCreated += int(v)
				}
			}
		}
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"nodesCreated":         nodesCreated,
			"relationshipsCreated": relsCreated,
		},
	}, nil
}

// buildNodeMergeCypher generates a MERGE Cypher for a labeled node with properties.
func buildNodeMergeCypher(label string, props map[string]any) (string, map[string]any) {
	params := make(map[string]any)
	var setParts []string
	for k, v := range props {
		paramKey := "prop_" + k
		params[paramKey] = v
		setParts = append(setParts, fmt.Sprintf("n.%s = $%s", k, paramKey))
	}
	cypher := fmt.Sprintf("MERGE (n:%s) SET %s RETURN count(n) AS nodesCreated", label, strings.Join(setParts, ", "))
	if len(setParts) == 0 {
		cypher = fmt.Sprintf("MERGE (n:%s) RETURN count(n) AS nodesCreated", label)
	}
	return cypher, params
}

// buildRelMergeCypher generates a MERGE Cypher for a relationship between two node labels.
func buildRelMergeCypher(fromLabel, toLabel, relType string, props map[string]any) (string, map[string]any) {
	params := make(map[string]any)
	var setParts []string
	for k, v := range props {
		paramKey := "rel_" + k
		params[paramKey] = v
		setParts = append(setParts, fmt.Sprintf("r.%s = $%s", k, paramKey))
	}
	var cypher string
	if len(setParts) > 0 {
		cypher = fmt.Sprintf(
			"MATCH (a:%s), (b:%s) MERGE (a)-[r:%s]->(b) SET %s RETURN count(r) AS relationshipsCreated",
			fromLabel, toLabel, relType, strings.Join(setParts, ", "),
		)
	} else {
		cypher = fmt.Sprintf(
			"MATCH (a:%s), (b:%s) MERGE (a)-[r:%s]->(b) RETURN count(r) AS relationshipsCreated",
			fromLabel, toLabel, relType,
		)
	}
	return cypher, params
}

// -- step.graph_import --

type graphImportStep struct {
	name string
}

// NewGraphImportStep creates a new step.graph_import instance.
func NewGraphImportStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &graphImportStep{name: name}, nil
}

func (s *graphImportStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	moduleName, _ := config["module"].(string)
	if moduleName == "" {
		return nil, fmt.Errorf("step.graph_import %q: module is required", s.name)
	}

	source, _ := config["source"].([]any)
	if len(source) == 0 {
		return &sdk.StepResult{Output: map[string]any{"imported": 0, "nodeLabel": ""}}, nil
	}

	mapping, _ := config["mapping"].(map[string]any)
	if mapping == nil {
		return nil, fmt.Errorf("step.graph_import %q: mapping is required", s.name)
	}
	nodeLabel, _ := mapping["nodeLabel"].(string)
	if nodeLabel == "" {
		return nil, fmt.Errorf("step.graph_import %q: mapping.nodeLabel is required", s.name)
	}
	propMap, _ := mapping["properties"].(map[string]any) // neo4jProp -> sourceField

	mod, err := LookupNeo4jModule(moduleName)
	if err != nil {
		return nil, fmt.Errorf("step.graph_import %q: %w", s.name, err)
	}

	// Build UNWIND batch MERGE
	cypher, params := buildImportCypher(nodeLabel, propMap, source)
	rows, err := mod.ExecuteCypher(ctx, cypher, params)
	if err != nil {
		return nil, fmt.Errorf("step.graph_import %q: %w", s.name, err)
	}

	imported := len(source)
	if len(rows) > 0 {
		if v, ok := rows[0]["imported"].(int64); ok {
			imported = int(v)
		}
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"imported":  imported,
			"nodeLabel": nodeLabel,
		},
	}, nil
}

// buildImportCypher builds an UNWIND + MERGE query for bulk import.
func buildImportCypher(nodeLabel string, propMap map[string]any, source []any) (string, map[string]any) {
	// Transform source records applying property mapping
	rows := make([]any, 0, len(source))
	for _, item := range source {
		src, ok := item.(map[string]any)
		if !ok {
			continue
		}
		row := make(map[string]any)
		if len(propMap) > 0 {
			for neo4jProp, srcFieldAny := range propMap {
				srcField, _ := srcFieldAny.(string)
				if val, ok := src[srcField]; ok {
					row[neo4jProp] = val
				}
			}
		} else {
			// Pass through all fields
			for k, v := range src {
				row[k] = v
			}
		}
		rows = append(rows, row)
	}

	// Build SET clause from propMap keys (or generic "row" spread)
	var setParts []string
	if len(propMap) > 0 {
		for neo4jProp := range propMap {
			setParts = append(setParts, fmt.Sprintf("n.%s = row.%s", neo4jProp, neo4jProp))
		}
	}

	var cypher string
	if len(setParts) > 0 {
		cypher = fmt.Sprintf(
			"UNWIND $rows AS row MERGE (n:%s) SET %s RETURN count(n) AS imported",
			nodeLabel, strings.Join(setParts, ", "),
		)
	} else {
		cypher = fmt.Sprintf(
			"UNWIND $rows AS row MERGE (n:%s) RETURN count(n) AS imported",
			nodeLabel,
		)
	}

	return cypher, map[string]any{"rows": rows}
}
