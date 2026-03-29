package catalog

import (
	"context"
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// DatasetRef identifies a dataset by name and platform.
type DatasetRef struct {
	Dataset  string
	Platform string
}

// LineageNode is a node in the lineage graph.
type LineageNode struct {
	Dataset  string `json:"dataset"`
	Platform string `json:"platform"`
	Depth    int    `json:"depth"`
}

// LineageEdge is a directed edge in the lineage graph.
type LineageEdge struct {
	From     string `json:"from"`
	To       string `json:"to"`
	Pipeline string `json:"pipeline"`
}

// LineageQueryResult holds the result of a lineage query.
type LineageQueryResult struct {
	Nodes []LineageNode
	Edges []LineageEdge
}

// LineageProvider is the interface for recording and querying dataset lineage.
type LineageProvider interface {
	RecordLineage(ctx context.Context, upstreams, downstreams []DatasetRef, pipeline string) error
	QueryLineage(ctx context.Context, dataset, direction string, depth int) (*LineageQueryResult, error)
}

// LookupLineageProvider returns the LineageProvider for the named catalog module.
func LookupLineageProvider(name string) (LineageProvider, error) {
	catMu.RLock()
	defer catMu.RUnlock()
	entry, ok := catModules[name]
	if !ok {
		return nil, fmt.Errorf("catalog: no module registered for %q", name)
	}
	if entry.dh != nil {
		return &dhLineageProvider{module: entry.dh}, nil
	}
	if entry.om != nil {
		return &omLineageProvider{module: entry.om}, nil
	}
	return nil, fmt.Errorf("catalog: module %q has no lineage provider", name)
}

// datasetToURN converts a DatasetRef to a DataHub URN.
func datasetToURN(ref DatasetRef) string {
	platform := ref.Platform
	if platform == "" {
		platform = "unknown"
	}
	return fmt.Sprintf("urn:li:dataset:(urn:li:dataPlatform:%s,%s,PROD)", platform, ref.Dataset)
}

// -- DataHub LineageProvider --

type dhLineageProvider struct {
	module *DataHubModule
}

func (p *dhLineageProvider) RecordLineage(ctx context.Context, upstreams, downstreams []DatasetRef, _ string) error {
	client := p.module.Client()
	if client == nil {
		return fmt.Errorf("catalog.datahub: not started")
	}
	for _, up := range upstreams {
		for _, down := range downstreams {
			if err := client.SetLineage(ctx, datasetToURN(up), datasetToURN(down)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *dhLineageProvider) QueryLineage(ctx context.Context, dataset, direction string, _ int) (*LineageQueryResult, error) {
	client := p.module.Client()
	if client == nil {
		return nil, fmt.Errorf("catalog.datahub: not started")
	}
	result, err := client.GetLineage(ctx, dataset, direction)
	if err != nil {
		return nil, err
	}
	var nodes []LineageNode
	for _, e := range result.Entities {
		nodes = append(nodes, LineageNode{
			Dataset:  e.Name,
			Platform: e.Platform,
			Depth:    1,
		})
	}
	return &LineageQueryResult{Nodes: nodes}, nil
}

// -- OpenMetadata LineageProvider --

type omLineageProvider struct {
	module *OpenMetadataModule
}

func (p *omLineageProvider) RecordLineage(ctx context.Context, upstreams, downstreams []DatasetRef, _ string) error {
	client := p.module.Client()
	if client == nil {
		return fmt.Errorf("catalog.openmetadata: not started")
	}
	for _, up := range upstreams {
		for _, down := range downstreams {
			from := EntityRef{FQN: up.Dataset, Type: "table"}
			to := EntityRef{FQN: down.Dataset, Type: "table"}
			if err := client.AddLineageEdge(ctx, from, to); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *omLineageProvider) QueryLineage(ctx context.Context, dataset, direction string, _ int) (*LineageQueryResult, error) {
	client := p.module.Client()
	if client == nil {
		return nil, fmt.Errorf("catalog.openmetadata: not started")
	}
	result, err := client.GetLineage(ctx, dataset)
	if err != nil {
		return nil, err
	}
	var nodes []LineageNode
	for _, t := range result.Upstream {
		if direction == "upstream" || direction == "both" || direction == "" {
			nodes = append(nodes, LineageNode{Dataset: t.FullyQualifiedName, Depth: 1})
		}
	}
	for _, t := range result.Downstream {
		if direction == "downstream" || direction == "both" || direction == "" {
			nodes = append(nodes, LineageNode{Dataset: t.FullyQualifiedName, Depth: 1})
		}
	}
	return &LineageQueryResult{Nodes: nodes}, nil
}

// -- step.catalog_lineage --

type catalogLineageStep struct {
	name string
}

// NewCatalogLineageStep creates a new step.catalog_lineage instance.
func NewCatalogLineageStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &catalogLineageStep{name: name}, nil
}

func (s *catalogLineageStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	catalogName, _ := strValCatalog(config, "catalog")
	if catalogName == "" {
		return nil, fmt.Errorf("step.catalog_lineage %q: catalog is required", s.name)
	}
	pipeline, _ := strValCatalog(config, "pipeline")

	upstreams := parseDatasetRefs(config, "upstream")
	downstreams := parseDatasetRefs(config, "downstream")

	provider, err := LookupLineageProvider(catalogName)
	if err != nil {
		return nil, fmt.Errorf("step.catalog_lineage %q: %w", s.name, err)
	}

	if err := provider.RecordLineage(ctx, upstreams, downstreams, pipeline); err != nil {
		return nil, fmt.Errorf("step.catalog_lineage %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":          "recorded",
		"upstreamCount":   len(upstreams),
		"downstreamCount": len(downstreams),
	}}, nil
}

// parseDatasetRefs extracts a []DatasetRef from config[key] which is []any of {dataset, platform}.
func parseDatasetRefs(config map[string]any, key string) []DatasetRef {
	raw, ok := config[key].([]any)
	if !ok {
		return nil
	}
	var refs []DatasetRef
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		ref := DatasetRef{}
		if v, ok := m["dataset"].(string); ok {
			ref.Dataset = v
		}
		if v, ok := m["platform"].(string); ok {
			ref.Platform = v
		}
		if ref.Dataset != "" {
			refs = append(refs, ref)
		}
	}
	return refs
}

// -- step.catalog_lineage_query --

type catalogLineageQueryStep struct {
	name string
}

// NewCatalogLineageQueryStep creates a new step.catalog_lineage_query instance.
func NewCatalogLineageQueryStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &catalogLineageQueryStep{name: name}, nil
}

func (s *catalogLineageQueryStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	catalogName, _ := strValCatalog(config, "catalog")
	if catalogName == "" {
		return nil, fmt.Errorf("step.catalog_lineage_query %q: catalog is required", s.name)
	}
	dataset, _ := strValCatalog(config, "dataset")
	if dataset == "" {
		return nil, fmt.Errorf("step.catalog_lineage_query %q: dataset is required", s.name)
	}
	direction, _ := strValCatalog(config, "direction")
	if direction == "" {
		direction = "both"
	}
	depth := 1
	if v, ok := config["depth"].(int); ok && v > 0 {
		depth = v
	}

	provider, err := LookupLineageProvider(catalogName)
	if err != nil {
		return nil, fmt.Errorf("step.catalog_lineage_query %q: %w", s.name, err)
	}

	result, err := provider.QueryLineage(ctx, dataset, direction, depth)
	if err != nil {
		return nil, fmt.Errorf("step.catalog_lineage_query %q: %w", s.name, err)
	}

	nodes := make([]map[string]any, 0, len(result.Nodes))
	for _, n := range result.Nodes {
		nodes = append(nodes, map[string]any{
			"dataset":  n.Dataset,
			"platform": n.Platform,
			"depth":    n.Depth,
		})
	}
	edges := make([]map[string]any, 0, len(result.Edges))
	for _, e := range result.Edges {
		edges = append(edges, map[string]any{
			"from":     e.From,
			"to":       e.To,
			"pipeline": e.Pipeline,
		})
	}

	return &sdk.StepResult{Output: map[string]any{
		"nodes": nodes,
		"edges": edges,
	}}, nil
}
