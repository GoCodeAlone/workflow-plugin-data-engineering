package catalog

import (
	"context"
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// -- step.catalog_register --

type catalogRegisterStep struct {
	name string
}

// NewCatalogRegisterStep creates a new step.catalog_register instance.
func NewCatalogRegisterStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &catalogRegisterStep{name: name}, nil
}

func (s *catalogRegisterStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	catalogName, _ := strValCatalog(config, "catalog")
	if catalogName == "" {
		return nil, fmt.Errorf("step.catalog_register %q: catalog is required", s.name)
	}
	dataset, _ := strValCatalog(config, "dataset")
	if dataset == "" {
		return nil, fmt.Errorf("step.catalog_register %q: dataset is required", s.name)
	}

	provider, err := LookupCatalogProvider(catalogName)
	if err != nil {
		return nil, fmt.Errorf("step.catalog_register %q: %w", s.name, err)
	}

	req := RegisterDatasetRequest{
		Dataset: dataset,
	}
	if owner, ok := strValCatalog(config, "owner"); ok {
		req.Owner = owner
	}
	if tags, ok := config["tags"].([]any); ok {
		for _, t := range tags {
			if ts, ok := t.(string); ok {
				req.Tags = append(req.Tags, ts)
			}
		}
	}
	if props, ok := config["properties"].(map[string]any); ok {
		req.Properties = make(map[string]string)
		for k, v := range props {
			if vs, ok := v.(string); ok {
				req.Properties[k] = vs
			}
		}
	}
	if schema, ok := config["schema"].([]any); ok {
		for _, f := range schema {
			if fm, ok := f.(map[string]any); ok {
				field := SchemaField{}
				if n, ok := fm["name"].(string); ok {
					field.Name = n
				}
				if t, ok := fm["type"].(string); ok {
					field.Type = t
				}
				req.Schema = append(req.Schema, field)
			}
		}
	}

	if err := provider.RegisterDataset(ctx, req); err != nil {
		return nil, fmt.Errorf("step.catalog_register %q: %w", s.name, err)
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"status":  "registered",
			"dataset": dataset,
			"catalog": catalogName,
		},
	}, nil
}

// -- step.catalog_search --

type catalogSearchStep struct {
	name string
}

// NewCatalogSearchStep creates a new step.catalog_search instance.
func NewCatalogSearchStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &catalogSearchStep{name: name}, nil
}

func (s *catalogSearchStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	catalogName, _ := strValCatalog(config, "catalog")
	if catalogName == "" {
		return nil, fmt.Errorf("step.catalog_search %q: catalog is required", s.name)
	}
	query, _ := strValCatalog(config, "query")
	if query == "" {
		return nil, fmt.Errorf("step.catalog_search %q: query is required", s.name)
	}
	limit := 10
	if v, ok := config["limit"].(int); ok && v > 0 {
		limit = v
	}

	provider, err := LookupCatalogProvider(catalogName)
	if err != nil {
		return nil, fmt.Errorf("step.catalog_search %q: %w", s.name, err)
	}

	datasets, total, err := provider.SearchDatasets(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("step.catalog_search %q: %w", s.name, err)
	}

	results := make([]map[string]any, 0, len(datasets))
	for _, d := range datasets {
		results = append(results, map[string]any{
			"name":     d.Name,
			"platform": d.Platform,
			"owner":    d.Owner,
		})
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"results": results,
			"total":   total,
		},
	}, nil
}
