package catalog

import (
	"context"
	"fmt"
	"sync"
)

// CatalogProvider is the common interface for catalog backends (DataHub, OpenMetadata).
type CatalogProvider interface {
	RegisterDataset(ctx context.Context, req RegisterDatasetRequest) error
	SearchDatasets(ctx context.Context, query string, limit int) ([]CatalogDataset, int, error)
}

// RegisterDatasetRequest carries all fields needed to register a dataset.
type RegisterDatasetRequest struct {
	Dataset    string
	Schema     []SchemaField
	Owner      string
	Tags       []string
	Properties map[string]string
}

// CatalogDataset is a uniform dataset representation across catalog backends.
type CatalogDataset struct {
	Name     string `json:"name"`
	Platform string `json:"platform"`
	Owner    string `json:"owner"`
}

// -- DataHub CatalogProvider --

type datahubCatalogProvider struct {
	module *DataHubModule
}

func (p *datahubCatalogProvider) RegisterDataset(ctx context.Context, req RegisterDatasetRequest) error {
	client := p.module.Client()
	if client == nil {
		return fmt.Errorf("catalog.datahub: not started")
	}

	aspect := map[string]any{
		"name":        req.Dataset,
		"description": "",
	}
	if len(req.Properties) > 0 {
		aspect["customProperties"] = req.Properties
	}

	proposals := []MetadataProposal{
		{
			EntityURN:  "urn:li:dataset:" + req.Dataset,
			AspectName: "datasetProperties",
			Aspect:     aspect,
		},
	}

	if req.Owner != "" {
		if err := client.SetOwner(ctx, "urn:li:dataset:"+req.Dataset, req.Owner, "USER"); err != nil {
			return err
		}
	}
	for _, tag := range req.Tags {
		if err := client.AddTag(ctx, "urn:li:dataset:"+req.Dataset, tag); err != nil {
			return err
		}
	}

	return client.EmitMetadata(ctx, proposals)
}

func (p *datahubCatalogProvider) SearchDatasets(ctx context.Context, query string, limit int) ([]CatalogDataset, int, error) {
	client := p.module.Client()
	if client == nil {
		return nil, 0, fmt.Errorf("catalog.datahub: not started")
	}
	result, err := client.SearchDatasets(ctx, query, 0, limit)
	if err != nil {
		return nil, 0, err
	}
	datasets := make([]CatalogDataset, 0, len(result.Entities))
	for _, e := range result.Entities {
		datasets = append(datasets, CatalogDataset{
			Name:     e.Name,
			Platform: e.Platform,
			Owner:    e.Owner,
		})
	}
	return datasets, result.Total, nil
}

// -- OpenMetadata CatalogProvider --

type omCatalogProvider struct {
	module *OpenMetadataModule
}

func (p *omCatalogProvider) RegisterDataset(ctx context.Context, req RegisterDatasetRequest) error {
	client := p.module.Client()
	if client == nil {
		return fmt.Errorf("catalog.openmetadata: not started")
	}
	table := OMTable{
		Name: req.Dataset,
	}
	for _, t := range req.Tags {
		table.Tags = append(table.Tags, OMTag{TagFQN: t})
	}
	if req.Owner != "" {
		table.Owner = &OMEntityRef{Name: req.Owner, Type: "user"}
	}
	for _, f := range req.Schema {
		table.Columns = append(table.Columns, OMColumn{Name: f.Name, DataType: f.Type})
	}
	return client.CreateOrUpdateTable(ctx, table)
}

func (p *omCatalogProvider) SearchDatasets(ctx context.Context, query string, limit int) ([]CatalogDataset, int, error) {
	client := p.module.Client()
	if client == nil {
		return nil, 0, fmt.Errorf("catalog.openmetadata: not started")
	}
	result, err := client.SearchTables(ctx, query, limit)
	if err != nil {
		return nil, 0, err
	}
	datasets := make([]CatalogDataset, 0, len(result.Tables))
	for _, t := range result.Tables {
		owner := ""
		if t.Owner != nil {
			owner = t.Owner.Name
		}
		datasets = append(datasets, CatalogDataset{
			Name:  t.Name,
			Owner: owner,
		})
	}
	return datasets, result.Total, nil
}

// -- Module registry --

var (
	catMu      sync.RWMutex
	catModules = map[string]catalogModuleEntry{}
)

type catalogModuleEntry struct {
	dh *DataHubModule
	om *OpenMetadataModule
}

// RegisterCatalogModule registers a DataHub or OpenMetadata module.
func RegisterCatalogModule(name string, m any) error {
	catMu.Lock()
	defer catMu.Unlock()
	if _, exists := catModules[name]; exists {
		return fmt.Errorf("catalog: module %q already registered", name)
	}
	entry := catalogModuleEntry{}
	switch v := m.(type) {
	case *DataHubModule:
		entry.dh = v
	case *OpenMetadataModule:
		entry.om = v
	default:
		return fmt.Errorf("catalog: unsupported module type %T", m)
	}
	catModules[name] = entry
	return nil
}

// UnregisterCatalogModule removes a registered catalog module.
func UnregisterCatalogModule(name string) {
	catMu.Lock()
	defer catMu.Unlock()
	delete(catModules, name)
}

// LookupCatalogProvider returns the CatalogProvider for the given module name.
func LookupCatalogProvider(name string) (CatalogProvider, error) {
	catMu.RLock()
	defer catMu.RUnlock()
	entry, ok := catModules[name]
	if !ok {
		return nil, fmt.Errorf("catalog: no module registered for %q", name)
	}
	if entry.dh != nil {
		return entry.dh.CatalogProvider(), nil
	}
	if entry.om != nil {
		return entry.om.CatalogProvider(), nil
	}
	return nil, fmt.Errorf("catalog: module %q has no provider", name)
}
