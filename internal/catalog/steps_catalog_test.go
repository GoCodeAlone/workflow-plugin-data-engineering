package catalog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// -- Mock CatalogProvider --

type mockCatalogProvider struct {
	registerErr error
	searchFunc  func(ctx context.Context, query string, limit int) ([]CatalogDataset, int, error)
}

func (m *mockCatalogProvider) RegisterDataset(_ context.Context, _ RegisterDatasetRequest) error {
	return m.registerErr
}

func (m *mockCatalogProvider) SearchDatasets(ctx context.Context, query string, limit int) ([]CatalogDataset, int, error) {
	if m.searchFunc != nil {
		return m.searchFunc(ctx, query, limit)
	}
	return nil, 0, nil
}

// registerMockProvider registers a mock catalog provider for testing.
func registerMockProvider(t *testing.T, name string, provider CatalogProvider) {
	t.Helper()
	// We store a DataHubModule with an overridden provider using injection
	// Instead, we inject directly via the catalog module mechanism.
	// Use a stub DataHubModule with injected provider for simplicity.
	mod := &DataHubModule{
		name: name,
	}
	if err := RegisterCatalogModule(name, mod); err != nil {
		t.Fatalf("register mock catalog: %v", err)
	}
	// Override CatalogProvider by patching the entry in catModules
	catMu.Lock()
	catModules[name] = catalogModuleEntry{dh: &DataHubModule{
		name:   name,
		client: &mockDHClient{provider: provider},
	}}
	catMu.Unlock()
	t.Cleanup(func() { UnregisterCatalogModule(name) })
}

// mockDHClient is a DataHubClient that delegates to a CatalogProvider for testing.
type mockDHClient struct {
	provider CatalogProvider
	// Track calls for assertions
	registerReq *RegisterDatasetRequest
}

func (m *mockDHClient) GetDataset(_ context.Context, _ string) (*Dataset, error) {
	return &Dataset{URN: "urn:test", Name: "test"}, nil
}

func (m *mockDHClient) SearchDatasets(ctx context.Context, query string, start, count int) (*SearchResult, error) {
	datasets, total, err := m.provider.SearchDatasets(ctx, query, count)
	if err != nil {
		return nil, err
	}
	result := &SearchResult{Total: total, Count: len(datasets)}
	for _, d := range datasets {
		result.Entities = append(result.Entities, Dataset{Name: d.Name, Platform: d.Platform, Owner: d.Owner})
	}
	return result, nil
}

func (m *mockDHClient) EmitMetadata(_ context.Context, _ []MetadataProposal) error {
	return nil
}

func (m *mockDHClient) AddTag(_ context.Context, _, _ string) error { return nil }
func (m *mockDHClient) AddGlossaryTerm(_ context.Context, _, _ string) error { return nil }
func (m *mockDHClient) SetOwner(_ context.Context, _, _, _ string) error { return nil }
func (m *mockDHClient) SetLineage(_ context.Context, _, _ string) error { return nil }
func (m *mockDHClient) GetLineage(_ context.Context, urn, direction string) (*LineageResult, error) {
	return &LineageResult{URN: urn, Direction: direction}, nil
}

// -- Steps tests --

func TestCatalogRegister_DataHub(t *testing.T) {
	// Use a real DataHubModule with a mock HTTP server
	provider := &mockCatalogProvider{}
	registerMockProvider(t, "dh_reg", provider)

	step, _ := NewCatalogRegisterStep("reg1", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"catalog": "dh_reg",
		"dataset": "orders",
		"owner":   "alice",
		"tags":    []any{"pii", "finance"},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "registered" {
		t.Errorf("expected status=registered, got %v", result.Output["status"])
	}
	if result.Output["dataset"] != "orders" {
		t.Errorf("expected dataset=orders, got %v", result.Output["dataset"])
	}
}

func TestCatalogRegister_OpenMetadata(t *testing.T) {
	// Register an OpenMetadata module directly
	mod := &OpenMetadataModule{
		name: "om_reg",
		client: &mockOMClient{},
	}
	if err := RegisterCatalogModule("om_reg", mod); err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { UnregisterCatalogModule("om_reg") })

	step, _ := NewCatalogRegisterStep("reg2", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"catalog": "om_reg",
		"dataset": "events",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != "registered" {
		t.Errorf("expected status=registered, got %v", result.Output["status"])
	}
}

func TestCatalogSearch(t *testing.T) {
	provider := &mockCatalogProvider{
		searchFunc: func(_ context.Context, query string, limit int) ([]CatalogDataset, int, error) {
			return []CatalogDataset{
				{Name: "orders", Platform: "mysql", Owner: "alice"},
			}, 1, nil
		},
	}
	registerMockProvider(t, "dh_srch", provider)

	step, _ := NewCatalogSearchStep("srch1", nil)
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"catalog": "dh_srch",
		"query":   "orders",
		"limit":   5,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["total"] != 1 {
		t.Errorf("expected total=1, got %v", result.Output["total"])
	}
	results, ok := result.Output["results"].([]map[string]any)
	if !ok || len(results) != 1 {
		t.Errorf("expected 1 result, got %v", result.Output["results"])
	}
}

func TestCatalogSearch_MissingCatalog(t *testing.T) {
	step, _ := NewCatalogSearchStep("srch_bad", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"query": "test",
	})
	if err == nil {
		t.Fatal("expected error for missing catalog")
	}
}

// -- mockOMClient for OpenMetadata tests --

type mockOMClient struct{}

func (m *mockOMClient) GetTable(_ context.Context, _ string) (*OMTable, error) {
	return &OMTable{ID: "t1", Name: "test"}, nil
}
func (m *mockOMClient) SearchTables(_ context.Context, _ string, _ int) (*OMSearchResult, error) {
	return &OMSearchResult{}, nil
}
func (m *mockOMClient) CreateOrUpdateTable(_ context.Context, _ OMTable) error     { return nil }
func (m *mockOMClient) AddTag(_ context.Context, _, _ string) error                { return nil }
func (m *mockOMClient) SetOwner(_ context.Context, _, _ string) error              { return nil }
func (m *mockOMClient) AddLineageEdge(_ context.Context, _, _ EntityRef) error     { return nil }
func (m *mockOMClient) GetLineage(_ context.Context, _ string) (*OMLineageResult, error) {
	return &OMLineageResult{}, nil
}

// -- Contract validation tests --

func writeContractFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "contract.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write contract file: %v", err)
	}
	return path
}

const passingContract = `
dataset: orders
version: "1.0"
schema:
  - name: id
    type: integer
    required: true
  - name: amount
    type: decimal
    nullable: false
quality: []
`

const failingContract = `
dataset: orders
version: "1.0"
schema:
  - name: id
    type: integer
    required: true
quality:
  - name: no_nulls_in_amount
    query: "SELECT COUNT(*) FROM orders WHERE amount IS NULL"
    type: not_null
`

func TestContractValidate_Pass(t *testing.T) {
	path := writeContractFile(t, passingContract)

	step := &contractValidateStep{
		name:         "cv_pass",
		loadContract: loadContractFromFile,
		lookupDB:     lookupContractDB,
	}
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"contract": path,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["passed"] != true {
		t.Errorf("expected passed=true, got %v", result.Output["passed"])
	}
	if result.Output["schemaOk"] != true {
		t.Errorf("expected schemaOk=true")
	}
}

func TestContractValidate_Fail(t *testing.T) {
	path := writeContractFile(t, failingContract)

	// Register a mock DB that returns violations
	mockDB := &mockContractDB{count: 3}
	RegisterContractDB("testdb", mockDB)
	defer UnregisterContractDB("testdb")

	step := &contractValidateStep{
		name:         "cv_fail",
		loadContract: loadContractFromFile,
		lookupDB:     lookupContractDB,
	}
	result, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"contract": path,
		"database": "testdb",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["passed"] != false {
		t.Errorf("expected passed=false, got %v", result.Output["passed"])
	}
	if result.Output["qualityOk"] != false {
		t.Errorf("expected qualityOk=false")
	}
	errs, _ := result.Output["errors"].([]string)
	if len(errs) == 0 {
		t.Error("expected at least one error")
	}
}

func TestContractValidate_MissingFile(t *testing.T) {
	step, _ := NewContractValidateStep("cv_missing", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{
		"contract": "/nonexistent/contract.yaml",
	})
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestContractValidate_MissingContract(t *testing.T) {
	step, _ := NewContractValidateStep("cv_nopath", nil)
	_, err := step.Execute(context.Background(), nil, nil, nil, nil, map[string]any{})
	if err == nil {
		t.Fatal("expected error for missing contract path")
	}
}

// -- mockContractDB --

type mockContractDB struct {
	count int
	err   error
}

func (m *mockContractDB) QueryCount(_ context.Context, _ string) (int, error) {
	return m.count, m.err
}
