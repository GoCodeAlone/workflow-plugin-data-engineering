package catalog

import (
	"context"
	"fmt"
	"os"

	"go.yaml.in/yaml/v3"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// DataContract is the YAML schema for a data contract file.
type DataContract struct {
	Dataset  string              `yaml:"dataset"`
	Version  string              `yaml:"version"`
	Schema   []ContractField     `yaml:"schema"`
	Quality  []QualityCheck      `yaml:"quality"`
}

// ContractField describes a field in the contract schema.
type ContractField struct {
	Name     string `yaml:"name"`
	Type     string `yaml:"type"`
	Nullable bool   `yaml:"nullable"`
	Required bool   `yaml:"required"`
}

// QualityCheck is a simple SQL-based quality assertion in the contract.
type QualityCheck struct {
	Name  string `yaml:"name"`
	Query string `yaml:"query"` // must return 0 rows for passing
	Type  string `yaml:"type"`  // not_null, unique, custom
}

// ContractDB is an interface for executing SQL assertions during contract validation.
type ContractDB interface {
	QueryCount(ctx context.Context, query string) (int, error)
}

// -- step.contract_validate --

type contractValidateStep struct {
	name string
	// loadContract and dbLookup are injectable for testing
	loadContract func(path string) (*DataContract, error)
	lookupDB     func(name string) (ContractDB, error)
}

// NewContractValidateStep creates a new step.contract_validate instance.
func NewContractValidateStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &contractValidateStep{
		name:         name,
		loadContract: loadContractFromFile,
		lookupDB:     lookupContractDB,
	}, nil
}

func (s *contractValidateStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	contractPath, _ := strValCatalog(config, "contract")
	if contractPath == "" {
		return nil, fmt.Errorf("step.contract_validate %q: contract is required", s.name)
	}

	contract, err := s.loadContract(contractPath)
	if err != nil {
		return nil, fmt.Errorf("step.contract_validate %q: load contract: %w", s.name, err)
	}

	dbName, _ := strValCatalog(config, "database")

	var errs []string
	schemaOk := true
	qualityOk := true

	// Schema validation: verify all required fields are present
	for _, field := range contract.Schema {
		if field.Required && field.Name == "" {
			schemaOk = false
			errs = append(errs, fmt.Sprintf("schema: field missing name"))
		}
	}

	// Quality checks (optional: only run if database is configured)
	if dbName != "" {
		db, err := s.lookupDB(dbName)
		if err != nil {
			return nil, fmt.Errorf("step.contract_validate %q: %w", s.name, err)
		}
		for _, check := range contract.Quality {
			count, err := db.QueryCount(ctx, check.Query)
			if err != nil {
				qualityOk = false
				errs = append(errs, fmt.Sprintf("quality check %q: %v", check.Name, err))
				continue
			}
			if count > 0 {
				qualityOk = false
				errs = append(errs, fmt.Sprintf("quality check %q: found %d violations", check.Name, count))
			}
		}
	}

	passed := schemaOk && qualityOk

	return &sdk.StepResult{
		Output: map[string]any{
			"dataset":   contract.Dataset,
			"passed":    passed,
			"schemaOk":  schemaOk,
			"qualityOk": qualityOk,
			"errors":    errs,
		},
	}, nil
}

func loadContractFromFile(path string) (*DataContract, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file %q: %w", path, err)
	}
	var contract DataContract
	if err := yaml.Unmarshal(data, &contract); err != nil {
		return nil, fmt.Errorf("parse YAML %q: %w", path, err)
	}
	return &contract, nil
}

// contractDBRegistry is a global registry for ContractDB implementations.
var contractDBRegistry = map[string]ContractDB{}

// RegisterContractDB registers a ContractDB under a name for testing/wiring.
func RegisterContractDB(name string, db ContractDB) {
	contractDBRegistry[name] = db
}

// UnregisterContractDB removes a ContractDB from the registry.
func UnregisterContractDB(name string) {
	delete(contractDBRegistry, name)
}

func lookupContractDB(name string) (ContractDB, error) {
	db, ok := contractDBRegistry[name]
	if !ok {
		return nil, fmt.Errorf("contract_validate: no database %q registered", name)
	}
	return db, nil
}
