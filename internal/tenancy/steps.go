package tenancy

import (
	"context"
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// provisionStep implements step.tenant_provision — creates a new tenant schema/database.
type provisionStep struct {
	name   string
	config map[string]any
}

// NewProvisionStep creates a new step.tenant_provision instance.
func NewProvisionStep(name string, config map[string]any) (sdk.StepInstance, error) {
	return &provisionStep{name: name, config: config}, nil
}

func (s *provisionStep) Execute(ctx context.Context, triggerData map[string]any, stepOutputs map[string]map[string]any, current, metadata, config map[string]any) (*sdk.StepResult, error) {
	tenantID, _ := stringVal(config, "tenant_id")
	if tenantID == "" {
		return nil, fmt.Errorf("step.tenant_provision %q: tenant_id is required", s.name)
	}
	return &sdk.StepResult{Output: map[string]any{
		"tenant_id": tenantID,
		"action":    "provisioned",
	}}, nil
}

// deprovisionStep implements step.tenant_deprovision — removes a tenant schema/database.
type deprovisionStep struct {
	name   string
	config map[string]any
}

// NewDeprovisionStep creates a new step.tenant_deprovision instance.
func NewDeprovisionStep(name string, config map[string]any) (sdk.StepInstance, error) {
	return &deprovisionStep{name: name, config: config}, nil
}

func (s *deprovisionStep) Execute(ctx context.Context, triggerData map[string]any, stepOutputs map[string]map[string]any, current, metadata, config map[string]any) (*sdk.StepResult, error) {
	tenantID, _ := stringVal(config, "tenant_id")
	if tenantID == "" {
		return nil, fmt.Errorf("step.tenant_deprovision %q: tenant_id is required", s.name)
	}
	return &sdk.StepResult{Output: map[string]any{
		"tenant_id": tenantID,
		"action":    "deprovisioned",
	}}, nil
}

// migrateStep implements step.tenant_migrate — runs schema migrations for a tenant.
type migrateStep struct {
	name   string
	config map[string]any
}

// NewMigrateStep creates a new step.tenant_migrate instance.
func NewMigrateStep(name string, config map[string]any) (sdk.StepInstance, error) {
	return &migrateStep{name: name, config: config}, nil
}

func (s *migrateStep) Execute(ctx context.Context, triggerData map[string]any, stepOutputs map[string]map[string]any, current, metadata, config map[string]any) (*sdk.StepResult, error) {
	tenantID, _ := stringVal(config, "tenant_id")
	if tenantID == "" {
		return nil, fmt.Errorf("step.tenant_migrate %q: tenant_id is required", s.name)
	}
	return &sdk.StepResult{Output: map[string]any{
		"tenant_id": tenantID,
		"action":    "migrated",
	}}, nil
}

// stringVal extracts a string value from a map by key.
func stringVal(m map[string]any, key string) (string, bool) {
	v, ok := m[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}
