package tenancy

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// ─── step.tenant_provision ───────────────────────────────────────────────────

// provisionStep implements step.tenant_provision — creates a namespace for a new tenant.
type provisionStep struct {
	name     string
	strategy TenancyStrategy
}

// NewProvisionStep creates a step.tenant_provision instance.
// Config must include "strategy" and any strategy-specific fields.
func NewProvisionStep(name string, config map[string]any) (sdk.StepInstance, error) {
	strategy, err := strategyFromConfig(config, noopSQLExecutor)
	if err != nil {
		return nil, fmt.Errorf("step.tenant_provision %q: %w", name, err)
	}
	return &provisionStep{name: name, strategy: strategy}, nil
}

func (s *provisionStep) Execute(
	ctx context.Context,
	_ map[string]any,
	_ map[string]map[string]any,
	_ map[string]any,
	_ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	tenantID, _ := stringVal(config, "tenant_id")
	if tenantID == "" {
		return nil, fmt.Errorf("step.tenant_provision %q: tenant_id is required", s.name)
	}
	if err := s.strategy.ProvisionTenant(ctx, tenantID); err != nil {
		return nil, fmt.Errorf("step.tenant_provision %q: %w", s.name, err)
	}
	return &sdk.StepResult{Output: map[string]any{
		"status":    "provisioned",
		"tenant_id": tenantID,
	}}, nil
}

// ─── step.tenant_deprovision ─────────────────────────────────────────────────

// deprovisionStep implements step.tenant_deprovision — archives or deletes a tenant namespace.
type deprovisionStep struct {
	name     string
	strategy TenancyStrategy
}

// NewDeprovisionStep creates a step.tenant_deprovision instance.
func NewDeprovisionStep(name string, config map[string]any) (sdk.StepInstance, error) {
	strategy, err := strategyFromConfig(config, noopSQLExecutor)
	if err != nil {
		return nil, fmt.Errorf("step.tenant_deprovision %q: %w", name, err)
	}
	return &deprovisionStep{name: name, strategy: strategy}, nil
}

func (s *deprovisionStep) Execute(
	ctx context.Context,
	_ map[string]any,
	_ map[string]map[string]any,
	_ map[string]any,
	_ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	tenantID, _ := stringVal(config, "tenant_id")
	if tenantID == "" {
		return nil, fmt.Errorf("step.tenant_deprovision %q: tenant_id is required", s.name)
	}
	mode, _ := stringVal(config, "mode")
	if mode == "" {
		return nil, fmt.Errorf("step.tenant_deprovision %q: mode is required (archive or delete)", s.name)
	}
	if err := s.strategy.DeprovisionTenant(ctx, tenantID, mode); err != nil {
		return nil, fmt.Errorf("step.tenant_deprovision %q: %w", s.name, err)
	}
	return &sdk.StepResult{Output: map[string]any{
		"status":    "deprovisioned",
		"tenant_id": tenantID,
		"mode":      mode,
	}}, nil
}

// ─── step.tenant_migrate ─────────────────────────────────────────────────────

// migrateStep implements step.tenant_migrate — runs migrations for a list of tenants.
// Uses a goroutine pool (semaphore) with a consecutive-failure circuit breaker.
type migrateStep struct {
	name     string
	strategy TenancyStrategy
}

// NewMigrateStep creates a step.tenant_migrate instance.
func NewMigrateStep(name string, config map[string]any) (sdk.StepInstance, error) {
	strategy, err := strategyFromConfig(config, noopSQLExecutor)
	if err != nil {
		return nil, fmt.Errorf("step.tenant_migrate %q: %w", name, err)
	}
	return &migrateStep{name: name, strategy: strategy}, nil
}

func (s *migrateStep) Execute(
	ctx context.Context,
	_ map[string]any,
	_ map[string]map[string]any,
	_ map[string]any,
	_ map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	tenantIDs := listVal(config, "tenant_ids")
	if len(tenantIDs) == 0 {
		return nil, fmt.Errorf("step.tenant_migrate %q: tenant_ids is required", s.name)
	}
	parallelism := intVal(config, "parallelism", 5)
	if parallelism < 1 {
		parallelism = 1
	}
	failureThreshold := intVal(config, "failure_threshold", 0)

	sem := make(chan struct{}, parallelism)

	var (
		mu               sync.Mutex
		failed           []string
		consecutiveFails atomic.Int64
		circuitOpen      atomic.Bool
	)

	var wg sync.WaitGroup
	for _, tenantID := range tenantIDs {
		// Acquire semaphore slot first — blocks until a worker is free.
		// This ensures we see the latest circuit state (updated by goroutines that
		// just finished) before deciding whether to dispatch more work.
		sem <- struct{}{}

		// Re-check circuit breaker after acquiring the slot, so we catch state
		// updated by the goroutine that just freed this slot.
		if failureThreshold > 0 && circuitOpen.Load() {
			<-sem // release slot we just acquired
			break
		}

		wg.Add(1)
		go func(tid string) {
			defer wg.Done()
			defer func() { <-sem }()

			err := s.strategy.ProvisionTenant(ctx, tid)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failed = append(failed, tid)
				n := consecutiveFails.Add(1)
				if failureThreshold > 0 && n >= int64(failureThreshold) {
					circuitOpen.Store(true)
				}
			} else {
				consecutiveFails.Store(0)
			}
		}(tenantID)
	}
	wg.Wait()

	status := "completed"
	if circuitOpen.Load() {
		status = "stopped"
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":  status,
		"tenants": tenantIDs,
		"count":   len(tenantIDs) - len(failed),
		"failed":  failed,
	}}, nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// stringVal extracts a string value from a map by key.
func stringVal(m map[string]any, key string) (string, bool) {
	v, ok := m[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// intVal extracts an int from a map, with a default fallback.
func intVal(m map[string]any, key string, def int) int {
	v, ok := m[key]
	if !ok {
		return def
	}
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return def
}

// listVal extracts a []string from a map key that holds []any or []string.
func listVal(m map[string]any, key string) []string {
	v, ok := m[key]
	if !ok {
		return nil
	}
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}
