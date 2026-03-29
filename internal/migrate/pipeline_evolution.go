package migrate

import (
	"context"
	"fmt"
)

// ─── Interfaces for pipeline evolution ────────────────────────────────────────

// CDCRestarter can stop and restart a CDC connector to pick up schema changes.
type CDCRestarter interface {
	Stop(ctx context.Context) error
	Start(ctx context.Context) error
}

// SchemaRegistryProvider registers schemas and checks compatibility.
// Also used by the verify step to read schema fields.
type SchemaRegistryProvider interface {
	RegisterSubject(ctx context.Context, subject, schema string) (int, error)
	CheckCompatibility(ctx context.Context, subject, schema string) (bool, error)
	GetSchemaFields(ctx context.Context, subject string) ([]string, int, error)
}

// LakehouseEvolver evolves an Iceberg table schema.
type LakehouseEvolver interface {
	EvolveTable(ctx context.Context, namespace []string, table string, change map[string]any) error
}

// ─── Plan types ───────────────────────────────────────────────────────────────

// ConnectorAction describes an action to take on a CDC connector.
type ConnectorAction struct {
	Action string `json:"action"` // "restart", "reconfigure"
	Name   string `json:"name"`
}

// SchemaAction describes an action to take in the schema registry.
type SchemaAction struct {
	Action        string `json:"action"`        // "register", "check_compat"
	Subject       string `json:"subject"`
	Schema        string `json:"schema"`
	Compatibility string `json:"compatibility"`
}

// TableUpdate describes a change to apply to an Iceberg table.
type TableUpdate struct {
	Action    string         `json:"action"` // "add_column", "widen_type"
	Table     string         `json:"table"`
	Namespace []string       `json:"namespace"`
	Changes   []map[string]any `json:"changes"`
}

// EvolutionStep is a single ordered step in a schema evolution plan.
type EvolutionStep struct {
	Target      string         `json:"target"`      // source_db, cdc_connector, schema_registry, lakehouse
	Action      string         `json:"action"`      // alter_table, restart_connector, register_schema, evolve_table
	SQL         string         `json:"sql,omitempty"`
	Config      map[string]any `json:"config,omitempty"`
	Rollback    string         `json:"rollback"`
	Description string         `json:"description"`
}

// SchemaEvolutionPlan coordinates a schema change across the full CDC pipeline.
type SchemaEvolutionPlan struct {
	SourceDB       SchemaChange   // DDL to apply in the source database
	CDCConnector   ConnectorAction
	SchemaRegistry SchemaAction
	Lakehouse      TableUpdate
	Safe           bool
	Steps          []EvolutionStep
}

// ─── Lookup registry (set by plugin.go for production wiring) ─────────────────

var (
	globalCDCLookup      func(string) (CDCRestarter, error)
	globalSRLookup       func(string) (SchemaRegistryProvider, error)
	globalLakehouseLookup func(string) (LakehouseEvolver, error)
	globalDBLookup       func(string) (SQLExecutor, error)
)

// SetCDCLookup registers the function used to resolve a CDC connector by module name.
func SetCDCLookup(fn func(string) (CDCRestarter, error)) { globalCDCLookup = fn }

// SetSchemaRegistryLookup registers the function used to resolve a schema registry module.
func SetSchemaRegistryLookup(fn func(string) (SchemaRegistryProvider, error)) { globalSRLookup = fn }

// SetLakehouseLookup registers the function used to resolve a lakehouse catalog module.
func SetLakehouseLookup(fn func(string) (LakehouseEvolver, error)) { globalLakehouseLookup = fn }

// SetDBLookup registers the function used to resolve a source DB executor by module name.
func SetDBLookup(fn func(string) (SQLExecutor, error)) { globalDBLookup = fn }

// ─── Plan building ─────────────────────────────────────────────────────────────

// parseEvolutionChange extracts a SchemaChange + metadata from the config "change" map.
func parseEvolutionChange(raw map[string]any) (SchemaChange, string, string, []string) {
	change := SchemaChange{}
	change.Type, _ = raw["type"].(string)
	change.Description, _ = raw["description"].(string)
	change.SQL, _ = raw["sql"].(string)
	safe, _ := raw["safe"].(bool)
	change.Breaking = !safe
	subject, _ := raw["subject"].(string)
	schema, _ := raw["schema"].(string)

	var icebergChanges []string
	if v, ok := raw["icebergFields"].([]any); ok {
		for _, f := range v {
			if s, ok := f.(string); ok {
				icebergChanges = append(icebergChanges, s)
			}
		}
	}

	return change, subject, schema, icebergChanges
}

// BuildEvolutionPlan constructs a SchemaEvolutionPlan from a change description.
// It does not execute anything — callers execute via executeEvolutionPlan.
func BuildEvolutionPlan(table, namespace string, change SchemaChange, subject, schema string, icebergFields []string) *SchemaEvolutionPlan {
	safe := !change.Breaking

	plan := &SchemaEvolutionPlan{
		SourceDB: change,
		CDCConnector: ConnectorAction{
			Action: "restart",
		},
		SchemaRegistry: SchemaAction{
			Action:  "register",
			Subject: subject,
			Schema:  schema,
		},
		Lakehouse: TableUpdate{
			Action:    change.Type,
			Table:     table,
			Namespace: []string{namespace},
		},
		Safe: safe,
	}

	// Build ordered execution steps.
	plan.Steps = []EvolutionStep{
		{
			Target:      "schema_registry",
			Action:      "check_compat",
			Config:      map[string]any{"subject": subject, "schema": schema},
			Rollback:    "n/a — read-only compatibility check",
			Description: "validate schema change is compatible in schema registry",
		},
		{
			Target:      "schema_registry",
			Action:      "register_schema",
			Config:      map[string]any{"subject": subject, "schema": schema},
			Rollback:    fmt.Sprintf("delete registered schema version for subject %q", subject),
			Description: fmt.Sprintf("register new schema version for subject %q", subject),
		},
		{
			Target:      "lakehouse",
			Action:      "evolve_table",
			Config:      map[string]any{"table": table, "namespace": namespace, "change": change.Type},
			Rollback:    fmt.Sprintf("revert Iceberg schema for table %q", table),
			Description: fmt.Sprintf("evolve Iceberg table schema: %s", change.Description),
		},
		{
			Target:      "source_db",
			Action:      "alter_table",
			SQL:         change.SQL,
			Rollback:    deriveRollbackSQL(change),
			Description: change.Description,
		},
		{
			Target:      "cdc_connector",
			Action:      "restart_connector",
			Rollback:    "restart CDC connector to previous configuration",
			Description: "restart CDC connector to pick up updated source schema",
		},
	}

	return plan
}

// deriveRollbackSQL generates a best-effort rollback SQL for a schema change.
func deriveRollbackSQL(ch SchemaChange) string {
	if ch.Breaking {
		return "manual rollback required — breaking change"
	}
	return fmt.Sprintf("-- reverse of: %s", ch.SQL)
}

// executeEvolutionPlan runs the plan steps in order using the provided connectors.
// Any nil connector skips that step.
func executeEvolutionPlan(
	ctx context.Context,
	plan *SchemaEvolutionPlan,
	db SQLExecutor,
	cdc CDCRestarter,
	sr SchemaRegistryProvider,
	lh LakehouseEvolver,
) error {
	// Step 1: compatibility check
	if sr != nil && plan.SchemaRegistry.Subject != "" && plan.SchemaRegistry.Schema != "" {
		compat, err := sr.CheckCompatibility(ctx, plan.SchemaRegistry.Subject, plan.SchemaRegistry.Schema)
		if err != nil {
			return fmt.Errorf("schema_evolve_pipeline: compatibility check: %w", err)
		}
		if !compat {
			return fmt.Errorf("schema_evolve_pipeline: schema change is incompatible with subject %q", plan.SchemaRegistry.Subject)
		}
	}

	// Step 2: register schema
	if sr != nil && plan.SchemaRegistry.Subject != "" && plan.SchemaRegistry.Schema != "" {
		if _, err := sr.RegisterSubject(ctx, plan.SchemaRegistry.Subject, plan.SchemaRegistry.Schema); err != nil {
			return fmt.Errorf("schema_evolve_pipeline: register schema: %w", err)
		}
	}

	// Step 3: evolve lakehouse table
	if lh != nil {
		change := map[string]any{
			"type":   plan.SourceDB.Type,
			"sql":    plan.SourceDB.SQL,
			"safe":   plan.Safe,
		}
		if err := lh.EvolveTable(ctx, plan.Lakehouse.Namespace, plan.Lakehouse.Table, change); err != nil {
			return fmt.Errorf("schema_evolve_pipeline: evolve lakehouse table: %w", err)
		}
	}

	// Step 4: apply DDL to source DB
	if db != nil && plan.SourceDB.SQL != "" {
		if _, err := db.ExecContext(ctx, plan.SourceDB.SQL); err != nil {
			return fmt.Errorf("schema_evolve_pipeline: apply source DDL: %w", err)
		}
	}

	// Step 5: restart CDC connector
	if cdc != nil {
		if err := cdc.Stop(ctx); err != nil {
			return fmt.Errorf("schema_evolve_pipeline: stop CDC connector: %w", err)
		}
		if err := cdc.Start(ctx); err != nil {
			return fmt.Errorf("schema_evolve_pipeline: start CDC connector: %w", err)
		}
	}

	return nil
}

// stepsToOutput converts EvolutionStep slice to []map[string]any for step output.
func stepsToOutput(steps []EvolutionStep) []map[string]any {
	out := make([]map[string]any, len(steps))
	for i, s := range steps {
		out[i] = map[string]any{
			"target":      s.Target,
			"action":      s.Action,
			"sql":         s.SQL,
			"rollback":    s.Rollback,
			"description": s.Description,
		}
	}
	return out
}
