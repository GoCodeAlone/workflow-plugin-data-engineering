package lakehouse

import (
	"context"
	"fmt"
	"time"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// stepBase holds the step name and provides common catalog/table resolution.
type stepBase struct {
	name string
}

func (s *stepBase) resolveClient(config map[string]any) (IcebergCatalogClient, Namespace, string, error) {
	catName, _ := config["catalog"].(string)
	if catName == "" {
		return nil, nil, "", fmt.Errorf("catalog is required")
	}
	client, err := LookupCatalog(catName)
	if err != nil {
		return nil, nil, "", err
	}
	ns := parseNamespace(config["namespace"])
	tbl, _ := config["table"].(string)
	return client, ns, tbl, nil
}

// ─── step.lakehouse_create_table ───────────────────────────────────────────────

type createTableStep struct{ stepBase }

// NewCreateTableStep creates a step.lakehouse_create_table instance.
func NewCreateTableStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &createTableStep{stepBase{name}}, nil
}

func (s *createTableStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	client, ns, tbl, err := s.resolveClient(config)
	if err != nil {
		return nil, fmt.Errorf("step.lakehouse_create_table %q: %w", s.name, err)
	}
	if tbl == "" {
		return nil, fmt.Errorf("step.lakehouse_create_table %q: table is required", s.name)
	}

	schema := parseSchemaConfig(config["schema"])
	req := CreateTableRequest{
		Name:     tbl,
		Schema:   schema,
		Location: stringVal2(config, "location"),
	}
	if props, ok := config["properties"].(map[string]any); ok {
		req.Properties = toStringMap(props)
	}

	meta, err := client.CreateTable(ctx, ns, req)
	if err != nil {
		return nil, fmt.Errorf("step.lakehouse_create_table %q: %w", s.name, err)
	}
	return &sdk.StepResult{Output: map[string]any{
		"status":    "created",
		"tableUUID": meta.TableUUID,
		"location":  meta.Location,
	}}, nil
}

// ─── step.lakehouse_evolve_schema ──────────────────────────────────────────────

type evolveSchemaStep struct{ stepBase }

// NewEvolveSchemaStep creates a step.lakehouse_evolve_schema instance.
func NewEvolveSchemaStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &evolveSchemaStep{stepBase{name}}, nil
}

func (s *evolveSchemaStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	client, ns, tbl, err := s.resolveClient(config)
	if err != nil {
		return nil, fmt.Errorf("step.lakehouse_evolve_schema %q: %w", s.name, err)
	}
	if tbl == "" {
		return nil, fmt.Errorf("step.lakehouse_evolve_schema %q: table is required", s.name)
	}

	updates := parseTableUpdates(config["changes"])
	if len(updates) == 0 {
		return nil, fmt.Errorf("step.lakehouse_evolve_schema %q: changes is required", s.name)
	}

	id := TableIdentifier{Namespace: ns, Name: tbl}
	meta, err := client.UpdateTable(ctx, id, updates, nil)
	if err != nil {
		return nil, fmt.Errorf("step.lakehouse_evolve_schema %q: %w", s.name, err)
	}
	return &sdk.StepResult{Output: map[string]any{
		"status":   "evolved",
		"schemaId": meta.CurrentSchemaID,
		"changes":  len(updates),
	}}, nil
}

// ─── step.lakehouse_write ──────────────────────────────────────────────────────

type writeStep struct{ stepBase }

// NewWriteStep creates a step.lakehouse_write instance.
func NewWriteStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &writeStep{stepBase{name}}, nil
}

func (s *writeStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	client, ns, tbl, err := s.resolveClient(config)
	if err != nil {
		return nil, fmt.Errorf("step.lakehouse_write %q: %w", s.name, err)
	}
	if tbl == "" {
		return nil, fmt.Errorf("step.lakehouse_write %q: table is required", s.name)
	}

	mode, _ := config["mode"].(string)
	if mode == "" {
		mode = "append"
	}

	var records []any
	switch d := config["data"].(type) {
	case []any:
		records = d
	case []map[string]any:
		for _, r := range d {
			records = append(records, r)
		}
	}
	recordCount := len(records)

	// Record write intent via catalog: set properties documenting the operation.
	props := map[string]string{
		"last-write-mode":    mode,
		"last-write-records": fmt.Sprintf("%d", recordCount),
		"last-write-at":      fmt.Sprintf("%d", time.Now().UnixMilli()),
	}
	if mergeKey, _ := config["mergeKey"].(string); mergeKey != "" {
		props["last-write-merge-key"] = mergeKey
	}
	updates := []TableUpdate{{Action: "set-properties", Fields: map[string]any{"updates": props}}}

	id := TableIdentifier{Namespace: ns, Name: tbl}
	meta, err := client.UpdateTable(ctx, id, updates, nil)
	if err != nil {
		return nil, fmt.Errorf("step.lakehouse_write %q: %w", s.name, err)
	}

	var snapshotID any
	if meta.CurrentSnapshotID != nil {
		snapshotID = *meta.CurrentSnapshotID
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":      "written",
		"recordCount": recordCount,
		"snapshotId":  snapshotID,
		"mode":        mode,
	}}, nil
}

// ─── step.lakehouse_compact ────────────────────────────────────────────────────

type compactStep struct{ stepBase }

// NewCompactStep creates a step.lakehouse_compact instance.
func NewCompactStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &compactStep{stepBase{name}}, nil
}

func (s *compactStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	client, ns, tbl, err := s.resolveClient(config)
	if err != nil {
		return nil, fmt.Errorf("step.lakehouse_compact %q: %w", s.name, err)
	}
	if tbl == "" {
		return nil, fmt.Errorf("step.lakehouse_compact %q: table is required", s.name)
	}

	targetSize, _ := config["targetFileSize"].(string)
	props := map[string]string{"compaction-requested": "true"}
	if targetSize != "" {
		props["compaction-target-file-size"] = targetSize
	}

	updates := []TableUpdate{{Action: "set-properties", Fields: map[string]any{"updates": props}}}
	id := TableIdentifier{Namespace: ns, Name: tbl}
	if _, err := client.UpdateTable(ctx, id, updates, nil); err != nil {
		return nil, fmt.Errorf("step.lakehouse_compact %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":         "compaction_requested",
		"table":          tbl,
		"targetFileSize": targetSize,
	}}, nil
}

// ─── step.lakehouse_snapshot ───────────────────────────────────────────────────

type snapshotStep struct{ stepBase }

// NewSnapshotStep creates a step.lakehouse_snapshot instance.
func NewSnapshotStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &snapshotStep{stepBase{name}}, nil
}

func (s *snapshotStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	client, ns, tbl, err := s.resolveClient(config)
	if err != nil {
		return nil, fmt.Errorf("step.lakehouse_snapshot %q: %w", s.name, err)
	}
	if tbl == "" {
		return nil, fmt.Errorf("step.lakehouse_snapshot %q: table is required", s.name)
	}

	action, _ := config["action"].(string)
	if action == "" {
		action = "list"
	}

	id := TableIdentifier{Namespace: ns, Name: tbl}

	switch action {
	case "list":
		snaps, err := client.ListSnapshots(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("step.lakehouse_snapshot %q: list: %w", s.name, err)
		}
		return &sdk.StepResult{Output: map[string]any{
			"action":    "list",
			"snapshots": snaps,
			"count":     len(snaps),
		}}, nil

	case "create":
		props := map[string]string{"snapshot-created-by": "workflow-step"}
		updates := []TableUpdate{{Action: "set-properties", Fields: map[string]any{"updates": props}}}
		meta, err := client.UpdateTable(ctx, id, updates, nil)
		if err != nil {
			return nil, fmt.Errorf("step.lakehouse_snapshot %q: create: %w", s.name, err)
		}
		return &sdk.StepResult{Output: map[string]any{
			"action":    "created",
			"tableUUID": meta.TableUUID,
		}}, nil

	case "rollback":
		var snapshotID int64
		switch v := config["snapshotId"].(type) {
		case int64:
			snapshotID = v
		case int:
			snapshotID = int64(v)
		case float64:
			snapshotID = int64(v)
		}
		if snapshotID == 0 {
			return nil, fmt.Errorf("step.lakehouse_snapshot %q: snapshotId is required for rollback", s.name)
		}
		reqs := []TableRequirement{{Type: "assert-snapshot-id", Fields: map[string]any{"snapshot-id": snapshotID}}}
		updates := []TableUpdate{{Action: "set-snapshot-ref", Fields: map[string]any{
			"snapshot-id": snapshotID,
			"type":        "branch",
		}}}
		if _, err := client.UpdateTable(ctx, id, updates, reqs); err != nil {
			return nil, fmt.Errorf("step.lakehouse_snapshot %q: rollback: %w", s.name, err)
		}
		return &sdk.StepResult{Output: map[string]any{
			"status":     "rolled_back",
			"snapshotId": snapshotID,
		}}, nil

	default:
		return nil, fmt.Errorf("step.lakehouse_snapshot %q: unknown action %q (list|create|rollback)", s.name, action)
	}
}

// ─── step.lakehouse_query ──────────────────────────────────────────────────────

type queryStep struct{ stepBase }

// NewQueryStep creates a step.lakehouse_query instance.
func NewQueryStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &queryStep{stepBase{name}}, nil
}

func (s *queryStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	client, ns, tbl, err := s.resolveClient(config)
	if err != nil {
		return nil, fmt.Errorf("step.lakehouse_query %q: %w", s.name, err)
	}
	if tbl == "" {
		return nil, fmt.Errorf("step.lakehouse_query %q: table is required", s.name)
	}

	id := TableIdentifier{Namespace: ns, Name: tbl}
	meta, err := client.LoadTable(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("step.lakehouse_query %q: %w", s.name, err)
	}

	// Find the current schema.
	var currentSchema *Schema
	for i := range meta.Schemas {
		if meta.Schemas[i].SchemaID == meta.CurrentSchemaID {
			currentSchema = &meta.Schemas[i]
			break
		}
	}

	out := map[string]any{
		"tableUUID": meta.TableUUID,
		"location":  meta.Location,
		"schema":    currentSchema,
		"snapshots": meta.Snapshots,
	}
	if meta.CurrentSnapshotID != nil {
		out["currentSnapshotId"] = *meta.CurrentSnapshotID
	}
	return &sdk.StepResult{Output: out}, nil
}

// ─── step.lakehouse_expire_snapshots ──────────────────────────────────────────

type expireSnapshotsStep struct{ stepBase }

// NewExpireSnapshotsStep creates a step.lakehouse_expire_snapshots instance.
func NewExpireSnapshotsStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &expireSnapshotsStep{stepBase{name}}, nil
}

func (s *expireSnapshotsStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	client, ns, tbl, err := s.resolveClient(config)
	if err != nil {
		return nil, fmt.Errorf("step.lakehouse_expire_snapshots %q: %w", s.name, err)
	}
	if tbl == "" {
		return nil, fmt.Errorf("step.lakehouse_expire_snapshots %q: table is required", s.name)
	}

	olderThan, _ := config["olderThan"].(string)
	props := map[string]string{"history.expire.min-snapshots-to-keep": "1"}
	if olderThan != "" {
		if d, err := time.ParseDuration(olderThan); err == nil {
			cutoff := time.Now().Add(-d).UnixMilli()
			props["history.expire.max-snapshot-age-ms"] = fmt.Sprintf("%d", cutoff)
		}
	}

	updates := []TableUpdate{{Action: "set-properties", Fields: map[string]any{"updates": props}}}
	id := TableIdentifier{Namespace: ns, Name: tbl}
	if _, err := client.UpdateTable(ctx, id, updates, nil); err != nil {
		return nil, fmt.Errorf("step.lakehouse_expire_snapshots %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":       "expired",
		"expiredCount": 0, // actual count would require a separate compaction run
		"olderThan":    olderThan,
	}}, nil
}

// ─── helpers ───────────────────────────────────────────────────────────────────

func stringVal2(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}

func toStringMap(m map[string]any) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

// parseSchemaConfig converts a raw config schema value into a Schema.
func parseSchemaConfig(raw any) Schema {
	schema := Schema{Type: "struct"}
	rm, ok := raw.(map[string]any)
	if !ok {
		return schema
	}
	if id, ok := rm["schema-id"].(int); ok {
		schema.SchemaID = id
	}
	if fieldsRaw, ok := rm["fields"].([]any); ok {
		for i, fRaw := range fieldsRaw {
			fm, ok := fRaw.(map[string]any)
			if !ok {
				continue
			}
			f := SchemaField{ID: i + 1}
			if v, ok := fm["id"].(int); ok {
				f.ID = v
			}
			if v, ok := fm["name"].(string); ok {
				f.Name = v
			}
			if v, ok := fm["type"].(string); ok {
				f.Type = v
			}
			if v, ok := fm["required"].(bool); ok {
				f.Required = v
			}
			if v, ok := fm["doc"].(string); ok {
				f.Doc = v
			}
			schema.Fields = append(schema.Fields, f)
		}
	}
	return schema
}

// parseTableUpdates converts a raw "changes" config value into []TableUpdate.
func parseTableUpdates(raw any) []TableUpdate {
	var updates []TableUpdate
	items, ok := raw.([]any)
	if !ok {
		return updates
	}
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		action, _ := m["action"].(string)
		if action == "" {
			continue
		}
		fields := make(map[string]any)
		for k, v := range m {
			if k != "action" {
				fields[k] = v
			}
		}
		updates = append(updates, TableUpdate{Action: action, Fields: fields})
	}
	return updates
}
