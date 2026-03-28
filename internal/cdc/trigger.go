package cdc

import (
	"context"
	"fmt"
	"sync"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// Trigger implements sdk.TriggerInstance for trigger.cdc.
// It registers an EventHandler on the named cdc.source provider and fires the
// workflow callback for each matching CDC change event.
//
// The trigger filters events by table name and action (INSERT/UPDATE/DELETE) when
// configured. If tables or actions are empty, all events are forwarded.
type Trigger struct {
	mu     sync.RWMutex
	config TriggerConfig
	cb     sdk.TriggerCallback
	cancel context.CancelFunc
}

// TriggerConfig holds configuration for the CDC trigger.
type TriggerConfig struct {
	// SourceID references a running cdc.source module by source_id.
	SourceID string `json:"source_id" yaml:"source_id"`
	// Tables filters events to the given table names. Empty = all tables.
	Tables []string `json:"tables" yaml:"tables"`
	// Actions filters events by DML operation type (INSERT, UPDATE, DELETE). Empty = all.
	Actions []string `json:"actions" yaml:"actions"`
}

// NewTrigger creates a new CDC trigger instance.
func NewTrigger(config map[string]any, cb sdk.TriggerCallback) (sdk.TriggerInstance, error) {
	cfg, err := parseTriggerConfig(config)
	if err != nil {
		return nil, fmt.Errorf("trigger.cdc: %w", err)
	}
	return &Trigger{config: cfg, cb: cb}, nil
}

// Start registers an event handler on the running cdc.source module and begins
// forwarding matching CDC events to the workflow callback.
func (t *Trigger) Start(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.cancel != nil {
		return fmt.Errorf("trigger.cdc %q: already started", t.config.SourceID)
	}

	provider, err := LookupSource(t.config.SourceID)
	if err != nil {
		return fmt.Errorf("trigger.cdc %q: %w", t.config.SourceID, err)
	}

	_, cancel := context.WithCancel(ctx)
	t.cancel = cancel

	tableSet := toSet(t.config.Tables)
	actionSet := toSet(t.config.Actions)

	handler := func(sourceID string, event map[string]any) error {
		// Filter by table.
		if len(tableSet) > 0 {
			table, _ := event["table"].(string)
			if table == "" {
				table, _ = event["source"].(string)
			}
			if !tableSet[table] {
				return nil
			}
		}

		// Filter by action/operation type.
		if len(actionSet) > 0 {
			op, _ := event["op"].(string)
			if op == "" {
				op, _ = event["action"].(string)
			}
			if !actionSet[op] {
				return nil
			}
		}

		// Fire the workflow callback with the CDC event as trigger data.
		return t.cb("cdc.change", event)
	}

	return provider.RegisterEventHandler(t.config.SourceID, handler)
}

// Stop deregisters the event handler and halts event forwarding.
func (t *Trigger) Stop(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.cancel != nil {
		t.cancel()
		t.cancel = nil
	}

	// Clear the event handler on the provider to stop receiving events.
	provider, err := LookupSource(t.config.SourceID)
	if err != nil {
		// Provider may have been stopped already — this is not an error.
		return nil
	}
	// Deregister by setting a no-op handler.
	_ = provider.RegisterEventHandler(t.config.SourceID, func(_ string, _ map[string]any) error {
		return nil
	})
	return nil
}

func parseTriggerConfig(config map[string]any) (TriggerConfig, error) {
	var cfg TriggerConfig
	if v, ok := config["source_id"].(string); ok {
		cfg.SourceID = v
	}
	if cfg.SourceID == "" {
		return cfg, fmt.Errorf("source_id is required")
	}
	cfg.Tables = stringList(config, "tables")
	cfg.Actions = stringList(config, "actions")
	return cfg, nil
}

// stringList extracts a []string from a config map key.
func stringList(config map[string]any, key string) []string {
	switch t := config[key].(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, v := range t {
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// toSet converts a string slice to a set (map[string]bool) for O(1) lookups.
func toSet(ss []string) map[string]bool {
	if len(ss) == 0 {
		return nil
	}
	m := make(map[string]bool, len(ss))
	for _, s := range ss {
		m[s] = true
	}
	return m
}
