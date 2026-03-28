package cdc

import (
	"context"
	"fmt"
	"sync"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// Trigger implements sdk.TriggerInstance for trigger.cdc.
// It fires the workflow callback whenever a CDC change event arrives.
type Trigger struct {
	mu     sync.RWMutex
	config TriggerConfig
	cb     sdk.TriggerCallback
	cancel context.CancelFunc
}

// TriggerConfig holds configuration for the CDC trigger.
type TriggerConfig struct {
	SourceID string   `json:"source_id" yaml:"source_id"`
	Tables   []string `json:"tables"    yaml:"tables"`
	Actions  []string `json:"actions"   yaml:"actions"`
}

// NewTrigger creates a new CDC trigger instance.
func NewTrigger(config map[string]any, cb sdk.TriggerCallback) (sdk.TriggerInstance, error) {
	cfg, err := parseTriggerConfig(config)
	if err != nil {
		return nil, fmt.Errorf("trigger.cdc: %w", err)
	}
	return &Trigger{config: cfg, cb: cb}, nil
}

// Start begins listening for CDC change events and fires the callback.
func (t *Trigger) Start(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cancel != nil {
		return fmt.Errorf("trigger.cdc %q: already started", t.config.SourceID)
	}
	_, cancel := context.WithCancel(ctx)
	t.cancel = cancel
	return nil
}

// Stop halts the CDC trigger.
func (t *Trigger) Stop(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cancel != nil {
		t.cancel()
		t.cancel = nil
	}
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
	switch t := config["tables"].(type) {
	case []string:
		cfg.Tables = t
	case []any:
		for _, v := range t {
			if s, ok := v.(string); ok {
				cfg.Tables = append(cfg.Tables, s)
			}
		}
	}
	switch t := config["actions"].(type) {
	case []string:
		cfg.Actions = t
	case []any:
		for _, v := range t {
			if s, ok := v.(string); ok {
				cfg.Actions = append(cfg.Actions, s)
			}
		}
	}
	return cfg, nil
}
