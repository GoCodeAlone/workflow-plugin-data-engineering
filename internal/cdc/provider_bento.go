package cdc

import (
	"context"
	"fmt"
	"sync"
)

// BentoProvider implements CDCProvider by generating Bento input YAML configs
// and delegating actual stream management to the workflow-plugin-bento engine module.
//
// The data-engineering plugin no longer manages Bento stream lifecycle in-process.
// Instead, the engine config declares a bento.input module (referenced via options.bento_module)
// alongside the cdc.source module. The Bento module handles stream execution and publishes
// events to the engine's EventBus, which the cdc trigger consumes.
type BentoProvider struct {
	mu      sync.RWMutex
	configs map[string]*bentoCDCConfig
}

// bentoCDCConfig holds the generated config and delegation reference for a CDC source.
type bentoCDCConfig struct {
	sourceConfig SourceConfig
	bentoYAML    string // generated Bento input YAML fragment
	state        string // "configured" or "snapshot"
	bentoModule  string // name of the bento.input engine module to delegate to
	handler      EventHandler
}

// newBentoProvider creates a new BentoProvider.
func newBentoProvider() *BentoProvider {
	return &BentoProvider{
		configs: make(map[string]*bentoCDCConfig),
	}
}

// Connect generates the Bento CDC input YAML and stores it for delegation.
// Actual stream management is handled by the bento.input engine module referenced
// in config.Options["bento_module"].
func (p *BentoProvider) Connect(_ context.Context, config SourceConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.configs[config.SourceID]; exists {
		return fmt.Errorf("bento CDC provider: source %q already configured", config.SourceID)
	}

	inputYAML, err := buildBentoInputYAML(config)
	if err != nil {
		return fmt.Errorf("bento CDC provider %q: build input config: %w", config.SourceID, err)
	}

	bentoModule, _ := config.Options["bento_module"].(string)
	p.configs[config.SourceID] = &bentoCDCConfig{
		sourceConfig: config,
		bentoYAML:    inputYAML,
		state:        "configured",
		bentoModule:  bentoModule,
	}
	return nil
}

// Disconnect removes the CDC source configuration.
func (p *BentoProvider) Disconnect(_ context.Context, sourceID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.configs[sourceID]; !exists {
		return fmt.Errorf("bento CDC provider: source %q not found", sourceID)
	}
	delete(p.configs, sourceID)
	return nil
}

// Status returns the current status of a CDC source.
func (p *BentoProvider) Status(_ context.Context, sourceID string) (*CDCStatus, error) {
	p.mu.RLock()
	cfg, exists := p.configs[sourceID]
	p.mu.RUnlock()

	if !exists {
		return &CDCStatus{SourceID: sourceID, State: "not_found", Provider: "bento"}, nil
	}
	return &CDCStatus{
		SourceID: sourceID,
		State:    cfg.state,
		Provider: "bento",
	}, nil
}

// Snapshot regenerates the CDC config for the given source, optionally overriding
// the table list. The updated YAML is available for the delegated bento.input module.
func (p *BentoProvider) Snapshot(_ context.Context, sourceID string, tables []string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	cfg, exists := p.configs[sourceID]
	if !exists {
		return fmt.Errorf("bento CDC provider: source %q not found", sourceID)
	}

	snapCfg := cfg.sourceConfig
	if len(tables) > 0 {
		snapCfg.Tables = tables
	}

	inputYAML, err := buildBentoInputYAML(snapCfg)
	if err != nil {
		return fmt.Errorf("bento CDC provider %q: snapshot build config: %w", sourceID, err)
	}

	cfg.sourceConfig = snapCfg
	cfg.bentoYAML = inputYAML
	cfg.state = "snapshot"
	return nil
}

// SchemaHistory returns schema change history. Bento streams do not track DDL history
// natively; this always returns empty.
func (p *BentoProvider) SchemaHistory(_ context.Context, sourceID string, _ string) ([]SchemaVersion, error) {
	p.mu.RLock()
	_, exists := p.configs[sourceID]
	p.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("bento CDC provider: source %q not found", sourceID)
	}
	return []SchemaVersion{}, nil
}

// RegisterEventHandler stores the event handler for a CDC source. Events are
// delivered by the delegated bento.input engine module via the engine's EventBus.
func (p *BentoProvider) RegisterEventHandler(sourceID string, h EventHandler) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cfg, exists := p.configs[sourceID]
	if !exists {
		return fmt.Errorf("bento CDC provider: source %q not found", sourceID)
	}
	cfg.handler = h
	return nil
}

// ConfigYAML returns the generated Bento input YAML for a configured CDC source.
// This can be used to inspect what config was generated for the delegated module.
func (p *BentoProvider) ConfigYAML(sourceID string) (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	cfg, exists := p.configs[sourceID]
	if !exists {
		return "", fmt.Errorf("bento CDC provider: source %q not found", sourceID)
	}
	return cfg.bentoYAML, nil
}
