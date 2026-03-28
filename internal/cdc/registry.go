package cdc

import (
	"fmt"
	"sync"
)

// sourceRegistry maps sourceID → CDCProvider for running cdc.source modules.
// Steps and triggers use this to operate on live CDC streams without needing
// a direct reference to the module instance.
type sourceRegistry struct {
	mu      sync.RWMutex
	sources map[string]CDCProvider
}

// globalRegistry is the process-level CDC source registry.
var globalRegistry = &sourceRegistry{
	sources: make(map[string]CDCProvider),
}

// Register adds a CDC provider to the registry under the given sourceID.
// Returns an error if the sourceID is already registered.
func (r *sourceRegistry) Register(sourceID string, p CDCProvider) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.sources[sourceID]; exists {
		return fmt.Errorf("cdc registry: source %q already registered", sourceID)
	}
	r.sources[sourceID] = p
	return nil
}

// Unregister removes a CDC provider from the registry.
func (r *sourceRegistry) Unregister(sourceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sources, sourceID)
}

// Lookup returns the CDCProvider for the given sourceID, or an error if not found.
func (r *sourceRegistry) Lookup(sourceID string) (CDCProvider, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.sources[sourceID]
	if !ok {
		return nil, fmt.Errorf("cdc registry: source %q not found (is the cdc.source module running?)", sourceID)
	}
	return p, nil
}

// RegisterSource registers a CDC provider in the global registry.
func RegisterSource(sourceID string, p CDCProvider) error {
	return globalRegistry.Register(sourceID, p)
}

// UnregisterSource removes a source from the global registry.
func UnregisterSource(sourceID string) {
	globalRegistry.Unregister(sourceID)
}

// LookupSource finds a running CDC source provider by ID.
func LookupSource(sourceID string) (CDCProvider, error) {
	return globalRegistry.Lookup(sourceID)
}
