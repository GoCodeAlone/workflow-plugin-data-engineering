package cdc

import (
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/registry"
)

// globalRegistry is the process-level CDC source registry.
var globalRegistry = registry.New[CDCProvider]("CDC source")

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
