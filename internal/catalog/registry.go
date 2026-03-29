package catalog

import (
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/registry"
)

var srRegistry = registry.New[*SchemaRegistryModule]("schema registry")

// RegisterSRModule registers a SchemaRegistryModule under the given name.
func RegisterSRModule(name string, m *SchemaRegistryModule) error {
	return srRegistry.Register(name, m)
}

// UnregisterSRModule removes a registered SchemaRegistryModule.
func UnregisterSRModule(name string) {
	srRegistry.Unregister(name)
}

// LookupSRModule returns the registered SchemaRegistryModule by name.
func LookupSRModule(name string) (*SchemaRegistryModule, error) {
	return srRegistry.Lookup(name)
}
