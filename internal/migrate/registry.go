package migrate

import (
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/registry"
)

var moduleReg = registry.New[*SchemaModule]("migration module")

// RegisterModule adds a named SchemaModule to the global registry.
func RegisterModule(name string, m *SchemaModule) error {
	return moduleReg.Register(name, m)
}

// LookupModule retrieves a SchemaModule by name.
func LookupModule(name string) (*SchemaModule, error) {
	return moduleReg.Lookup(name)
}

// UnregisterModule removes a SchemaModule from the registry.
func UnregisterModule(name string) {
	moduleReg.Unregister(name)
}
