package migrate

import (
	"fmt"
	"sync"
)

var (
	moduleMu      sync.RWMutex
	moduleRegistry = map[string]*SchemaModule{}
)

// RegisterModule adds a named SchemaModule to the global registry.
func RegisterModule(name string, m *SchemaModule) error {
	moduleMu.Lock()
	defer moduleMu.Unlock()
	if _, exists := moduleRegistry[name]; exists {
		return fmt.Errorf("migrate: module %q already registered", name)
	}
	moduleRegistry[name] = m
	return nil
}

// LookupModule retrieves a SchemaModule by name.
func LookupModule(name string) (*SchemaModule, error) {
	moduleMu.RLock()
	defer moduleMu.RUnlock()
	m, ok := moduleRegistry[name]
	if !ok {
		return nil, fmt.Errorf("migrate: module %q not registered", name)
	}
	return m, nil
}

// UnregisterModule removes a SchemaModule from the registry.
func UnregisterModule(name string) {
	moduleMu.Lock()
	defer moduleMu.Unlock()
	delete(moduleRegistry, name)
}
