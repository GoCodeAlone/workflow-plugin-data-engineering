package catalog

import (
	"fmt"
	"sync"
)

var (
	srMu      sync.RWMutex
	srModules = map[string]*SchemaRegistryModule{}
)

// RegisterSRModule registers a SchemaRegistryModule under the given name.
func RegisterSRModule(name string, m *SchemaRegistryModule) error {
	srMu.Lock()
	defer srMu.Unlock()
	if _, exists := srModules[name]; exists {
		return fmt.Errorf("catalog: schema registry module %q already registered", name)
	}
	srModules[name] = m
	return nil
}

// UnregisterSRModule removes a registered SchemaRegistryModule.
func UnregisterSRModule(name string) {
	srMu.Lock()
	defer srMu.Unlock()
	delete(srModules, name)
}

// LookupSRModule returns the registered SchemaRegistryModule by name.
func LookupSRModule(name string) (*SchemaRegistryModule, error) {
	srMu.RLock()
	defer srMu.RUnlock()
	m, ok := srModules[name]
	if !ok {
		return nil, fmt.Errorf("catalog: no schema registry module registered for %q", name)
	}
	return m, nil
}
