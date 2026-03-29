package quality

import (
	"fmt"
	"sync"
)

var (
	qcMu      sync.RWMutex
	qcModules = map[string]*ChecksModule{}
)

// RegisterChecksModule registers a ChecksModule under the given name.
func RegisterChecksModule(name string, m *ChecksModule) error {
	qcMu.Lock()
	defer qcMu.Unlock()
	if _, exists := qcModules[name]; exists {
		return fmt.Errorf("quality: checks module %q already registered", name)
	}
	qcModules[name] = m
	return nil
}

// UnregisterChecksModule removes a registered ChecksModule.
func UnregisterChecksModule(name string) {
	qcMu.Lock()
	defer qcMu.Unlock()
	delete(qcModules, name)
}

// LookupChecksModule returns the registered ChecksModule by name.
func LookupChecksModule(name string) (*ChecksModule, error) {
	qcMu.RLock()
	defer qcMu.RUnlock()
	m, ok := qcModules[name]
	if !ok {
		return nil, fmt.Errorf("quality: no checks module registered for %q", name)
	}
	return m, nil
}
