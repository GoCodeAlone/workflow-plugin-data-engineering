package timeseries

import (
	"fmt"
	"sync"
)

var (
	registryMu sync.RWMutex
	registry   = map[string]TimeSeriesWriter{}
)

// Register adds a named TimeSeriesWriter to the global registry.
// Returns an error if the name is already registered.
func Register(name string, w TimeSeriesWriter) error {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, ok := registry[name]; ok {
		return fmt.Errorf("timeseries: module %q already registered", name)
	}
	registry[name] = w
	return nil
}

// Unregister removes a named TimeSeriesWriter from the global registry.
func Unregister(name string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	delete(registry, name)
}

// Lookup finds a TimeSeriesWriter by module name.
func Lookup(name string) (TimeSeriesWriter, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	w, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("timeseries: no module registered with name %q", name)
	}
	return w, nil
}
