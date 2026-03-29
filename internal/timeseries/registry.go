package timeseries

import (
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/registry"
)

var globalRegistry = registry.New[TimeSeriesWriter]("time-series writer")

// Register adds a named TimeSeriesWriter to the global registry.
// Returns an error if the name is already registered.
func Register(name string, w TimeSeriesWriter) error {
	return globalRegistry.Register(name, w)
}

// Unregister removes a named TimeSeriesWriter from the global registry.
func Unregister(name string) {
	globalRegistry.Unregister(name)
}

// Lookup finds a TimeSeriesWriter by module name.
func Lookup(name string) (TimeSeriesWriter, error) {
	return globalRegistry.Lookup(name)
}
