package lakehouse

import (
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/registry"
)

var catalogRegistry = registry.New[IcebergCatalogClient]("Iceberg catalog")

// RegisterCatalog adds a named catalog client to the global registry.
// Returns an error if a catalog with that name is already registered.
func RegisterCatalog(name string, client IcebergCatalogClient) error {
	return catalogRegistry.Register(name, client)
}

// LookupCatalog retrieves a catalog client by name.
func LookupCatalog(name string) (IcebergCatalogClient, error) {
	return catalogRegistry.Lookup(name)
}

// UnregisterCatalog removes a catalog client from the registry.
func UnregisterCatalog(name string) {
	catalogRegistry.Unregister(name)
}
