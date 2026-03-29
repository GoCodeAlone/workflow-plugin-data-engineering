package lakehouse

import (
	"fmt"
	"sync"
)

var (
	catalogMu      sync.RWMutex
	catalogClients = map[string]IcebergCatalogClient{}
)

// RegisterCatalog adds a named catalog client to the global registry.
// Returns an error if a catalog with that name is already registered.
func RegisterCatalog(name string, client IcebergCatalogClient) error {
	catalogMu.Lock()
	defer catalogMu.Unlock()
	if _, exists := catalogClients[name]; exists {
		return fmt.Errorf("lakehouse: catalog %q already registered", name)
	}
	catalogClients[name] = client
	return nil
}

// LookupCatalog retrieves a catalog client by name.
func LookupCatalog(name string) (IcebergCatalogClient, error) {
	catalogMu.RLock()
	defer catalogMu.RUnlock()
	c, ok := catalogClients[name]
	if !ok {
		return nil, fmt.Errorf("lakehouse: catalog %q not registered", name)
	}
	return c, nil
}

// UnregisterCatalog removes a catalog client from the registry.
func UnregisterCatalog(name string) {
	catalogMu.Lock()
	defer catalogMu.Unlock()
	delete(catalogClients, name)
}
