package graph

import (
	"fmt"
	"sync"
)

var (
	neo4jMu      sync.RWMutex
	neo4jModules = map[string]*Neo4jModule{}
)

// RegisterNeo4jModule registers a Neo4jModule under the given name.
func RegisterNeo4jModule(name string, m *Neo4jModule) error {
	neo4jMu.Lock()
	defer neo4jMu.Unlock()
	if _, exists := neo4jModules[name]; exists {
		return fmt.Errorf("graph: neo4j module %q already registered", name)
	}
	neo4jModules[name] = m
	return nil
}

// UnregisterNeo4jModule removes a registered Neo4jModule.
func UnregisterNeo4jModule(name string) {
	neo4jMu.Lock()
	defer neo4jMu.Unlock()
	delete(neo4jModules, name)
}

// LookupNeo4jModule returns the registered Neo4jModule by name.
func LookupNeo4jModule(name string) (*Neo4jModule, error) {
	neo4jMu.RLock()
	defer neo4jMu.RUnlock()
	m, ok := neo4jModules[name]
	if !ok {
		return nil, fmt.Errorf("graph: no neo4j module registered for %q", name)
	}
	return m, nil
}
