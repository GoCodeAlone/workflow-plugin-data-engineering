package graph

import (
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/registry"
)

var neo4jRegistry = registry.New[*Neo4jModule]("Neo4j module")

// RegisterNeo4jModule registers a Neo4jModule under the given name.
func RegisterNeo4jModule(name string, m *Neo4jModule) error {
	return neo4jRegistry.Register(name, m)
}

// UnregisterNeo4jModule removes a registered Neo4jModule.
func UnregisterNeo4jModule(name string) {
	neo4jRegistry.Unregister(name)
}

// LookupNeo4jModule returns the registered Neo4jModule by name.
func LookupNeo4jModule(name string) (*Neo4jModule, error) {
	return neo4jRegistry.Lookup(name)
}
