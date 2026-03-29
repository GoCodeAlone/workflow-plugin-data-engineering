// Package graph implements the graph.neo4j module and associated steps.
package graph

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// Neo4jDriver is a narrow interface over neo4j.DriverWithContext for testability.
type Neo4jDriver interface {
	NewSession(ctx context.Context, config neo4j.SessionConfig) GraphSession
	VerifyConnectivity(ctx context.Context) error
	Close(ctx context.Context) error
}

// GraphSession is a narrow interface over neo4j.SessionWithContext for testability.
type GraphSession interface {
	Run(ctx context.Context, cypher string, params map[string]any) (GraphResult, error)
	Close(ctx context.Context) error
}

// GraphResult is a narrow interface over neo4j.ResultWithContext for testability.
type GraphResult interface {
	Next(ctx context.Context) bool
	Record() *neo4j.Record
	Err() error
}

// realNeo4jDriver wraps neo4j.DriverWithContext to satisfy Neo4jDriver.
type realNeo4jDriver struct {
	inner neo4j.DriverWithContext
}

func (d *realNeo4jDriver) NewSession(ctx context.Context, config neo4j.SessionConfig) GraphSession {
	return &realGraphSession{inner: d.inner.NewSession(ctx, config)}
}

func (d *realNeo4jDriver) VerifyConnectivity(ctx context.Context) error {
	return d.inner.VerifyConnectivity(ctx)
}

func (d *realNeo4jDriver) Close(ctx context.Context) error {
	return d.inner.Close(ctx)
}

// realGraphSession wraps neo4j.SessionWithContext to satisfy GraphSession.
type realGraphSession struct {
	inner neo4j.SessionWithContext
}

func (s *realGraphSession) Run(ctx context.Context, cypher string, params map[string]any) (GraphResult, error) {
	result, err := s.inner.Run(ctx, cypher, params)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *realGraphSession) Close(ctx context.Context) error {
	return s.inner.Close(ctx)
}

// Neo4jConfig holds configuration for the graph.neo4j module.
type Neo4jConfig struct {
	URI                          string        `json:"uri"                          yaml:"uri"`
	Database                     string        `json:"database"                     yaml:"database"`
	Username                     string        `json:"username"                     yaml:"username"`
	Password                     string        `json:"password"                     yaml:"password"`
	MaxConnectionPoolSize        int           `json:"maxConnectionPoolSize"        yaml:"maxConnectionPoolSize"`
	ConnectionAcquisitionTimeout time.Duration `json:"connectionAcquisitionTimeout" yaml:"connectionAcquisitionTimeout"`
}

// Neo4jModule implements the graph.neo4j module.
type Neo4jModule struct {
	mu        sync.RWMutex
	name      string
	config    Neo4jConfig
	driver    Neo4jDriver
	newDriver func(ctx context.Context, cfg Neo4jConfig) (Neo4jDriver, error)
}

// NewNeo4jModule creates a new graph.neo4j module.
func NewNeo4jModule(name string, config map[string]any) (sdk.ModuleInstance, error) {
	cfg, err := parseNeo4jConfig(config)
	if err != nil {
		return nil, fmt.Errorf("graph.neo4j %q: %w", name, err)
	}
	return &Neo4jModule{
		name:   name,
		config: cfg,
		newDriver: func(ctx context.Context, c Neo4jConfig) (Neo4jDriver, error) {
			return newRealNeo4jDriver(ctx, c)
		},
	}, nil
}

func newRealNeo4jDriver(_ context.Context, cfg Neo4jConfig) (Neo4jDriver, error) {
	auth := neo4j.BasicAuth(cfg.Username, cfg.Password, "")
	var opts []func(*neo4j.Config)
	if cfg.MaxConnectionPoolSize > 0 {
		size := cfg.MaxConnectionPoolSize
		opts = append(opts, func(c *neo4j.Config) {
			c.MaxConnectionPoolSize = size
		})
	}
	if cfg.ConnectionAcquisitionTimeout > 0 {
		timeout := cfg.ConnectionAcquisitionTimeout
		opts = append(opts, func(c *neo4j.Config) {
			c.ConnectionAcquisitionTimeout = timeout
		})
	}
	inner, err := neo4j.NewDriverWithContext(cfg.URI, auth, opts...)
	if err != nil {
		return nil, err
	}
	return &realNeo4jDriver{inner: inner}, nil
}

// Init validates configuration.
func (m *Neo4jModule) Init() error {
	if m.config.URI == "" {
		return fmt.Errorf("graph.neo4j %q: uri is required", m.name)
	}
	return nil
}

// Start creates the driver and verifies connectivity.
func (m *Neo4jModule) Start(ctx context.Context) error {
	driver, err := m.newDriver(ctx, m.config)
	if err != nil {
		return fmt.Errorf("graph.neo4j %q: create driver: %w", m.name, err)
	}
	if err := driver.VerifyConnectivity(ctx); err != nil {
		_ = driver.Close(ctx)
		return fmt.Errorf("graph.neo4j %q: verify connectivity: %w", m.name, err)
	}
	m.mu.Lock()
	m.driver = driver
	m.mu.Unlock()

	if err := RegisterNeo4jModule(m.name, m); err != nil {
		return fmt.Errorf("graph.neo4j %q: register: %w", m.name, err)
	}
	return nil
}

// Stop closes the driver and unregisters the module.
func (m *Neo4jModule) Stop(ctx context.Context) error {
	UnregisterNeo4jModule(m.name)
	m.mu.Lock()
	driver := m.driver
	m.driver = nil
	m.mu.Unlock()
	if driver != nil {
		return driver.Close(ctx)
	}
	return nil
}

// ExecuteCypher runs a Cypher query and returns all rows as maps.
func (m *Neo4jModule) ExecuteCypher(ctx context.Context, cypher string, params map[string]any) ([]map[string]any, error) {
	m.mu.RLock()
	driver := m.driver
	m.mu.RUnlock()
	if driver == nil {
		return nil, fmt.Errorf("graph.neo4j %q: not started", m.name)
	}

	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: m.config.Database})
	defer session.Close(ctx) //nolint:errcheck

	result, err := session.Run(ctx, cypher, params)
	if err != nil {
		return nil, fmt.Errorf("graph.neo4j %q: run cypher: %w", m.name, err)
	}

	var rows []map[string]any
	for result.Next(ctx) {
		rec := result.Record()
		row := make(map[string]any, len(rec.Keys))
		for _, key := range rec.Keys {
			val, _ := rec.Get(key)
			row[key] = val
		}
		rows = append(rows, row)
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("graph.neo4j %q: collect results: %w", m.name, err)
	}
	return rows, nil
}

// NewNeo4jModuleForTest creates a Neo4jModule with an injected driver for integration testing.
func NewNeo4jModuleForTest(name string, driver Neo4jDriver) *Neo4jModule {
	return &Neo4jModule{
		name:   name,
		config: Neo4jConfig{URI: "bolt://test:7687", Database: "neo4j"},
		driver: driver,
	}
}

func parseNeo4jConfig(config map[string]any) (Neo4jConfig, error) {
	var cfg Neo4jConfig
	cfg.Database = "neo4j" // default

	if v, ok := config["uri"].(string); ok {
		cfg.URI = v
	}
	if v, ok := config["database"].(string); ok {
		cfg.Database = v
	}
	if auth, ok := config["auth"].(map[string]any); ok {
		if u, ok := auth["username"].(string); ok {
			cfg.Username = u
		}
		if p, ok := auth["password"].(string); ok {
			cfg.Password = p
		}
	}
	if v, ok := config["username"].(string); ok {
		cfg.Username = v
	}
	if v, ok := config["password"].(string); ok {
		cfg.Password = v
	}
	if v, ok := config["maxConnectionPoolSize"].(int); ok {
		cfg.MaxConnectionPoolSize = v
	}
	if v, ok := config["connectionAcquisitionTimeout"]; ok {
		switch d := v.(type) {
		case string:
			parsed, err := time.ParseDuration(d)
			if err != nil {
				return cfg, fmt.Errorf("invalid connectionAcquisitionTimeout %q: %w", d, err)
			}
			cfg.ConnectionAcquisitionTimeout = parsed
		case time.Duration:
			cfg.ConnectionAcquisitionTimeout = d
		}
	}
	if cfg.URI == "" {
		return cfg, fmt.Errorf("uri is required")
	}
	return cfg, nil
}
