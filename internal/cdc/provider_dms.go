package cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	dms "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	dmstypes "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
)

// DMSClient is the interface over AWS DMS SDK calls, enabling mock injection in tests.
type DMSClient interface {
	CreateReplicationTask(ctx context.Context, params *dms.CreateReplicationTaskInput, optFns ...func(*dms.Options)) (*dms.CreateReplicationTaskOutput, error)
	StartReplicationTask(ctx context.Context, params *dms.StartReplicationTaskInput, optFns ...func(*dms.Options)) (*dms.StartReplicationTaskOutput, error)
	StopReplicationTask(ctx context.Context, params *dms.StopReplicationTaskInput, optFns ...func(*dms.Options)) (*dms.StopReplicationTaskOutput, error)
	DescribeReplicationTasks(ctx context.Context, params *dms.DescribeReplicationTasksInput, optFns ...func(*dms.Options)) (*dms.DescribeReplicationTasksOutput, error)
	DeleteReplicationTask(ctx context.Context, params *dms.DeleteReplicationTaskInput, optFns ...func(*dms.Options)) (*dms.DeleteReplicationTaskOutput, error)
	DescribeTableStatistics(ctx context.Context, params *dms.DescribeTableStatisticsInput, optFns ...func(*dms.Options)) (*dms.DescribeTableStatisticsOutput, error)
}

// DMSConfig holds DMS-specific configuration for the provider.
type DMSConfig struct {
	SourceEndpointARN      string `json:"source_endpoint_arn"      yaml:"source_endpoint_arn"`
	TargetEndpointARN      string `json:"target_endpoint_arn"      yaml:"target_endpoint_arn"`
	ReplicationInstanceARN string `json:"replication_instance_arn" yaml:"replication_instance_arn"`
	MigrationType          string `json:"migration_type"           yaml:"migration_type"` // cdc, full-load-and-cdc
}

// DMSProvider implements CDCProvider using AWS Database Migration Service.
// It creates and manages AWS DMS replication tasks via the AWS SDK.
// The SourceConfig.Connection field is not used; DMS config comes from DMSConfig.
type DMSProvider struct {
	mu     sync.RWMutex
	tasks  map[string]*dmsTask
	client DMSClient
	config DMSConfig
}

// dmsTask holds state for a single managed DMS replication task.
type dmsTask struct {
	mu      sync.RWMutex
	config  SourceConfig
	arn     string // ReplicationTaskArn returned by CreateReplicationTask
	handler EventHandler
}

// newDMSProvider creates a DMSProvider by loading AWS credentials from the environment
// and parsing DMS-specific config (region, ARNs, migration type) from cfg.Options.
func newDMSProvider(cfg SourceConfig) (*DMSProvider, error) {
	opts := cfg.Options

	region, _ := opts["region"].(string)
	dmsCfg := DMSConfig{
		MigrationType: "cdc", // default
	}
	if v, ok := opts["source_endpoint_arn"].(string); ok {
		dmsCfg.SourceEndpointARN = v
	}
	if v, ok := opts["target_endpoint_arn"].(string); ok {
		dmsCfg.TargetEndpointARN = v
	}
	if v, ok := opts["replication_instance_arn"].(string); ok {
		dmsCfg.ReplicationInstanceARN = v
	}
	if v, ok := opts["migration_type"].(string); ok && v != "" {
		dmsCfg.MigrationType = v
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("dms CDC provider: load AWS config: %w", err)
	}

	return &DMSProvider{
		tasks:  make(map[string]*dmsTask),
		client: dms.NewFromConfig(awsCfg),
		config: dmsCfg,
	}, nil
}

// Connect creates an AWS DMS replication task and starts it.
func (p *DMSProvider) Connect(ctx context.Context, config SourceConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.tasks[config.SourceID]; exists {
		return fmt.Errorf("dms CDC provider: task %q already exists", config.SourceID)
	}
	if p.client == nil {
		return fmt.Errorf("dms CDC provider: no AWS DMS client configured")
	}

	migrationType := dmsMigrationType(p.config.MigrationType)
	tableMappings := buildDMSTableMappings(config.Tables)

	createOut, err := p.client.CreateReplicationTask(ctx, &dms.CreateReplicationTaskInput{
		ReplicationTaskIdentifier: aws.String(config.SourceID),
		MigrationType:             migrationType,
		SourceEndpointArn:         aws.String(p.config.SourceEndpointARN),
		TargetEndpointArn:         aws.String(p.config.TargetEndpointARN),
		ReplicationInstanceArn:    aws.String(p.config.ReplicationInstanceARN),
		TableMappings:             aws.String(tableMappings),
	})
	if err != nil {
		return fmt.Errorf("dms CDC provider: create task %q: %w", config.SourceID, err)
	}

	taskARN := ""
	if createOut.ReplicationTask != nil {
		taskARN = aws.ToString(createOut.ReplicationTask.ReplicationTaskArn)
	}

	_, err = p.client.StartReplicationTask(ctx, &dms.StartReplicationTaskInput{
		ReplicationTaskArn:        aws.String(taskARN),
		StartReplicationTaskType:  dmstypes.StartReplicationTaskTypeValueStartReplication,
	})
	if err != nil {
		return fmt.Errorf("dms CDC provider: start task %q: %w", config.SourceID, err)
	}

	p.tasks[config.SourceID] = &dmsTask{config: config, arn: taskARN}
	return nil
}

// Disconnect stops and deletes the AWS DMS replication task.
func (p *DMSProvider) Disconnect(ctx context.Context, sourceID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	task, exists := p.tasks[sourceID]
	if !exists {
		return fmt.Errorf("dms CDC provider: task %q not found", sourceID)
	}

	task.mu.RLock()
	arn := task.arn
	task.mu.RUnlock()

	// Stop the task (ignore error — task may already be stopped)
	_, _ = p.client.StopReplicationTask(ctx, &dms.StopReplicationTaskInput{
		ReplicationTaskArn: aws.String(arn),
	})

	if _, err := p.client.DeleteReplicationTask(ctx, &dms.DeleteReplicationTaskInput{
		ReplicationTaskArn: aws.String(arn),
	}); err != nil {
		return fmt.Errorf("dms CDC provider: delete task %q: %w", sourceID, err)
	}

	delete(p.tasks, sourceID)
	return nil
}

// Status returns the current status of an AWS DMS replication task.
func (p *DMSProvider) Status(ctx context.Context, sourceID string) (*CDCStatus, error) {
	p.mu.RLock()
	task, exists := p.tasks[sourceID]
	p.mu.RUnlock()
	if !exists {
		return &CDCStatus{SourceID: sourceID, State: "not_found", Provider: "dms"}, nil
	}

	task.mu.RLock()
	arn := task.arn
	task.mu.RUnlock()

	out, err := p.client.DescribeReplicationTasks(ctx, &dms.DescribeReplicationTasksInput{
		Filters: []dmstypes.Filter{{
			Name:   aws.String("replication-task-arn"),
			Values: []string{arn},
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("dms CDC provider: describe task %q: %w", sourceID, err)
	}

	state := "unknown"
	lagSeconds := int64(0)
	if len(out.ReplicationTasks) > 0 {
		t := out.ReplicationTasks[0]
		state = dmsTaskState(aws.ToString(t.Status))
		// CDCLatencySeconds is surfaced via table statistics; use 0 here (no
		// direct latency field on ReplicationTaskStats in this SDK version).
		_ = lagSeconds
	}

	return &CDCStatus{
		SourceID: sourceID,
		State:    state,
		Provider: "dms",
	}, nil
}

// PauseSource stops the DMS replication task (pause = stop for DMS).
func (p *DMSProvider) PauseSource(ctx context.Context, sourceID string) error {
	p.mu.RLock()
	task, exists := p.tasks[sourceID]
	p.mu.RUnlock()
	if !exists {
		return fmt.Errorf("dms CDC provider: task %q not found", sourceID)
	}
	task.mu.RLock()
	arn := task.arn
	task.mu.RUnlock()
	if _, err := p.client.StopReplicationTask(ctx, &dms.StopReplicationTaskInput{
		ReplicationTaskArn: aws.String(arn),
	}); err != nil {
		return fmt.Errorf("dms CDC provider: pause task %q: %w", sourceID, err)
	}
	return nil
}

// ResumeSource restarts the DMS replication task from where it stopped.
func (p *DMSProvider) ResumeSource(ctx context.Context, sourceID string) error {
	p.mu.RLock()
	task, exists := p.tasks[sourceID]
	p.mu.RUnlock()
	if !exists {
		return fmt.Errorf("dms CDC provider: task %q not found", sourceID)
	}
	task.mu.RLock()
	arn := task.arn
	task.mu.RUnlock()
	if _, err := p.client.StartReplicationTask(ctx, &dms.StartReplicationTaskInput{
		ReplicationTaskArn:       aws.String(arn),
		StartReplicationTaskType: dmstypes.StartReplicationTaskTypeValueResumeProcessing,
	}); err != nil {
		return fmt.Errorf("dms CDC provider: resume task %q: %w", sourceID, err)
	}
	return nil
}

// Snapshot triggers a full re-snapshot via reload-target:
// stops the task then restarts with reload-target migration type.
func (p *DMSProvider) Snapshot(ctx context.Context, sourceID string, tables []string) error {
	p.mu.RLock()
	task, exists := p.tasks[sourceID]
	p.mu.RUnlock()
	if !exists {
		return fmt.Errorf("dms CDC provider: task %q not found", sourceID)
	}

	task.mu.RLock()
	arn := task.arn
	task.mu.RUnlock()

	// Stop the task first
	if _, err := p.client.StopReplicationTask(ctx, &dms.StopReplicationTaskInput{
		ReplicationTaskArn: aws.String(arn),
	}); err != nil {
		return fmt.Errorf("dms CDC provider: snapshot stop task %q: %w", sourceID, err)
	}

	// Restart with reload-target to trigger full re-snapshot
	if _, err := p.client.StartReplicationTask(ctx, &dms.StartReplicationTaskInput{
		ReplicationTaskArn:       aws.String(arn),
		StartReplicationTaskType: dmstypes.StartReplicationTaskTypeValueReloadTarget,
	}); err != nil {
		return fmt.Errorf("dms CDC provider: snapshot restart task %q: %w", sourceID, err)
	}

	return nil
}

// SchemaHistory returns schema change stats from AWS DMS table statistics.
// DMS tracks DDL counts per table via DescribeTableStatistics.
func (p *DMSProvider) SchemaHistory(ctx context.Context, sourceID string, table string) ([]SchemaVersion, error) {
	p.mu.RLock()
	task, exists := p.tasks[sourceID]
	p.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("dms CDC provider: task %q not found", sourceID)
	}

	task.mu.RLock()
	arn := task.arn
	task.mu.RUnlock()

	out, err := p.client.DescribeTableStatistics(ctx, &dms.DescribeTableStatisticsInput{
		ReplicationTaskArn: aws.String(arn),
	})
	if err != nil {
		return nil, fmt.Errorf("dms CDC provider: table statistics %q: %w", sourceID, err)
	}

	var history []SchemaVersion
	for i, ts := range out.TableStatistics {
		if ts.Ddls == 0 {
			continue
		}
		schema := aws.ToString(ts.SchemaName)
		tbl := aws.ToString(ts.TableName)
		history = append(history, SchemaVersion{
			Table:     schema + "." + tbl,
			Version:   int64(i + 1),
			DDL:       fmt.Sprintf("DDL changes: %d (inserts:%d updates:%d deletes:%d)", ts.Ddls, ts.Inserts, ts.Updates, ts.Deletes),
			AppliedAt: "",
		})
	}
	return history, nil
}

// RegisterEventHandler registers a callback for CDC events from an AWS DMS task.
func (p *DMSProvider) RegisterEventHandler(sourceID string, h EventHandler) error {
	p.mu.RLock()
	task, exists := p.tasks[sourceID]
	p.mu.RUnlock()
	if !exists {
		return fmt.Errorf("dms CDC provider: task %q not found", sourceID)
	}
	task.mu.Lock()
	task.handler = h
	task.mu.Unlock()
	return nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// dmsTaskState maps AWS DMS task status strings to CDCStatus states.
func dmsTaskState(s string) string {
	switch s {
	case "running":
		return "running"
	case "stopped", "ready":
		return "stopped"
	case "failed":
		return "error"
	default:
		return "unknown"
	}
}

// dmsMigrationType converts a string to the AWS DMS MigrationTypeValue enum.
func dmsMigrationType(s string) dmstypes.MigrationTypeValue {
	switch s {
	case "full-load":
		return dmstypes.MigrationTypeValueFullLoad
	case "full-load-and-cdc":
		return dmstypes.MigrationTypeValueFullLoadAndCdc
	default:
		return dmstypes.MigrationTypeValueCdc
	}
}

// buildDMSTableMappings builds a DMS table mappings JSON string from a list of tables.
// Tables should be in "schema.table" format; a catch-all wildcard is used if empty.
func buildDMSTableMappings(tables []string) string {
	type rule struct {
		RuleType   string `json:"rule-type"`
		RuleID     string `json:"rule-id"`
		RuleName   string `json:"rule-name"`
		RuleAction string `json:"rule-action"`
		ObjectLocator struct {
			SchemaName string `json:"schema-name"`
			TableName  string `json:"table-name"`
		} `json:"object-locator"`
	}

	var rules []rule
	if len(tables) == 0 {
		r := rule{
			RuleType:   "selection",
			RuleID:     "1",
			RuleName:   "include-all",
			RuleAction: "include",
		}
		r.ObjectLocator.SchemaName = "%"
		r.ObjectLocator.TableName = "%"
		rules = append(rules, r)
	} else {
		for i, t := range tables {
			schema, table := "%", t
			if parts := strings.SplitN(t, ".", 2); len(parts) == 2 {
				schema, table = parts[0], parts[1]
			}
			r := rule{
				RuleType:   "selection",
				RuleID:     fmt.Sprintf("%d", i+1),
				RuleName:   fmt.Sprintf("include-%d", i+1),
				RuleAction: "include",
			}
			r.ObjectLocator.SchemaName = schema
			r.ObjectLocator.TableName = table
			rules = append(rules, r)
		}
	}

	data, _ := json.Marshal(map[string]any{"rules": rules})
	return string(data)
}
