package cdc

import (
	"context"
	"errors"
	"sync"
	"testing"

	dms "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	dmstypes "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// mockDMSClient implements DMSClient for testing.
type mockDMSClient struct {
	mu    sync.Mutex
	calls []string // method names called in order

	createFn      func(*dms.CreateReplicationTaskInput) (*dms.CreateReplicationTaskOutput, error)
	startFn       func(*dms.StartReplicationTaskInput) (*dms.StartReplicationTaskOutput, error)
	stopFn        func(*dms.StopReplicationTaskInput) (*dms.StopReplicationTaskOutput, error)
	describeFn    func(*dms.DescribeReplicationTasksInput) (*dms.DescribeReplicationTasksOutput, error)
	deleteFn      func(*dms.DeleteReplicationTaskInput) (*dms.DeleteReplicationTaskOutput, error)
	tableStatsFn  func(*dms.DescribeTableStatisticsInput) (*dms.DescribeTableStatisticsOutput, error)
}

func (m *mockDMSClient) record(method string) {
	m.mu.Lock()
	m.calls = append(m.calls, method)
	m.mu.Unlock()
}

func (m *mockDMSClient) CreateReplicationTask(ctx context.Context, params *dms.CreateReplicationTaskInput, _ ...func(*dms.Options)) (*dms.CreateReplicationTaskOutput, error) {
	m.record("CreateReplicationTask")
	if m.createFn != nil {
		return m.createFn(params)
	}
	return &dms.CreateReplicationTaskOutput{
		ReplicationTask: &dmstypes.ReplicationTask{
			ReplicationTaskArn:        aws.String("arn:aws:dms:us-east-1:123:task:" + aws.ToString(params.ReplicationTaskIdentifier)),
			ReplicationTaskIdentifier: params.ReplicationTaskIdentifier,
			Status:                    aws.String("creating"),
		},
	}, nil
}

func (m *mockDMSClient) StartReplicationTask(ctx context.Context, params *dms.StartReplicationTaskInput, _ ...func(*dms.Options)) (*dms.StartReplicationTaskOutput, error) {
	m.record("StartReplicationTask")
	if m.startFn != nil {
		return m.startFn(params)
	}
	return &dms.StartReplicationTaskOutput{
		ReplicationTask: &dmstypes.ReplicationTask{
			ReplicationTaskArn: params.ReplicationTaskArn,
			Status:             aws.String("running"),
		},
	}, nil
}

func (m *mockDMSClient) StopReplicationTask(ctx context.Context, params *dms.StopReplicationTaskInput, _ ...func(*dms.Options)) (*dms.StopReplicationTaskOutput, error) {
	m.record("StopReplicationTask")
	if m.stopFn != nil {
		return m.stopFn(params)
	}
	return &dms.StopReplicationTaskOutput{
		ReplicationTask: &dmstypes.ReplicationTask{
			ReplicationTaskArn: params.ReplicationTaskArn,
			Status:             aws.String("stopped"),
		},
	}, nil
}

func (m *mockDMSClient) DescribeReplicationTasks(ctx context.Context, params *dms.DescribeReplicationTasksInput, _ ...func(*dms.Options)) (*dms.DescribeReplicationTasksOutput, error) {
	m.record("DescribeReplicationTasks")
	if m.describeFn != nil {
		return m.describeFn(params)
	}
	return &dms.DescribeReplicationTasksOutput{
		ReplicationTasks: []dmstypes.ReplicationTask{{
			Status: aws.String("running"),
		}},
	}, nil
}

func (m *mockDMSClient) DeleteReplicationTask(ctx context.Context, params *dms.DeleteReplicationTaskInput, _ ...func(*dms.Options)) (*dms.DeleteReplicationTaskOutput, error) {
	m.record("DeleteReplicationTask")
	if m.deleteFn != nil {
		return m.deleteFn(params)
	}
	return &dms.DeleteReplicationTaskOutput{}, nil
}

func (m *mockDMSClient) DescribeTableStatistics(ctx context.Context, params *dms.DescribeTableStatisticsInput, _ ...func(*dms.Options)) (*dms.DescribeTableStatisticsOutput, error) {
	m.record("DescribeTableStatistics")
	if m.tableStatsFn != nil {
		return m.tableStatsFn(params)
	}
	return &dms.DescribeTableStatisticsOutput{
		TableStatistics: []dmstypes.TableStatistics{
			{SchemaName: aws.String("public"), TableName: aws.String("users"), Inserts: 100, Updates: 50, Deletes: 10, Ddls: 2},
		},
	}, nil
}

// helper to create provider with mock + config
func newDMSProviderForTest(client DMSClient, cfg DMSConfig) *DMSProvider {
	return &DMSProvider{
		tasks:  make(map[string]*dmsTask),
		client: client,
		config: cfg,
	}
}

var testDMSConfig = DMSConfig{
	SourceEndpointARN:      "arn:aws:dms:us-east-1:123:endpoint:source",
	TargetEndpointARN:      "arn:aws:dms:us-east-1:123:endpoint:target",
	ReplicationInstanceARN: "arn:aws:dms:us-east-1:123:rep:my-rep",
	MigrationType:          "cdc",
}

// ─── Tests ────────────────────────────────────────────────────────────────────

func TestDMSProvider_CreateReplicationTask(t *testing.T) {
	var capturedCreate *dms.CreateReplicationTaskInput
	mock := &mockDMSClient{
		createFn: func(in *dms.CreateReplicationTaskInput) (*dms.CreateReplicationTaskOutput, error) {
			capturedCreate = in
			return &dms.CreateReplicationTaskOutput{
				ReplicationTask: &dmstypes.ReplicationTask{
					ReplicationTaskArn:        aws.String("arn:aws:dms:us-east-1:123:task:my-task"),
					ReplicationTaskIdentifier: in.ReplicationTaskIdentifier,
					Status:                    aws.String("creating"),
				},
			}, nil
		},
	}
	p := newDMSProviderForTest(mock, testDMSConfig)
	cfg := SourceConfig{
		SourceID: "my-task",
		Tables:   []string{"public.users", "public.orders"},
	}
	if err := p.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if capturedCreate == nil {
		t.Fatal("CreateReplicationTask was not called")
	}
	if aws.ToString(capturedCreate.ReplicationTaskIdentifier) != "my-task" {
		t.Errorf("ReplicationTaskIdentifier = %v, want my-task",
			aws.ToString(capturedCreate.ReplicationTaskIdentifier))
	}
	if aws.ToString(capturedCreate.SourceEndpointArn) != testDMSConfig.SourceEndpointARN {
		t.Errorf("SourceEndpointArn = %v, want %v",
			aws.ToString(capturedCreate.SourceEndpointArn), testDMSConfig.SourceEndpointARN)
	}
	if aws.ToString(capturedCreate.TargetEndpointArn) != testDMSConfig.TargetEndpointARN {
		t.Errorf("TargetEndpointArn = %v, want %v",
			aws.ToString(capturedCreate.TargetEndpointArn), testDMSConfig.TargetEndpointARN)
	}
	if capturedCreate.TableMappings == nil || *capturedCreate.TableMappings == "" {
		t.Error("TableMappings should be non-empty JSON")
	}
	// StartReplicationTask should follow
	mock.mu.Lock()
	calls := mock.calls
	mock.mu.Unlock()
	if len(calls) < 2 || calls[0] != "CreateReplicationTask" || calls[1] != "StartReplicationTask" {
		t.Errorf("expected [Create, Start] calls, got %v", calls)
	}
}

func TestDMSProvider_StartTask(t *testing.T) {
	var capturedStart *dms.StartReplicationTaskInput
	mock := &mockDMSClient{
		startFn: func(in *dms.StartReplicationTaskInput) (*dms.StartReplicationTaskOutput, error) {
			capturedStart = in
			return &dms.StartReplicationTaskOutput{
				ReplicationTask: &dmstypes.ReplicationTask{Status: aws.String("running")},
			}, nil
		},
	}
	p := newDMSProviderForTest(mock, testDMSConfig)
	if err := p.Connect(context.Background(), SourceConfig{SourceID: "start-test"}); err != nil {
		t.Fatal(err)
	}
	if capturedStart == nil {
		t.Fatal("StartReplicationTask not called")
	}
	if capturedStart.StartReplicationTaskType != dmstypes.StartReplicationTaskTypeValueStartReplication {
		t.Errorf("StartReplicationTaskType = %v, want start-replication", capturedStart.StartReplicationTaskType)
	}
}

func TestDMSProvider_StopTask(t *testing.T) {
	mock := &mockDMSClient{}
	p := newDMSProviderForTest(mock, testDMSConfig)
	if err := p.Connect(context.Background(), SourceConfig{SourceID: "stop-test"}); err != nil {
		t.Fatal(err)
	}
	if err := p.Disconnect(context.Background(), "stop-test"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	mock.mu.Lock()
	calls := mock.calls
	mock.mu.Unlock()

	hasStop := false
	hasDelete := false
	for _, c := range calls {
		if c == "StopReplicationTask" {
			hasStop = true
		}
		if c == "DeleteReplicationTask" {
			hasDelete = true
		}
	}
	if !hasStop {
		t.Error("expected StopReplicationTask call in Disconnect")
	}
	if !hasDelete {
		t.Error("expected DeleteReplicationTask call in Disconnect")
	}
}

func TestDMSProvider_StatusMapping(t *testing.T) {
	tests := []struct {
		dmsStatus string
		wantState string
	}{
		{"running", "running"},
		{"stopped", "stopped"},
		{"ready", "stopped"},
		{"failed", "error"},
		{"creating", "unknown"},
		{"deleting", "unknown"},
	}
	for _, tc := range tests {
		t.Run(tc.dmsStatus, func(t *testing.T) {
			mock := &mockDMSClient{
				describeFn: func(in *dms.DescribeReplicationTasksInput) (*dms.DescribeReplicationTasksOutput, error) {
					return &dms.DescribeReplicationTasksOutput{
						ReplicationTasks: []dmstypes.ReplicationTask{{
							Status: aws.String(tc.dmsStatus),
						}},
					}, nil
				},
			}
			p := newDMSProviderForTest(mock, testDMSConfig)
			_ = p.Connect(context.Background(), SourceConfig{SourceID: "status-test"})

			status, err := p.Status(context.Background(), "status-test")
			if err != nil {
				t.Fatal(err)
			}
			if status.State != tc.wantState {
				t.Errorf("dmsStatus=%q: State=%q want %q", tc.dmsStatus, status.State, tc.wantState)
			}
			if status.Provider != "dms" {
				t.Errorf("Provider=%q want dms", status.Provider)
			}
		})
	}
}

func TestDMSProvider_Snapshot(t *testing.T) {
	var capturedStart *dms.StartReplicationTaskInput
	callCount := 0
	mock := &mockDMSClient{
		startFn: func(in *dms.StartReplicationTaskInput) (*dms.StartReplicationTaskOutput, error) {
			callCount++
			if callCount == 2 { // second Start call (snapshot)
				capturedStart = in
			}
			return &dms.StartReplicationTaskOutput{
				ReplicationTask: &dmstypes.ReplicationTask{Status: aws.String("running")},
			}, nil
		},
	}
	p := newDMSProviderForTest(mock, testDMSConfig)
	if err := p.Connect(context.Background(), SourceConfig{SourceID: "snap-test"}); err != nil {
		t.Fatal(err)
	}
	if err := p.Snapshot(context.Background(), "snap-test", []string{"public.users"}); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	if capturedStart == nil {
		t.Fatal("StartReplicationTask not called for snapshot")
	}
	if capturedStart.StartReplicationTaskType != dmstypes.StartReplicationTaskTypeValueReloadTarget {
		t.Errorf("snapshot StartReplicationTaskType = %v, want reload-target",
			capturedStart.StartReplicationTaskType)
	}

	mock.mu.Lock()
	calls := mock.calls
	mock.mu.Unlock()
	// Should have: Create, Start, Stop, Start(reload-target)
	hasStop := false
	for _, c := range calls {
		if c == "StopReplicationTask" {
			hasStop = true
		}
	}
	if !hasStop {
		t.Errorf("expected StopReplicationTask in snapshot flow, got calls: %v", calls)
	}
}

func TestDMSProvider_TableStatistics(t *testing.T) {
	mock := &mockDMSClient{
		tableStatsFn: func(in *dms.DescribeTableStatisticsInput) (*dms.DescribeTableStatisticsOutput, error) {
			return &dms.DescribeTableStatisticsOutput{
				TableStatistics: []dmstypes.TableStatistics{
					{SchemaName: aws.String("public"), TableName: aws.String("users"), Ddls: 3},
					{SchemaName: aws.String("public"), TableName: aws.String("orders"), Ddls: 1},
				},
			}, nil
		},
	}
	p := newDMSProviderForTest(mock, testDMSConfig)
	if err := p.Connect(context.Background(), SourceConfig{SourceID: "stats-test"}); err != nil {
		t.Fatal(err)
	}
	history, err := p.SchemaHistory(context.Background(), "stats-test", "public.users")
	if err != nil {
		t.Fatalf("SchemaHistory: %v", err)
	}
	if len(history) == 0 {
		t.Error("expected non-empty schema history from DMS table statistics")
	}
}

func TestDMSProvider_ErrorHandling_CreateFails(t *testing.T) {
	mock := &mockDMSClient{
		createFn: func(in *dms.CreateReplicationTaskInput) (*dms.CreateReplicationTaskOutput, error) {
			return nil, errors.New("AWS API error: access denied")
		},
	}
	p := newDMSProviderForTest(mock, testDMSConfig)
	err := p.Connect(context.Background(), SourceConfig{SourceID: "err-test"})
	if err == nil {
		t.Fatal("expected error when CreateReplicationTask fails")
	}
}

func TestDMSProvider_ErrorHandling_StartFails(t *testing.T) {
	mock := &mockDMSClient{
		startFn: func(in *dms.StartReplicationTaskInput) (*dms.StartReplicationTaskOutput, error) {
			return nil, errors.New("task not in ready state")
		},
	}
	p := newDMSProviderForTest(mock, testDMSConfig)
	err := p.Connect(context.Background(), SourceConfig{SourceID: "start-err-test"})
	if err == nil {
		t.Fatal("expected error when StartReplicationTask fails")
	}
}

func TestDMSProvider_DuplicateConnect(t *testing.T) {
	p := newDMSProviderForTest(&mockDMSClient{}, testDMSConfig)
	cfg := SourceConfig{SourceID: "dup"}
	if err := p.Connect(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if err := p.Connect(context.Background(), cfg); err == nil {
		t.Fatal("expected error for duplicate connect")
	}
}

func TestDMSProvider_DisconnectNotFound(t *testing.T) {
	p := newDMSProviderForTest(&mockDMSClient{}, testDMSConfig)
	if err := p.Disconnect(context.Background(), "missing"); err == nil {
		t.Fatal("expected error for unknown task")
	}
}

func TestDMSProvider_StatusNotFound(t *testing.T) {
	p := newDMSProviderForTest(&mockDMSClient{}, testDMSConfig)
	status, err := p.Status(context.Background(), "missing")
	if err != nil {
		t.Fatal(err)
	}
	if status.State != "not_found" {
		t.Errorf("State = %q, want not_found", status.State)
	}
}

func TestDMSProvider_RegisterEventHandler(t *testing.T) {
	p := newDMSProviderForTest(&mockDMSClient{}, testDMSConfig)
	if err := p.Connect(context.Background(), SourceConfig{SourceID: "ev-test"}); err != nil {
		t.Fatal(err)
	}
	if err := p.RegisterEventHandler("ev-test", func(string, map[string]any) error { return nil }); err != nil {
		t.Fatalf("RegisterEventHandler: %v", err)
	}
}

func TestDMSProvider_RegisterEventHandler_NotFound(t *testing.T) {
	p := newDMSProviderForTest(&mockDMSClient{}, testDMSConfig)
	err := p.RegisterEventHandler("missing", func(string, map[string]any) error { return nil })
	if err == nil {
		t.Fatal("expected error for unknown source")
	}
}
