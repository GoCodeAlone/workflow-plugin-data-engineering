package timeseries

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	parquet "github.com/parquet-go/parquet-go"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// TierManager is implemented by time-series modules that support hot/cold tiering.
type TierManager interface {
	Archive(ctx context.Context, olderThan time.Duration, dest, format string, deleteAfter bool) (*ArchiveResult, error)
	Status(ctx context.Context, measurement string) (*TierStatus, error)
}

// ArchiveResult holds the result of an archive operation.
type ArchiveResult struct {
	RowsArchived   int64  `json:"rowsArchived"`
	BytesWritten   int64  `json:"bytesWritten"`
	Destination    string `json:"destination"`
	DeletedFromHot bool   `json:"deletedFromHot"`
}

// TierStatus holds the hot vs cold data summary.
type TierStatus struct {
	HotRows             int64      `json:"hotRows"`
	HotOldestTimestamp  *time.Time `json:"hotOldestTimestamp,omitempty"`
	ColdFiles           int        `json:"coldFiles"`
	ColdOldestTimestamp *time.Time `json:"coldOldestTimestamp,omitempty"`
	TotalBytes          int64      `json:"totalBytes"`
}

// archiveRow is the fixed Parquet schema for archived time-series data.
type archiveRow struct {
	Timestamp   int64  `parquet:"timestamp"`
	Measurement string `parquet:"measurement"`
	Tags        string `parquet:"tags"`
	Fields      string `parquet:"fields"`
}

// writeArchiveFile writes rows to a Parquet or CSV file at destDir and returns bytes written.
func writeArchiveFile(rows []map[string]any, destDir, measurement, format string) (string, int64, error) {
	if err := os.MkdirAll(destDir, 0o750); err != nil {
		return "", 0, fmt.Errorf("create dest dir: %w", err)
	}
	ts := time.Now().Unix()
	var filePath string
	var n int64
	var err error

	switch format {
	case "csv":
		filePath = filepath.Join(destDir, fmt.Sprintf("%s-%d.csv", measurement, ts))
		n, err = writeCSVFile(rows, filePath)
	default: // parquet
		filePath = filepath.Join(destDir, fmt.Sprintf("%s-%d.parquet", measurement, ts))
		n, err = writeParquetFile(rows, filePath)
	}
	if err != nil {
		return "", 0, err
	}
	return filePath, n, nil
}

func writeParquetFile(rows []map[string]any, filePath string) (int64, error) {
	f, err := os.Create(filePath)
	if err != nil {
		return 0, fmt.Errorf("create parquet file: %w", err)
	}
	defer f.Close()

	writer := parquet.NewGenericWriter[archiveRow](f)
	for _, row := range rows {
		ts := int64(0)
		if t, ok := row["time"].(time.Time); ok {
			ts = t.UnixNano()
		}
		meas := ""
		if v, ok := row["measurement"].(string); ok {
			meas = v
		}
		tagsJSON, _ := json.Marshal(row["tags"])
		fieldsMap := make(map[string]any)
		for k, v := range row {
			if k != "time" && k != "measurement" && k != "tags" {
				fieldsMap[k] = v
			}
		}
		fieldsJSON, _ := json.Marshal(fieldsMap)
		if _, err := writer.Write([]archiveRow{{
			Timestamp:   ts,
			Measurement: meas,
			Tags:        string(tagsJSON),
			Fields:      string(fieldsJSON),
		}}); err != nil {
			return 0, fmt.Errorf("write parquet row: %w", err)
		}
	}
	if err := writer.Close(); err != nil {
		return 0, fmt.Errorf("close parquet writer: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		return 0, nil
	}
	return info.Size(), nil
}

func writeCSVFile(rows []map[string]any, filePath string) (int64, error) {
	f, err := os.Create(filePath)
	if err != nil {
		return 0, fmt.Errorf("create csv file: %w", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	headers := []string{"timestamp", "measurement", "tags", "fields"}
	if err := w.Write(headers); err != nil {
		return 0, err
	}
	for _, row := range rows {
		ts := ""
		if t, ok := row["time"].(time.Time); ok {
			ts = fmt.Sprintf("%d", t.UnixNano())
		}
		meas, _ := row["measurement"].(string)
		tagsJSON, _ := json.Marshal(row["tags"])
		fieldsMap := make(map[string]any)
		for k, v := range row {
			if k != "time" && k != "measurement" && k != "tags" {
				fieldsMap[k] = v
			}
		}
		fieldsJSON, _ := json.Marshal(fieldsMap)
		if err := w.Write([]string{ts, meas, string(tagsJSON), string(fieldsJSON)}); err != nil {
			return 0, err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return 0, err
	}
	info, err := f.Stat()
	if err != nil {
		return 0, nil
	}
	return info.Size(), nil
}

// durationToFluxStop converts a duration to a negative Flux duration string.
func durationToFluxStop(d time.Duration) string {
	hours := int64(d.Hours())
	if hours == 0 {
		hours = 1
	}
	return fmt.Sprintf("-%dh", hours)
}

// resolveDest strips "file://" prefix and returns the local path.
func resolveDest(dest string) string {
	dest = strings.TrimRight(dest, "/")
	if strings.HasPrefix(dest, "file://") {
		return strings.TrimPrefix(dest, "file://")
	}
	return dest
}

// -- InfluxDB TierManager --

// Archive implements TierManager for InfluxModule.
func (m *InfluxModule) Archive(ctx context.Context, olderThan time.Duration, dest, format string, deleteAfter bool) (*ArchiveResult, error) {
	m.mu.RLock()
	qapi := m.queryAPI
	bucket := m.config.Bucket
	org := m.config.Org
	rawURL := m.config.URL
	m.mu.RUnlock()

	if qapi == nil {
		return nil, fmt.Errorf("timeseries.influxdb %q: not started", m.name)
	}

	stop := durationToFluxStop(olderThan)
	fluxQuery := fmt.Sprintf(
		`from(bucket: %q) |> range(start: 0, stop: %s)`,
		bucket, stop,
	)
	rows, err := m.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("timeseries.influxdb %q: archive query: %w", m.name, err)
	}

	destDir := resolveDest(dest)
	filePath, bytesWritten, err := writeArchiveFile(rows, destDir, "archive", format)
	if err != nil {
		return nil, fmt.Errorf("timeseries.influxdb %q: write archive: %w", m.name, err)
	}

	deleted := false
	if deleteAfter && len(rows) > 0 {
		if err := influxDeleteOlderThan(ctx, rawURL, org, bucket, m.config.Token, olderThan); err != nil {
			return nil, fmt.Errorf("timeseries.influxdb %q: delete archived data: %w", m.name, err)
		}
		deleted = true
	}

	return &ArchiveResult{
		RowsArchived:   int64(len(rows)),
		BytesWritten:   bytesWritten,
		Destination:    filePath,
		DeletedFromHot: deleted,
	}, nil
}

// Status implements TierManager for InfluxModule.
func (m *InfluxModule) Status(ctx context.Context, measurement string) (*TierStatus, error) {
	m.mu.RLock()
	qapi := m.queryAPI
	bucket := m.config.Bucket
	m.mu.RUnlock()

	if qapi == nil {
		return nil, fmt.Errorf("timeseries.influxdb %q: not started", m.name)
	}

	fluxQuery := fmt.Sprintf(
		`from(bucket: %q) |> range(start: 0) |> filter(fn: (r) => r._measurement == %q) |> count()`,
		bucket, measurement,
	)
	rows, err := m.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("timeseries.influxdb %q: status query: %w", m.name, err)
	}

	status := &TierStatus{}
	for _, row := range rows {
		if v, ok := row["value"].(int64); ok {
			status.HotRows += v
		} else if v, ok := row["value"].(float64); ok {
			status.HotRows += int64(v)
		}
		if t, ok := row["time"].(time.Time); ok {
			if status.HotOldestTimestamp == nil || t.Before(*status.HotOldestTimestamp) {
				tc := t
				status.HotOldestTimestamp = &tc
			}
		}
	}
	return status, nil
}

// influxDeleteOlderThan calls the InfluxDB /api/v2/delete endpoint.
func influxDeleteOlderThan(ctx context.Context, rawURL, org, bucket, token string, olderThan time.Duration) error {
	stop := time.Now().Add(-olderThan)
	body := map[string]any{
		"start":     "1970-01-01T00:00:00Z",
		"stop":      stop.UTC().Format(time.RFC3339),
		"predicate": "",
	}
	b, _ := json.Marshal(body)
	url := fmt.Sprintf("%s/api/v2/delete?org=%s&bucket=%s", rawURL, org, bucket)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Token "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("influxdb delete: HTTP %d: %s", resp.StatusCode, body)
	}
	return nil
}

// -- ClickHouse TierManager --

// Archive implements TierManager for ClickHouseModule.
func (m *ClickHouseModule) Archive(ctx context.Context, olderThan time.Duration, dest, format string, deleteAfter bool) (*ArchiveResult, error) {
	m.mu.RLock()
	conn := m.conn
	m.mu.RUnlock()

	if conn == nil {
		return nil, fmt.Errorf("timeseries.clickhouse %q: not started", m.name)
	}

	cutoff := time.Now().Add(-olderThan)
	query := fmt.Sprintf(
		`SELECT ts, tag_keys, tag_values, field_keys, field_values FROM archive_data WHERE ts < '%s'`,
		cutoff.UTC().Format("2006-01-02 15:04:05"),
	)
	rows, err := m.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("timeseries.clickhouse %q: archive query: %w", m.name, err)
	}

	destDir := resolveDest(dest)
	filePath, bytesWritten, err := writeArchiveFile(rows, destDir, "archive", format)
	if err != nil {
		return nil, fmt.Errorf("timeseries.clickhouse %q: write archive: %w", m.name, err)
	}

	deleted := false
	if deleteAfter && len(rows) > 0 {
		deleteSQL := fmt.Sprintf(
			`ALTER TABLE archive_data DELETE WHERE ts < '%s'`,
			cutoff.UTC().Format("2006-01-02 15:04:05"),
		)
		if err := conn.Exec(ctx, deleteSQL); err != nil {
			return nil, fmt.Errorf("timeseries.clickhouse %q: delete archived data: %w", m.name, err)
		}
		deleted = true
	}

	return &ArchiveResult{
		RowsArchived:   int64(len(rows)),
		BytesWritten:   bytesWritten,
		Destination:    filePath,
		DeletedFromHot: deleted,
	}, nil
}

// Status implements TierManager for ClickHouseModule.
func (m *ClickHouseModule) Status(ctx context.Context, measurement string) (*TierStatus, error) {
	m.mu.RLock()
	conn := m.conn
	db := m.config.Database
	m.mu.RUnlock()

	if conn == nil {
		return nil, fmt.Errorf("timeseries.clickhouse %q: not started", m.name)
	}

	query := fmt.Sprintf(
		`SELECT sum(rows), sum(bytes_on_disk), min(min_time)`+
			` FROM system.parts WHERE database = '%s' AND table = '%s' AND active = 1`,
		db, measurement,
	)
	driverRows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("timeseries.clickhouse %q: status query: %w", m.name, err)
	}
	defer driverRows.Close()

	status := &TierStatus{}
	if driverRows.Next() {
		var hotRows, totalBytes any
		var oldest any
		if scanErr := driverRows.Scan(&hotRows, &totalBytes, &oldest); scanErr != nil {
			return nil, fmt.Errorf("timeseries.clickhouse %q: status scan: %w", m.name, scanErr)
		}
		switch v := hotRows.(type) {
		case uint64:
			status.HotRows = int64(v)
		case int64:
			status.HotRows = v
		case float64:
			status.HotRows = int64(v)
		}
		switch v := totalBytes.(type) {
		case uint64:
			status.TotalBytes = int64(v)
		case int64:
			status.TotalBytes = v
		case float64:
			status.TotalBytes = int64(v)
		}
		if t, ok := oldest.(time.Time); ok {
			status.HotOldestTimestamp = &t
		}
	}
	if err := driverRows.Err(); err != nil {
		return nil, fmt.Errorf("timeseries.clickhouse %q: status rows: %w", m.name, err)
	}
	return status, nil
}

// -- step.ts_archive --

type tsArchiveStep struct {
	name string
}

// NewTSArchiveStep creates a new step.ts_archive instance.
func NewTSArchiveStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &tsArchiveStep{name: name}, nil
}

func (s *tsArchiveStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	module, _ := stringValTS(config, "module")
	if module == "" {
		return nil, fmt.Errorf("step.ts_archive %q: module is required", s.name)
	}
	olderThanStr, _ := stringValTS(config, "olderThan")
	if olderThanStr == "" {
		return nil, fmt.Errorf("step.ts_archive %q: olderThan is required", s.name)
	}
	dest, _ := stringValTS(config, "destination")
	if dest == "" {
		return nil, fmt.Errorf("step.ts_archive %q: destination is required", s.name)
	}
	format, _ := stringValTS(config, "format")
	if format == "" {
		format = "parquet"
	}
	deleteAfter := false
	if v, ok := config["deleteAfterArchive"].(bool); ok {
		deleteAfter = v
	}

	olderThan, err := parseDurationToSeconds(olderThanStr)
	if err != nil {
		return nil, fmt.Errorf("step.ts_archive %q: invalid olderThan %q: %w", s.name, olderThanStr, err)
	}

	writer, err := Lookup(module)
	if err != nil {
		return nil, fmt.Errorf("step.ts_archive %q: %w", s.name, err)
	}
	tm, ok := writer.(TierManager)
	if !ok {
		return nil, fmt.Errorf("step.ts_archive %q: module %q does not support tiering", s.name, module)
	}

	result, err := tm.Archive(ctx, time.Duration(olderThan)*time.Second, dest, format, deleteAfter)
	if err != nil {
		return nil, fmt.Errorf("step.ts_archive %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":         "archived",
		"rowsArchived":   result.RowsArchived,
		"bytesWritten":   result.BytesWritten,
		"destination":    result.Destination,
		"deletedFromHot": result.DeletedFromHot,
	}}, nil
}

// -- step.ts_tier_status --

type tsTierStatusStep struct {
	name string
}

// NewTSTierStatusStep creates a new step.ts_tier_status instance.
func NewTSTierStatusStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &tsTierStatusStep{name: name}, nil
}

func (s *tsTierStatusStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	module, _ := stringValTS(config, "module")
	if module == "" {
		return nil, fmt.Errorf("step.ts_tier_status %q: module is required", s.name)
	}
	measurement, _ := stringValTS(config, "measurement")
	if measurement == "" {
		return nil, fmt.Errorf("step.ts_tier_status %q: measurement is required", s.name)
	}

	writer, err := Lookup(module)
	if err != nil {
		return nil, fmt.Errorf("step.ts_tier_status %q: %w", s.name, err)
	}
	tm, ok := writer.(TierManager)
	if !ok {
		return nil, fmt.Errorf("step.ts_tier_status %q: module %q does not support tiering", s.name, module)
	}

	status, err := tm.Status(ctx, measurement)
	if err != nil {
		return nil, fmt.Errorf("step.ts_tier_status %q: %w", s.name, err)
	}

	out := map[string]any{
		"hotRows":    status.HotRows,
		"coldFiles":  status.ColdFiles,
		"totalBytes": status.TotalBytes,
	}
	if status.HotOldestTimestamp != nil {
		out["hotOldestTimestamp"] = status.HotOldestTimestamp.Format(time.RFC3339)
	}
	if status.ColdOldestTimestamp != nil {
		out["coldOldestTimestamp"] = status.ColdOldestTimestamp.Format(time.RFC3339)
	}

	return &sdk.StepResult{Output: out}, nil
}
