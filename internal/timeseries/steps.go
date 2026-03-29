package timeseries

import (
	"context"
	"fmt"
	"strings"
	"time"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// tsWriteStep implements step.ts_write — writes a single data point.
type tsWriteStep struct {
	name string
}

// NewTSWriteStep creates a new step.ts_write instance.
func NewTSWriteStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &tsWriteStep{name: name}, nil
}

func (s *tsWriteStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	module, _ := stringValTS(config, "module")
	if module == "" {
		return nil, fmt.Errorf("step.ts_write %q: module is required", s.name)
	}
	measurement, _ := stringValTS(config, "measurement")
	if measurement == "" {
		return nil, fmt.Errorf("step.ts_write %q: measurement is required", s.name)
	}

	tags := extractStringMap(config, "tags")
	fields := extractAnyMap(config, "fields")
	if len(fields) == 0 {
		return nil, fmt.Errorf("step.ts_write %q: fields is required", s.name)
	}
	ts := extractTimestamp(config, "timestamp")

	writer, err := Lookup(module)
	if err != nil {
		return nil, fmt.Errorf("step.ts_write %q: %w", s.name, err)
	}

	if err := writer.WritePoint(ctx, measurement, tags, fields, ts); err != nil {
		return nil, fmt.Errorf("step.ts_write %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":      "written",
		"measurement": measurement,
		"timestamp":   ts.Format(time.RFC3339Nano),
	}}, nil
}

// tsWriteBatchStep implements step.ts_write_batch — writes multiple points.
type tsWriteBatchStep struct {
	name string
}

// NewTSWriteBatchStep creates a new step.ts_write_batch instance.
func NewTSWriteBatchStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &tsWriteBatchStep{name: name}, nil
}

func (s *tsWriteBatchStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	module, _ := stringValTS(config, "module")
	if module == "" {
		return nil, fmt.Errorf("step.ts_write_batch %q: module is required", s.name)
	}

	rawPoints, ok := config["points"].([]any)
	if !ok || len(rawPoints) == 0 {
		return nil, fmt.Errorf("step.ts_write_batch %q: points is required", s.name)
	}

	points := make([]Point, 0, len(rawPoints))
	for i, raw := range rawPoints {
		pm, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("step.ts_write_batch %q: points[%d] must be an object", s.name, i)
		}
		measurement, _ := stringValTS(pm, "measurement")
		if measurement == "" {
			return nil, fmt.Errorf("step.ts_write_batch %q: points[%d].measurement is required", s.name, i)
		}
		points = append(points, Point{
			Measurement: measurement,
			Tags:        extractStringMap(pm, "tags"),
			Fields:      extractAnyMap(pm, "fields"),
			Timestamp:   extractTimestamp(pm, "timestamp"),
		})
	}

	writer, err := Lookup(module)
	if err != nil {
		return nil, fmt.Errorf("step.ts_write_batch %q: %w", s.name, err)
	}

	if err := writer.WriteBatch(ctx, points); err != nil {
		return nil, fmt.Errorf("step.ts_write_batch %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"status": "written",
		"count":  len(points),
	}}, nil
}

// tsQueryStep implements step.ts_query — executes a query against the time-series store.
type tsQueryStep struct {
	name string
}

// NewTSQueryStep creates a new step.ts_query instance.
func NewTSQueryStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &tsQueryStep{name: name}, nil
}

func (s *tsQueryStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	module, _ := stringValTS(config, "module")
	if module == "" {
		return nil, fmt.Errorf("step.ts_query %q: module is required", s.name)
	}
	query, _ := stringValTS(config, "query")
	if query == "" {
		return nil, fmt.Errorf("step.ts_query %q: query is required", s.name)
	}

	writer, err := Lookup(module)
	if err != nil {
		return nil, fmt.Errorf("step.ts_query %q: %w", s.name, err)
	}

	rows, err := writer.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("step.ts_query %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"rows":  rows,
		"count": len(rows),
	}}, nil
}

// tsDownsampleStep implements step.ts_downsample — creates a downsampling task via Flux.
type tsDownsampleStep struct {
	name string
}

// NewTSDownsampleStep creates a new step.ts_downsample instance.
func NewTSDownsampleStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &tsDownsampleStep{name: name}, nil
}

func (s *tsDownsampleStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	module, _ := stringValTS(config, "module")
	if module == "" {
		return nil, fmt.Errorf("step.ts_downsample %q: module is required", s.name)
	}
	source, _ := stringValTS(config, "source")
	if source == "" {
		return nil, fmt.Errorf("step.ts_downsample %q: source is required", s.name)
	}
	target, _ := stringValTS(config, "target")
	if target == "" {
		return nil, fmt.Errorf("step.ts_downsample %q: target is required", s.name)
	}
	aggregation, _ := stringValTS(config, "aggregation")
	if aggregation == "" {
		aggregation = "mean"
	}
	interval, _ := stringValTS(config, "interval")
	if interval == "" {
		return nil, fmt.Errorf("step.ts_downsample %q: interval is required", s.name)
	}

	// Resolve the module to get bucket info.
	writer, err := Lookup(module)
	if err != nil {
		return nil, fmt.Errorf("step.ts_downsample %q: %w", s.name, err)
	}

	bucket := ""
	if im, ok := writer.(*InfluxModule); ok {
		bucket = im.Config().Bucket
	}
	if bucket == "" {
		bucket = "default"
	}

	fluxQuery := buildDownsampleFlux(bucket, source, target, aggregation, interval)

	_, err = writer.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("step.ts_downsample %q: execute downsample: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"status": "downsampled",
		"query":  fluxQuery,
	}}, nil
}

func buildDownsampleFlux(bucket, source, target, aggregation, interval string) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("from(bucket: \"%s\")", bucket))
	sb.WriteString("\n  |> range(start: -inf)")
	sb.WriteString(fmt.Sprintf("\n  |> filter(fn: (r) => r._measurement == \"%s\")", source))
	sb.WriteString(fmt.Sprintf("\n  |> aggregateWindow(every: %s, fn: %s, createEmpty: false)", interval, aggregation))
	sb.WriteString(fmt.Sprintf("\n  |> set(key: \"_measurement\", value: \"%s\")", target))
	sb.WriteString(fmt.Sprintf("\n  |> to(bucket: \"%s\")", bucket))
	return sb.String()
}

// tsRetentionStep implements step.ts_retention — manages bucket retention policies.
type tsRetentionStep struct {
	name string
}

// NewTSRetentionStep creates a new step.ts_retention instance.
func NewTSRetentionStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &tsRetentionStep{name: name}, nil
}

func (s *tsRetentionStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	module, _ := stringValTS(config, "module")
	if module == "" {
		return nil, fmt.Errorf("step.ts_retention %q: module is required", s.name)
	}
	duration, _ := stringValTS(config, "duration")
	if duration == "" {
		return nil, fmt.Errorf("step.ts_retention %q: duration is required", s.name)
	}

	writer, err := Lookup(module)
	if err != nil {
		return nil, fmt.Errorf("step.ts_retention %q: %w", s.name, err)
	}

	im, ok := writer.(*InfluxModule)
	if !ok {
		return nil, fmt.Errorf("step.ts_retention %q: only supported for timeseries.influxdb modules", s.name)
	}

	bucket, _ := stringValTS(config, "bucket")
	if bucket == "" {
		bucket = im.Config().Bucket
	}

	secs, err := parseDurationToSeconds(duration)
	if err != nil {
		return nil, fmt.Errorf("step.ts_retention %q: invalid duration %q: %w", s.name, duration, err)
	}

	if err := im.UpdateBucketRetention(ctx, bucket, secs); err != nil {
		return nil, fmt.Errorf("step.ts_retention %q: %w", s.name, err)
	}

	return &sdk.StepResult{Output: map[string]any{
		"status":    "updated",
		"bucket":    bucket,
		"retention": duration,
	}}, nil
}

// helpers

func stringValTS(m map[string]any, key string) (string, bool) {
	v, ok := m[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

func extractStringMap(m map[string]any, key string) map[string]string {
	raw, ok := m[key].(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func extractAnyMap(m map[string]any, key string) map[string]any {
	raw, ok := m[key].(map[string]any)
	if !ok {
		return nil
	}
	return raw
}

func extractTimestamp(m map[string]any, key string) time.Time {
	v, ok := m[key]
	if !ok {
		return time.Now()
	}
	switch t := v.(type) {
	case time.Time:
		return t
	case string:
		if parsed, err := time.Parse(time.RFC3339Nano, t); err == nil {
			return parsed
		}
		if parsed, err := time.Parse(time.RFC3339, t); err == nil {
			return parsed
		}
	case int64:
		return time.Unix(0, t)
	case float64:
		return time.Unix(0, int64(t))
	}
	return time.Now()
}

func parseDurationToSeconds(s string) (int64, error) {
	// Handle simple duration strings like "30d", "7d", "1h", "30m"
	// Also handle Go duration strings like "24h"
	if d, err := time.ParseDuration(s); err == nil {
		return int64(d.Seconds()), nil
	}
	// Handle day suffix
	if strings.HasSuffix(s, "d") {
		var days int64
		if _, err := fmt.Sscanf(s, "%dd", &days); err == nil {
			return days * 86400, nil
		}
	}
	return 0, fmt.Errorf("cannot parse duration %q", s)
}
