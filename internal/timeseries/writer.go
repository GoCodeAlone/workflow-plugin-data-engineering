package timeseries

import (
	"context"
	"time"
)

// Point represents a single time-series data point.
type Point struct {
	Measurement string            `json:"measurement" yaml:"measurement"`
	Tags        map[string]string `json:"tags"        yaml:"tags"`
	Fields      map[string]any    `json:"fields"      yaml:"fields"`
	Timestamp   time.Time         `json:"timestamp"   yaml:"timestamp"`
}

// TimeSeriesWriter is the common interface implemented by all time-series modules.
type TimeSeriesWriter interface {
	WritePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]any, timestamp time.Time) error
	WriteBatch(ctx context.Context, points []Point) error
	Query(ctx context.Context, query string, args ...any) ([]map[string]any, error)
}
