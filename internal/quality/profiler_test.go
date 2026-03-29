package quality

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestProfile_NumericColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Row count.
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM metrics").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	// Column "value": null count, distinct, min/max, values.
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM metrics WHERE value IS NULL").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery("SELECT COUNT\\(DISTINCT value\\) FROM metrics").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))
	mock.ExpectQuery("SELECT MIN\\(value\\), MAX\\(value\\) FROM metrics").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(1.0, 10.0))
	mock.ExpectQuery("SELECT value FROM metrics WHERE value IS NOT NULL").
		WillReturnRows(sqlmock.NewRows([]string{"value"}).
			AddRow(1.0).AddRow(5.0).AddRow(10.0))

	var q DBQuerier = db
	result, err := Profile(context.Background(), q, "metrics", []string{"value"})
	if err != nil {
		t.Fatalf("Profile: %v", err)
	}
	if result.RowCount != 3 {
		t.Errorf("RowCount: got %d, want 3", result.RowCount)
	}
	cp, ok := result.Columns["value"]
	if !ok {
		t.Fatal("missing column profile for 'value'")
	}
	if cp.NullCount != 0 {
		t.Errorf("NullCount: got %d, want 0", cp.NullCount)
	}
	if cp.DistinctCount != 3 {
		t.Errorf("DistinctCount: got %d, want 3", cp.DistinctCount)
	}
	if cp.Mean == nil {
		t.Fatal("Mean should not be nil for numeric column")
	}
	if cp.StdDev == nil {
		t.Fatal("StdDev should not be nil for numeric column")
	}
	if cp.Percentiles == nil {
		t.Fatal("Percentiles should not be nil for numeric column")
	}
	// Mean of [1, 5, 10] = 5.333...
	if *cp.Mean < 5.0 || *cp.Mean > 6.0 {
		t.Errorf("Mean: got %f, want ~5.33", *cp.Mean)
	}
	if cp.Percentiles.P50 <= 0 {
		t.Errorf("P50: got %f, want > 0", cp.Percentiles.P50)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestProfile_StringColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Row count.
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM users").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	// Column "name": null count, distinct, min/max, values (strings → scan as float64 fails).
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM users WHERE name IS NULL").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery("SELECT COUNT\\(DISTINCT name\\) FROM users").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))
	mock.ExpectQuery("SELECT MIN\\(name\\), MAX\\(name\\) FROM users").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow("Alice", "Zara"))
	mock.ExpectQuery("SELECT name FROM users WHERE name IS NOT NULL").
		WillReturnRows(sqlmock.NewRows([]string{"name"}).
			AddRow("Alice").AddRow("Bob").AddRow("Zara"))

	var q DBQuerier = db
	result, err := Profile(context.Background(), q, "users", []string{"name"})
	if err != nil {
		t.Fatalf("Profile: %v", err)
	}
	cp, ok := result.Columns["name"]
	if !ok {
		t.Fatal("missing column profile for 'name'")
	}
	if cp.Mean != nil {
		t.Error("Mean should be nil for string column")
	}
	if cp.StdDev != nil {
		t.Error("StdDev should be nil for string column")
	}
	if cp.Percentiles != nil {
		t.Error("Percentiles should be nil for string column")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestProfile_NullRates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Row count.
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM orders").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(10))

	// Column "notes": 4 nulls out of 10 rows.
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM orders WHERE notes IS NULL").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(4))
	mock.ExpectQuery("SELECT COUNT\\(DISTINCT notes\\) FROM orders").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(6))
	mock.ExpectQuery("SELECT MIN\\(notes\\), MAX\\(notes\\) FROM orders").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow("a", "z"))
	mock.ExpectQuery("SELECT notes FROM orders WHERE notes IS NOT NULL").
		WillReturnRows(sqlmock.NewRows([]string{"notes"}).
			AddRow("note1").AddRow("note2"))

	var q DBQuerier = db
	result, err := Profile(context.Background(), q, "orders", []string{"notes"})
	if err != nil {
		t.Fatalf("Profile: %v", err)
	}
	cp := result.Columns["notes"]
	if cp.NullCount != 4 {
		t.Errorf("NullCount: got %d, want 4", cp.NullCount)
	}
	if cp.NullRate < 0.39 || cp.NullRate > 0.41 {
		t.Errorf("NullRate: got %f, want 0.4", cp.NullRate)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestProfile_InvalidTable(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	var q DBQuerier = db
	_, err = Profile(context.Background(), q, "bad; DROP TABLE", []string{"id"})
	if err == nil {
		t.Error("expected error for invalid table name")
	}
}

func TestProfile_EmptyColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM events").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(100))

	var q DBQuerier = db
	result, err := Profile(context.Background(), q, "events", nil)
	if err != nil {
		t.Fatalf("Profile: %v", err)
	}
	if result.RowCount != 100 {
		t.Errorf("RowCount: got %d, want 100", result.RowCount)
	}
	if len(result.Columns) != 0 {
		t.Errorf("expected no columns, got %d", len(result.Columns))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}
