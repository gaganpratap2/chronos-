package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestNewJSONLogger(t *testing.T) {
	var output bytes.Buffer

	logger := NewJSONLogger(&output, slog.LevelInfo)

	logger.Info(
		"worker started",
		"worker_id", "worker-01",
	)

	var entry map[string]any

	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatalf("log output is not valid JSON: %v", err)
	}

	if entry["level"] != "INFO" {
		t.Errorf("expected INFO level, got %v", entry["level"])
	}

	if entry["msg"] != "worker started" {
		t.Errorf("unexpected message: %v", entry["msg"])
	}

	if entry["worker_id"] != "worker-01" {
		t.Errorf("unexpected worker_id: %v", entry["worker_id"])
	}
}

func TestNewJSONLoggerFiltersDebug(t *testing.T) {
	var output bytes.Buffer

	logger := NewJSONLogger(&output, slog.LevelInfo)

	logger.Debug("this should not appear")
	logger.Info("this should appear")

	if bytes.Contains(output.Bytes(), []byte("this should not appear")) {
		t.Fatal("DEBUG message should have been filtered")
	}

	if !bytes.Contains(output.Bytes(), []byte("this should appear")) {
		t.Fatal("INFO message should have been logged")
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected slog.Level
	}{
		{"default", "", slog.LevelInfo},
		{"info", "INFO", slog.LevelInfo},
		{"debug", "DEBUG", slog.LevelDebug},
		{"warn", "WARN", slog.LevelWarn},
		{"error", "ERROR", slog.LevelError},
		{"lowercase", "debug", slog.LevelDebug},
		{"whitespace", " INFO ", slog.LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			level, err := ParseLevel(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if level != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, level)
			}
		})
	}
}

func TestParseLevelRejectsInvalidValue(t *testing.T) {
	_, err := ParseLevel("VERBOSE")

	if err == nil {
		t.Fatal("expected an error for invalid log level")
	}
}
