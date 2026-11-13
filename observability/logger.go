package observability

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// NewJSONLogger creates a structured JSON logger.
func NewJSONLogger(output io.Writer, level slog.Level) *slog.Logger {
	handler := slog.NewJSONHandler(output, &slog.HandlerOptions{
		Level: level,
	})

	return slog.New(handler)
}

// ParseLevel converts a configuration string into a slog level.
func ParseLevel(value string) (slog.Level, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "", "INFO":
		return slog.LevelInfo, nil
	case "DEBUG":
		return slog.LevelDebug, nil
	case "WARN":
		return slog.LevelWarn, nil
	case "ERROR":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid log level %q", value)
	}
}

/*
io.Writer: allows logs to go to a file, terminal or test buffer.

slog.Level: determines which messages are recorded.

slog.NewJSONHandler: converts log events into JSON.

slog.New: creates the logger using that handler.
*/
