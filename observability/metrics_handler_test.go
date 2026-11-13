package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewMetricsHandlerExposesMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()

	metrics, err := NewMetrics(reg)
	if err != nil {
		t.Fatalf("NewMetrics() error = %v", err)
	}

	metrics.JobsCreated.Add(5)

	handler, err := NewMetricsHandler(reg)
	if err != nil {
		t.Fatalf("NewMetricsHandler() error = %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", response.Code, http.StatusOK)
	}

	body := response.Body.String()

	if !strings.Contains(body, "chronos_jobs_created_total 5") {
		t.Fatalf("metrics response does not contain expected counter:\n%s", body)
	}

	if !strings.Contains(response.Header().Get("Content-Type"), "text/plain") &&
		!strings.Contains(response.Header().Get("Content-Type"), "application/openmetrics-text") {
		t.Fatalf("unexpected Content-Type: %s", response.Header().Get("Content-Type"))
	}
}

func TestNewMetricsHandlerRejectsNilRegistry(t *testing.T) {
	handler, err := NewMetricsHandler(nil)

	if err == nil {
		t.Fatal("expected error for nil registry")
	}

	if handler != nil {
		t.Fatal("expected nil handler for nil registry")
	}
}
