package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNewMetricsRegistersAllMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()

	metrics, err := NewMetrics(reg)
	if err != nil {
		t.Fatalf("NewMetrics() error = %v", err)
	}

	if metrics == nil {
		t.Fatal("NewMetrics() returned nil metrics")
	}

	metricFamilies, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}

	if len(metricFamilies) != 10 {
		t.Fatalf("expected 10 registered metric families, got %d", len(metricFamilies))
	}
}

func TestNewMetricsRejectsNilRegisterer(t *testing.T) {
	_, err := NewMetrics(nil)
	if err == nil {
		t.Fatal("expected error for nil registerer")
	}
}

func TestCountersIncrease(t *testing.T) {
	reg := prometheus.NewRegistry()

	metrics, err := NewMetrics(reg)
	if err != nil {
		t.Fatalf("NewMetrics() error = %v", err)
	}

	metrics.JobsCreated.Inc()
	metrics.JobsCreated.Add(4)

	if got := testutil.ToFloat64(metrics.JobsCreated); got != 5 {
		t.Fatalf("JobsCreated = %v, want 5", got)
	}

	metrics.JobsCompleted.Inc()

	if got := testutil.ToFloat64(metrics.JobsCompleted); got != 1 {
		t.Fatalf("JobsCompleted = %v, want 1", got)
	}
}

func TestGaugesIncreaseAndDecrease(t *testing.T) {
	reg := prometheus.NewRegistry()

	metrics, err := NewMetrics(reg)
	if err != nil {
		t.Fatalf("NewMetrics() error = %v", err)
	}

	metrics.ActiveWorkers.Set(3)
	metrics.ActiveWorkers.Inc()

	if got := testutil.ToFloat64(metrics.ActiveWorkers); got != 4 {
		t.Fatalf("ActiveWorkers = %v, want 4", got)
	}

	metrics.ActiveWorkers.Dec()

	if got := testutil.ToFloat64(metrics.ActiveWorkers); got != 3 {
		t.Fatalf("ActiveWorkers = %v, want 3", got)
	}
}

func TestHistogramRecordsObservations(t *testing.T) {
	reg := prometheus.NewRegistry()

	metrics, err := NewMetrics(reg)
	if err != nil {
		t.Fatalf("NewMetrics() error = %v", err)
	}

	metrics.JobExecutionDuration.Observe(0.1)
	metrics.JobExecutionDuration.Observe(0.5)
	metrics.JobExecutionDuration.Observe(1.0)

	metricFamilies, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}

	for _, family := range metricFamilies {
		if family.GetName() != "chronos_job_execution_duration_seconds" {
			continue
		}

		if len(family.Metric) != 1 {
			t.Fatalf("expected 1 histogram metric, got %d", len(family.Metric))
		}

		histogram := family.Metric[0].GetHistogram()

		if histogram.GetSampleCount() != 3 {
			t.Fatalf("sample count = %d, want 3", histogram.GetSampleCount())
		}

		if histogram.GetSampleSum() != 1.6 {
			t.Fatalf("sample sum = %v, want 1.6", histogram.GetSampleSum())
		}

		return
	}

	t.Fatal("job execution histogram was not registered")
}

func TestNewMetricsRejectsDuplicateRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()

	_, err := NewMetrics(reg)
	if err != nil {
		t.Fatalf("first NewMetrics() error = %v", err)
	}

	_, err = NewMetrics(reg)
	if err == nil {
		t.Fatal("expected duplicate registration error")
	}
}
