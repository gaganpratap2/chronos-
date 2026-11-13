package observability

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics contains all Prometheus metrics exposed by Chronos.
type Metrics struct {
	// Counters: values that only increase.
	JobsCreated   prometheus.Counter
	JobsCompleted prometheus.Counter
	JobsFailed    prometheus.Counter
	JobRetries    prometheus.Counter

	// Gauges: values that can increase or decrease.
	ActiveWorkers prometheus.Gauge
	JobsRunning   prometheus.Gauge
	PendingJobs   prometheus.Gauge

	// Histograms: measure durations.
	JobExecutionDuration      prometheus.Histogram
	SchedulerDispatchDuration prometheus.Histogram
	ExternalOperationDuration prometheus.Histogram
}

// NewMetrics creates and registers all Chronos metrics.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	if reg == nil {
		return nil, fmt.Errorf("prometheus registerer cannot be nil")
	}

	m := &Metrics{
		JobsCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "chronos",
			Name:      "jobs_created_total",
			Help:      "Total number of jobs created.",
		}),

		JobsCompleted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "chronos",
			Name:      "jobs_completed_total",
			Help:      "Total number of jobs completed successfully.",
		}),

		JobsFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "chronos",
			Name:      "jobs_failed_total",
			Help:      "Total number of jobs that failed.",
		}),

		JobRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "chronos",
			Name:      "job_retries_total",
			Help:      "Total number of job retry attempts.",
		}),

		ActiveWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "chronos",
			Name:      "active_workers",
			Help:      "Current number of active workers.",
		}),

		JobsRunning: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "chronos",
			Name:      "jobs_running",
			Help:      "Current number of running jobs.",
		}),

		PendingJobs: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "chronos",
			Name:      "pending_jobs",
			Help:      "Current number of pending jobs.",
		}),

		JobExecutionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "chronos",
			Name:      "job_execution_duration_seconds",
			Help:      "Duration of job executions in seconds.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300},
		}),

		SchedulerDispatchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "chronos",
			Name:      "scheduler_dispatch_duration_seconds",
			Help:      "Duration of scheduler dispatch operations in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		}),

		ExternalOperationDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "chronos",
			Name:      "external_operation_duration_seconds",
			Help:      "Duration of external provider operations in seconds.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60},
		}),
	}

	collectors := []prometheus.Collector{
		m.JobsCreated,
		m.JobsCompleted,
		m.JobsFailed,
		m.JobRetries,
		m.ActiveWorkers,
		m.JobsRunning,
		m.PendingJobs,
		m.JobExecutionDuration,
		m.SchedulerDispatchDuration,
		m.ExternalOperationDuration,
	}

	for _, collector := range collectors {
		if err := reg.Register(collector); err != nil {
			return nil, fmt.Errorf("register Chronos metric: %w", err)
		}
	}

	return m, nil
}
