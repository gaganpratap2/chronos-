package observability

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewMetricsHandler creates an HTTP handler that exposes Prometheus metrics.
func NewMetricsHandler(reg *prometheus.Registry) (http.Handler, error) {
	if reg == nil {
		return nil, fmt.Errorf("prometheus registry cannot be nil")
	}

	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), nil
}
