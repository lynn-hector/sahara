// Package metrics defines Prometheus metrics for the Sahara Gateway.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// ── WebSocket ─────────────────────────────────────────
	WSConnsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sahara_gw_ws_connections_active",
		Help: "Number of active WebSocket connections",
	})
	WSMessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sahara_gw_ws_messages_total",
		Help: "Total WebSocket messages by method",
	}, []string{"method"})

	// ── Task dispatch ─────────────────────────────────────
	TasksSubmittedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sahara_gw_tasks_submitted_total",
		Help: "Total tasks submitted to workers",
	}, []string{"status"})
	TaskDispatchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sahara_gw_task_dispatch_duration_seconds",
		Help:    "Time to dispatch a task to a worker",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	})

	// ── Overload ──────────────────────────────────────────
	OverloadRejectsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sahara_gw_overload_rejects_total",
		Help: "Tasks rejected due to all workers being busy",
	})

	// ── Event broadcast ───────────────────────────────────
	EventsBroadcastTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sahara_gw_events_broadcast_total",
		Help: "Events broadcast to WebSocket clients",
	}, []string{"event_type"})

	// ── HTTP compat API ───────────────────────────────────
	CompatRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sahara_gw_compat_requests_total",
		Help: "OpenAI-compat HTTP API requests",
	}, []string{"endpoint", "status"})
	CompatRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sahara_gw_compat_request_duration_seconds",
		Help:    "OpenAI-compat HTTP API request duration",
		Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120},
	}, []string{"endpoint"})

	// ── Auth ──────────────────────────────────────────────
	AuthAttemptsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sahara_gw_auth_attempts_total",
		Help: "Authentication attempts by result",
	}, []string{"mode", "result"})
)

// Handler returns the Prometheus HTTP handler for /metrics.
func Handler() http.Handler {
	return promhttp.Handler()
}
