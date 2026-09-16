// Package metrics provides the Prometheus metrics shared by all services.
//
// Every service exposes /metrics on its HTTP port. The metric names match
// the spec: requests_total, request_latency (p50/p95/p99 via histogram),
// dispatch_latency, dispatch_success/failure_total, location_updates_total,
// kafka_messages_total, kafka_consumer_lag, redis_operations_total,
// redis_latency, database_latency, active_drivers, available_drivers,
// active_rides, reservation_conflicts, websocket_connections.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

// Registry is the per-service Prometheus registry (plus Go runtime metrics).
type Registry struct {
	*prometheus.Registry
}

// NewRegistry creates a registry with the standard process/go collectors.
func NewRegistry() *Registry {
	r := prometheus.NewRegistry()
	r.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
	return &Registry{Registry: r}
}

// Handler returns the /metrics HTTP handler.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.Registry, promhttp.HandlerOpts{})
}

// Common metric families (created per-service with a "service" label).
type Common struct {
	Service string

	RequestsTotal   *prometheus.CounterVec
	RequestLatency  *prometheus.HistogramVec
	RedisOpsTotal   *prometheus.CounterVec
	RedisLatency    *prometheus.HistogramVec
	DBLatency       *prometheus.HistogramVec
	KafkaMsgsTotal  *prometheus.CounterVec
	KafkaLag        *prometheus.GaugeVec
	WSConnections   prometheus.Gauge
}

// NewCommon registers the common metric families on reg.
func NewCommon(reg *Registry, service string) *Common {
	c := &Common{Service: service}
	c.RequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "requests_total", Help: "Total HTTP/gRPC requests by route and status.",
	}, []string{"route", "status"})
	c.RequestLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "request_latency_seconds",
		Help:    "Request latency in seconds (p50/p95/p99 via histogram_quantile).",
		Buckets: prometheus.DefBuckets,
	}, []string{"route"})
	c.RedisOpsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "redis_operations_total", Help: "Redis operations by op and result.",
	}, []string{"op", "result"})
	c.RedisLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "redis_latency_seconds", Help: "Redis operation latency.",
		Buckets: []float64{.0001, .0005, .001, .005, .01, .05, .1, .5},
	}, []string{"op"})
	c.DBLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "database_latency_seconds", Help: "Postgres query latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"op"})
	c.KafkaMsgsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_messages_total", Help: "Kafka messages produced/consumed.",
	}, []string{"topic", "direction", "result"})
	c.KafkaLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag", Help: "Consumer lag per topic/partition.",
	}, []string{"topic", "partition"})
	c.WSConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "websocket_connections", Help: "Active WebSocket connections.",
	})
	reg.MustRegister(
		c.RequestsTotal, c.RequestLatency,
		c.RedisOpsTotal, c.RedisLatency, c.DBLatency,
		c.KafkaMsgsTotal, c.KafkaLag, c.WSConnections,
	)
	return c
}

// Dispatch metrics (dispatch engine).
type Dispatch struct {
	DispatchLatency *prometheus.HistogramVec
	SuccessTotal    prometheus.Counter
	FailureTotal    *prometheus.CounterVec
	ConflictsTotal  prometheus.Counter
	Candidates      *prometheus.HistogramVec
}

// NewDispatch registers dispatch metrics.
func NewDispatch(reg *Registry) *Dispatch {
	d := &Dispatch{}
	d.DispatchLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dispatch_latency_seconds",
		Help:    "End-to-end dispatch latency (request → reservation).",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"result"})
	d.SuccessTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dispatch_success_total", Help: "Successful dispatches.",
	})
	d.FailureTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatch_failure_total", Help: "Failed dispatches by result code.",
	}, []string{"code"})
	d.ConflictsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "reservation_conflicts_total", Help: "Reservation conflicts (lost races).",
	})
	d.Candidates = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "dispatch_candidates", Help: "Candidates considered per dispatch.",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
	}, []string{})
	reg.MustRegister(d.DispatchLatency, d.SuccessTotal, d.FailureTotal, d.ConflictsTotal, d.Candidates)
	return d
}

// Location metrics (location service / processor).
type Location struct {
	UpdatesTotal *prometheus.CounterVec
	StaleTotal   prometheus.Counter
	ActiveDrivers   prometheus.Gauge
	AvailableDrivers prometheus.Gauge
	ActiveRides     prometheus.Gauge
}

// NewLocation registers location metrics.
func NewLocation(reg *Registry) *Location {
	l := &Location{}
	l.UpdatesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "location_updates_total", Help: "Location updates by result.",
	}, []string{"result"})
	l.StaleTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "location_updates_stale_total", Help: "Updates rejected as stale/duplicate.",
	})
	l.ActiveDrivers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "active_drivers", Help: "Drivers with recent location state.",
	})
	l.AvailableDrivers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "available_drivers", Help: "Drivers currently AVAILABLE.",
	})
	l.ActiveRides = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "active_rides", Help: "Rides in an active (non-terminal) state.",
	})
	reg.MustRegister(l.UpdatesTotal, l.StaleTotal, l.ActiveDrivers, l.AvailableDrivers, l.ActiveRides)
	return l
}