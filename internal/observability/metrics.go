package observability

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"taskforge.dev/internal/model"
)

// GaugeSource supplies the point-in-time values exported as gauges on every
// scrape.
type GaugeSource interface {
	StatusCounts(ctx context.Context) ([]model.StatusCount, error)
	ActiveWorkers(ctx context.Context) ([]model.WorkerStatus, error)
}

// Metrics holds TaskForge's Prometheus metrics. It is a model.Observer: event
// counters and the execution histogram are updated from transitions, while
// queue depth, DLQ depth, and worker heartbeat age are read from the store at
// scrape time so every API instance reports the same shared values.
type Metrics struct {
	registry  *prometheus.Registry
	enqueued  *prometheus.CounterVec
	completed *prometheus.CounterVec
	duration  *prometheus.HistogramVec
}

func NewMetrics(source GaugeSource, logger *slog.Logger) *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		enqueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_tasks_enqueued_total",
			Help: "Tasks enqueued, including dead-letter replays.",
		}, []string{"queue"}),
		completed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_tasks_completed_total",
			Help: "Task executions that finished, by outcome status (done, retry, dead).",
		}, []string{"queue", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "taskforge_task_execution_duration_seconds",
			Help:    "Time from claim to ack, nack, or watchdog reclaim.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 16),
		}, []string{"queue"}),
	}
	m.registry.MustRegister(
		m.enqueued,
		m.completed,
		m.duration,
		&gaugeCollector{source: source, logger: logger},
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

func (m *Metrics) TaskTransition(t model.Transition) {
	switch {
	case t.From == "" && t.To == model.TaskStatusPending:
		m.enqueued.WithLabelValues(t.Queue).Inc()
	case t.From == model.TaskStatusExecuting:
		m.completed.WithLabelValues(t.Queue, string(t.To)).Inc()
		m.duration.WithLabelValues(t.Queue).Observe(t.Duration.Seconds())
	}
}

// Handler serves the metrics in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

var (
	queueDepthDesc = prometheus.NewDesc(
		"taskforge_queue_depth", "Tasks in a queue by status.", []string{"queue", "status"}, nil)
	dlqDepthDesc = prometheus.NewDesc(
		"taskforge_dlq_depth", "Dead-letter tasks in a queue.", []string{"queue"}, nil)
	heartbeatAgeDesc = prometheus.NewDesc(
		"taskforge_worker_heartbeat_age_seconds", "Seconds since a worker was last seen.", []string{"worker_id"}, nil)
)

// alwaysReported are emitted (as zero when empty) for every queue that has
// any tasks so dashboards see a value instead of a gap.
var alwaysReported = []model.TaskStatus{model.TaskStatusPending, model.TaskStatusRetry, model.TaskStatusExecuting}

type gaugeCollector struct {
	source GaugeSource
	logger *slog.Logger
}

func (c *gaugeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- queueDepthDesc
	ch <- dlqDepthDesc
	ch <- heartbeatAgeDesc
}

func (c *gaugeCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	counts, err := c.source.StatusCounts(ctx)
	if err != nil {
		c.logError("collect queue depth", err)
	} else {
		seen := map[string]map[model.TaskStatus]bool{}
		for _, sc := range counts {
			if seen[sc.Queue] == nil {
				seen[sc.Queue] = map[model.TaskStatus]bool{}
			}
			seen[sc.Queue][sc.Status] = true
			if sc.Status == model.TaskStatusDead {
				ch <- prometheus.MustNewConstMetric(dlqDepthDesc, prometheus.GaugeValue, float64(sc.Count), sc.Queue)
				continue
			}
			ch <- prometheus.MustNewConstMetric(queueDepthDesc, prometheus.GaugeValue, float64(sc.Count), sc.Queue, string(sc.Status))
		}
		for queue, statuses := range seen {
			for _, status := range alwaysReported {
				if !statuses[status] {
					ch <- prometheus.MustNewConstMetric(queueDepthDesc, prometheus.GaugeValue, 0, queue, string(status))
				}
			}
			if !statuses[model.TaskStatusDead] {
				ch <- prometheus.MustNewConstMetric(dlqDepthDesc, prometheus.GaugeValue, 0, queue)
			}
		}
	}

	workers, err := c.source.ActiveWorkers(ctx)
	if err != nil {
		c.logError("collect worker heartbeat age", err)
		return
	}
	now := time.Now()
	for _, w := range workers {
		age := now.Sub(w.LastHeartbeatAt).Seconds()
		if age < 0 {
			age = 0
		}
		ch <- prometheus.MustNewConstMetric(heartbeatAgeDesc, prometheus.GaugeValue, age, w.WorkerID)
	}
}

func (c *gaugeCollector) logError(msg string, err error) {
	if c.logger != nil {
		c.logger.Error(msg, "error", err)
	}
}
