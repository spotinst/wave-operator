package sparkapi

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

type applicationRegistry struct {
	collectors map[string]*applicationCollector
}

func newApplicationRegistry() *applicationRegistry {
	return &applicationRegistry{
		collectors: make(map[string]*applicationCollector),
	}
}

type applicationCollector struct {
	app             *ApplicationInfo
	inputBytesTotal *prometheus.Desc
	executorCount   *prometheus.Desc
	attemptsTotal   *prometheus.Desc
}

func (ar *applicationRegistry) Register(app *ApplicationInfo) {
	// If if a collector has already been created for the application
	// Then we just update the app for the application
	if collector, ok := ar.collectors[app.ID]; ok {
		collector.app = app
		return
	}

	collector := newApplicationCollector(app)
	ar.collectors[app.ID] = collector
	metrics.Registry.MustRegister(collector)
}

func newApplicationCollector(info *ApplicationInfo) *applicationCollector {
	applicationLabels := prometheus.Labels{"application_name": info.ApplicationName, "application_id": info.ID}
	return &applicationCollector{
		app: info,
		inputBytesTotal: prometheus.NewDesc(
			"spark_executor_input_bytes_total",
			"Total amount of bytes processed by executor",
			[]string{"executor_id"},
			applicationLabels),
		executorCount: prometheus.NewDesc(
			"spark_executor_count",
			"Current executor count for the application",
			nil,
			applicationLabels),
		attemptsTotal: prometheus.NewDesc(
			"spark_attempts_total",
			"Count of total attempts",
			nil,
			applicationLabels),
	}
}

func (a *applicationCollector) Describe(descs chan<- *prometheus.Desc) {
	descs <- a.inputBytesTotal
	descs <- a.executorCount
	descs <- a.attemptsTotal
}

func (a *applicationCollector) Collect(metrics chan<- prometheus.Metric) {
	metrics <- prometheus.MustNewConstMetric(a.attemptsTotal, prometheus.CounterValue, float64(len(a.app.Attempts)))

	metrics <- prometheus.MustNewConstMetric(a.executorCount, prometheus.GaugeValue, float64(len(a.app.Executors)))
	for _, executor := range a.app.Executors {
		metrics <- prometheus.MustNewConstMetric(a.inputBytesTotal, prometheus.CounterValue, float64(executor.TotalInputBytes), executor.ID)
	}
}
