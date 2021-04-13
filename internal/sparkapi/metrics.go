package sparkapi

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spotinst/wave-operator/internal/sparkapi/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var registry = make(applicationRegistry)
type applicationRegistry map[string]*applicationCollector

func (ar applicationRegistry) Register(app *ApplicationInfo) {
	// If if a collector has already been created for the application
	// Then we just update the app for the application
	if collector, ok := ar[app.ID]; ok {
		collector.app = app
		return
	}

	collector := newApplicationCollector(app)
	ar[app.ID] = collector
	metrics.Registry.MustRegister(collector)
}

type executorCollector struct {
	count           *prometheus.Desc
	inputBytesTotal *prometheus.Desc
}

func newExecutorCollector(applicationLabels prometheus.Labels) *executorCollector {
	return &executorCollector{
		inputBytesTotal: prometheus.NewDesc(
			"spark_executor_input_bytes_total",
			"Total amount of bytes processed by executor",
			[]string{"executor_id"},
			applicationLabels),
		count: prometheus.NewDesc(
			"spark_executor_count",
			"Current executor count for the application",
			nil,
			applicationLabels),
	}
}

func (e *executorCollector) Describe(descs chan<- *prometheus.Desc) {
	descs <- e.inputBytesTotal
	descs <- e.count
}

func (e *executorCollector) Collect(executors []client.Executor, metrics chan<- prometheus.Metric) {
	metrics <- prometheus.MustNewConstMetric(e.count, prometheus.GaugeValue, float64(len(executors)))
	for _, executor := range executors {
		metrics <- prometheus.MustNewConstMetric(e.inputBytesTotal, prometheus.CounterValue, float64(executor.TotalInputBytes), executor.ID)
	}
}

type applicationCollector struct {
	app           *ApplicationInfo
	executors     *executorCollector
	attemptsTotal *prometheus.Desc
}



func newApplicationCollector(info *ApplicationInfo) *applicationCollector {
	applicationLabels := prometheus.Labels{
		"application_name": info.ApplicationName,
		"application_id":   info.ID,
	}

	return &applicationCollector{
		app:       info,
		executors: newExecutorCollector(applicationLabels),
		attemptsTotal: prometheus.NewDesc(
			"spark_attempts_total",
			"Count of total attempts",
			nil,
			applicationLabels),
	}
}

func (a *applicationCollector) Describe(descs chan<- *prometheus.Desc) {
	descs <- a.attemptsTotal
	a.executors.Describe(descs)
}

func (a *applicationCollector) Collect(metrics chan<- prometheus.Metric) {
	metrics <- prometheus.MustNewConstMetric(a.attemptsTotal, prometheus.CounterValue, float64(len(a.app.Attempts)))
	a.executors.Collect(a.app.Executors, metrics)
}
