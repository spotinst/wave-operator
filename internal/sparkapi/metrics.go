package sparkapi

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spotinst/wave-operator/internal/sparkapi/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var registry = make(ApplicationRegistry)

// ApplicationRegistry contains all registered application collectors indexed by ID
type ApplicationRegistry map[string]*applicationCollector

// Register creates a prometheus metrics collector for the specified application
// in the case the application has already been registered the collector is updated
// with the current application information
func (ar ApplicationRegistry) Register(app *ApplicationInfo) {
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

// executorCollector is a prometheus collector for spark executors
type executorCollector struct {
	count               *prometheus.Desc
	inputBytesTotal     *prometheus.Desc
	memoryUsedTotal     *prometheus.Desc
	diskUsedTotal       *prometheus.Desc
	coresTotal          *prometheus.Desc
	activeTasks         *prometheus.Desc
	failedTasksTotal    *prometheus.Desc
	completedTasksTotal *prometheus.Desc
	tasksTotal          *prometheus.Desc
}

// newExecutorCollector creates a new executorCollector where the specified applicationLabels
// are set as const labels for each metric that is collected
func newExecutorCollector(applicationLabels prometheus.Labels) *executorCollector {
	return &executorCollector{
		inputBytesTotal: prometheus.NewDesc(
			"spark_executor_input_bytes_total",
			"Total amount of bytes processed by executor",
			[]string{"executor_id"},
			applicationLabels),
		memoryUsedTotal: prometheus.NewDesc(
			"spark_executor_memory_used_bytes_total",
			"Total amount of bytes of memory used by executor",
			[]string{"executor_id"},
			applicationLabels),
		diskUsedTotal: prometheus.NewDesc(
			"spark_executor_disk_used_bytes_total",
			"Total amount of bytes of space used by executor",
			[]string{"executor_id"},
			applicationLabels),
		coresTotal: prometheus.NewDesc(
			"spark_executor_cores_total",
			"Total amount of cpu cores available to executor",
			[]string{"executor_id"},
			applicationLabels),
		activeTasks: prometheus.NewDesc(
			"spark_executor_active_tasks",
			"Current count of active tasks on the executor",
			[]string{"executor_id"},
			applicationLabels),
		failedTasksTotal: prometheus.NewDesc(
			"spark_executor_failed_tasks_total",
			"Total number of failed tasks on the executor",
			[]string{"executor_id"},
			applicationLabels),
		completedTasksTotal: prometheus.NewDesc(
			"spark_executor_completed_tasks_total",
			"Total number of tasks the executor has completed",
			[]string{"executor_id"},
			applicationLabels),
		tasksTotal: prometheus.NewDesc(
			"spark_executor_tasks_total",
			"Total number of tasks the executor has been assigned",
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
	descs <- e.memoryUsedTotal
	descs <- e.diskUsedTotal
	descs <- e.coresTotal
	descs <- e.activeTasks
	descs <- e.failedTasksTotal
	descs <- e.completedTasksTotal
	descs <- e.tasksTotal
	descs <- e.count
}

func (e *executorCollector) Collect(executors []client.Executor, metrics chan<- prometheus.Metric) {
	for _, executor := range executors {
		metrics <- prometheus.MustNewConstMetric(e.inputBytesTotal, prometheus.CounterValue, float64(executor.TotalInputBytes), executor.ID)
		metrics <- prometheus.MustNewConstMetric(e.memoryUsedTotal, prometheus.CounterValue, float64(executor.MemoryUsed), executor.ID)
		metrics <- prometheus.MustNewConstMetric(e.diskUsedTotal, prometheus.CounterValue, float64(executor.DiskUsed), executor.ID)
		metrics <- prometheus.MustNewConstMetric(e.coresTotal, prometheus.GaugeValue, float64(executor.TotalCores), executor.ID)
		metrics <- prometheus.MustNewConstMetric(e.activeTasks, prometheus.GaugeValue, float64(executor.ActiveTasks), executor.ID)
		metrics <- prometheus.MustNewConstMetric(e.failedTasksTotal, prometheus.CounterValue, float64(executor.FailedTasks), executor.ID)
		metrics <- prometheus.MustNewConstMetric(e.completedTasksTotal, prometheus.CounterValue, float64(executor.CompletedTasks), executor.ID)
		metrics <- prometheus.MustNewConstMetric(e.tasksTotal, prometheus.CounterValue, float64(executor.TotalTasks), executor.ID)
	}
	metrics <- prometheus.MustNewConstMetric(e.count, prometheus.GaugeValue, float64(len(executors)))
}

// applicationCollector is a prometheus collector that collects information for the specific spark application
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
