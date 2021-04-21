package sparkapi

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spotinst/wave-operator/internal/sparkapi/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	registry = NewApplicationRegistry(time.Now)

	ErrNoApp   = errors.New("metrics: application to register can not be nil")
	ErrNoAppID = errors.New("metrics: application to register has to have an application id specified")
)

func NewApplicationRegistry(timeProvider func() time.Time) *ApplicationRegistry {
	return &ApplicationRegistry{
		collectors:   make(map[string]*applicationCollector),
		timeProvider: timeProvider,
	}
}

// ApplicationRegistry contains all registered application collectors indexed by ID
type ApplicationRegistry struct {
	collectors   map[string]*applicationCollector
	timeProvider func() time.Time
}

// Register creates a prometheus metrics collector for the specified application
// in the case the application has already been registered the collector is updated
// with the current application information
func (ar ApplicationRegistry) Register(app *ApplicationInfo) (prometheus.Collector, error) {
	if app == nil {
		return nil, ErrNoApp
	}
	if app.ID == "" {
		return nil, ErrNoAppID
	}

	// If if a collector has already been created for the application
	// Then we just update the app for the application
	if collector, ok := ar.collectors[app.ID]; ok {
		collector.app = app
		return collector, nil
	}

	collector := newApplicationCollector(app, ar.timeProvider)
	ar.collectors[app.ID] = collector
	metrics.Registry.MustRegister(collector)
	return collector, nil
}

// executorCollector is a prometheus collector for spark executors
type executorCollector struct {
	count               *prometheus.Desc
	inputBytesTotal     *prometheus.Desc
	memoryUsed          *prometheus.Desc
	diskUsed            *prometheus.Desc
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
		memoryUsed: prometheus.NewDesc(
			"spark_executor_memory_used_bytes",
			"Total amount of bytes of memory used by executor",
			[]string{"executor_id"},
			applicationLabels),
		diskUsed: prometheus.NewDesc(
			"spark_executor_disk_used_bytes",
			"Total amount of bytes of space used by executor",
			[]string{"executor_id"},
			applicationLabels),
		coresTotal: prometheus.NewDesc(
			"spark_executor_cores_total",
			"Total amount of cpu cores available to executor",
			[]string{"executor_id"},
			applicationLabels),
		activeTasks: prometheus.NewDesc(
			"spark_executor_tasks_active_count",
			"Current count of active tasks on the executor",
			[]string{"executor_id"},
			applicationLabels),
		failedTasksTotal: prometheus.NewDesc(
			"spark_executor_tasks_failed_total",
			"Total number of failed tasks on the executor",
			[]string{"executor_id"},
			applicationLabels),
		completedTasksTotal: prometheus.NewDesc(
			"spark_executor_tasks_completed_total",
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
	descs <- e.memoryUsed
	descs <- e.diskUsed
	descs <- e.coresTotal
	descs <- e.activeTasks
	descs <- e.failedTasksTotal
	descs <- e.completedTasksTotal
	descs <- e.tasksTotal
	descs <- e.count
}

func (e *executorCollector) Collect(executors []client.Executor, metrics chan<- prometheus.Metric) {
	activeExecutors := 0
	for _, executor := range executors {
		if !executor.IsActive || executor.RemoveTime != "" {
			continue
		}
		activeExecutors++
		metrics <- prometheus.MustNewConstMetric(e.inputBytesTotal, prometheus.CounterValue, float64(executor.TotalInputBytes), executor.ID)
		metrics <- prometheus.MustNewConstMetric(e.memoryUsed, prometheus.GaugeValue, float64(executor.MemoryUsed), executor.ID)
		metrics <- prometheus.MustNewConstMetric(e.diskUsed, prometheus.GaugeValue, float64(executor.DiskUsed), executor.ID)
		metrics <- prometheus.MustNewConstMetric(e.coresTotal, prometheus.GaugeValue, float64(executor.TotalCores), executor.ID)
		metrics <- prometheus.MustNewConstMetric(e.activeTasks, prometheus.GaugeValue, float64(executor.ActiveTasks), executor.ID)
		metrics <- prometheus.MustNewConstMetric(e.failedTasksTotal, prometheus.CounterValue, float64(executor.FailedTasks), executor.ID)
		metrics <- prometheus.MustNewConstMetric(e.completedTasksTotal, prometheus.CounterValue, float64(executor.CompletedTasks), executor.ID)
		metrics <- prometheus.MustNewConstMetric(e.tasksTotal, prometheus.CounterValue, float64(executor.TotalTasks), executor.ID)
	}
	metrics <- prometheus.MustNewConstMetric(e.count, prometheus.GaugeValue, float64(activeExecutors))
}

// applicationCollector is a prometheus collector that collects information for the specific spark application
type applicationCollector struct {
	app             *ApplicationInfo
	timeProvider    func() time.Time
	info            *prometheus.Desc
	durationSeconds *prometheus.Desc
	executors       *executorCollector
}

func newApplicationCollector(info *ApplicationInfo, timeProvider func() time.Time) *applicationCollector {
	applicationLabels := prometheus.Labels{
		"application_name": info.ApplicationName,
		"application_id":   info.ID,
	}

	return &applicationCollector{
		app: info,
		timeProvider: timeProvider,
		info: prometheus.NewDesc("spark_app_info",
			"Spark application version information",
			[]string{"version"},
			applicationLabels),
		durationSeconds: prometheus.NewDesc("spark_app_duration_seconds",
			"Spark application running duration in seconds",
			nil,
			applicationLabels),
		executors: newExecutorCollector(applicationLabels),
	}
}

func (a *applicationCollector) Describe(descs chan<- *prometheus.Desc) {
	descs <- a.info
	descs <- a.durationSeconds
	a.executors.Describe(descs)
}

func (a *applicationCollector) Collect(metrics chan<- prometheus.Metric) {
	metrics <- prometheus.MustNewConstMetric(a.info, prometheus.GaugeValue, 1, a.app.Attempts[0].AppSparkVersion)
	metrics <- prometheus.MustNewConstMetric(a.durationSeconds, prometheus.GaugeValue, float64(a.timeProvider().Unix()-(a.app.Attempts[0].StartTimeEpoch/1000)))
	a.executors.Collect(a.app.Executors, metrics)
}
