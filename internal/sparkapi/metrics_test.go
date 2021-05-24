package sparkapi_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/spotinst/wave-operator/internal/sparkapi"
	sparkapiclient "github.com/spotinst/wave-operator/internal/sparkapi/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplicationRegistry(t *testing.T) {
	registry := sparkapi.NewApplicationRegistry(func() time.Time {
		return time.Unix(0, 0)
	})

	t.Run("ErrorWhenNoAppSpecified", func(tt *testing.T) {
		_, err := registry.Register(nil)
		require.Error(tt, err)
		assert.True(tt, errors.Is(err, sparkapi.ErrNoApp))
	})
	t.Run("ErrorWhenNoAppIDSpecified", func(tt *testing.T) {
		info := &sparkapi.ApplicationInfo{}
		_, err := registry.Register(info)
		require.Error(tt, err)
		assert.True(tt, errors.Is(err, sparkapi.ErrNoAppID))
	})
	t.Run("RegistersApplicationAndCreatesCollector", func(tt *testing.T) {
		info := &sparkapi.ApplicationInfo{
			ID:              "some-id",
			ApplicationName: "some-name",
			Attempts: []sparkapiclient.Attempt{
				{
					Duration: time.Unix(0, 0).Unix(),
				},
			},
		}
		collector, err := registry.Register(info)
		require.NoError(tt, err)
		assert.NotNil(tt, collector)

		expectedOutput := `
			# HELP spark_app_duration_seconds Spark application running duration in seconds
			# TYPE spark_app_duration_seconds gauge
			spark_app_duration_seconds{application_id="some-id",application_name="some-name"} 0
			# HELP spark_app_info Spark application version information
			# TYPE spark_app_info gauge
			spark_app_info{application_id="some-id",application_name="some-name",version=""} 1
			# HELP spark_executor_count Current executor count for the application
			# TYPE spark_executor_count gauge
			spark_executor_count{application_id="some-id",application_name="some-name"} 0
`

		assert.NoError(tt, testutil.CollectAndCompare(collector, strings.NewReader(expectedOutput)))
	})
	t.Run("UpdatesCurrentlyRegisteredApplication", func(tt *testing.T) {
		info := &sparkapi.ApplicationInfo{
			ID:              "update",
			ApplicationName: "update",
			Attempts: []sparkapiclient.Attempt{
				{
					Duration: time.Unix(0, 0).Unix(),
				},
			},
		}
		info.Executors = []sparkapiclient.Executor{
			{
				ID:       "0",
				IsActive: true,
			},
		}

		collector, err := registry.Register(info)
		require.NoError(tt, err)

		expectedOutput := `
			# HELP spark_app_duration_seconds Spark application running duration in seconds
			# TYPE spark_app_duration_seconds gauge
			spark_app_duration_seconds{application_id="update",application_name="update"} 0
			# HELP spark_app_info Spark application version information
			# TYPE spark_app_info gauge
			spark_app_info{application_id="update",application_name="update",version=""} 1
			# HELP spark_executor_cores_total Total amount of cpu cores available to executor
			# TYPE spark_executor_cores_total gauge
			spark_executor_cores_total{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_count Current executor count for the application
			# TYPE spark_executor_count gauge
			spark_executor_count{application_id="update",application_name="update"} 1
			# HELP spark_executor_disk_used_bytes Total amount of bytes of space used by executor
			# TYPE spark_executor_disk_used_bytes gauge
			spark_executor_disk_used_bytes{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_gc_time_total_milliseconds Total elapsed time the JVM spent in garbage collection
			# TYPE spark_executor_gc_time_total_milliseconds counter
			spark_executor_gc_time_total_milliseconds{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_info General executor info
			# TYPE spark_executor_info gauge
			spark_executor_info{active="true",add_time="",application_id="update",application_name="update",blacklisted="false",executor_id="0",removed_time=""} 1
			# HELP spark_executor_input_bytes_total Total amount of bytes processed by executor
			# TYPE spark_executor_input_bytes_total counter
			spark_executor_input_bytes_total{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_memory_bytes_max Total amount of memory available for storage
			# TYPE spark_executor_memory_bytes_max gauge
			spark_executor_memory_bytes_max{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_memory_used_bytes Total amount of bytes of memory used by executor
			# TYPE spark_executor_memory_used_bytes gauge
			spark_executor_memory_used_bytes{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_shuffle_read_bytes_total Total shuffle read bytes
			# TYPE spark_executor_shuffle_read_bytes_total counter
			spark_executor_shuffle_read_bytes_total{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_shuffle_write_bytes_total Total shuffle write bytes
			# TYPE spark_executor_shuffle_write_bytes_total counter
			spark_executor_shuffle_write_bytes_total{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_tasks_active_count Current count of active tasks on the executor
			# TYPE spark_executor_tasks_active_count gauge
			spark_executor_tasks_active_count{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_tasks_completed_total Total number of tasks the executor has completed
			# TYPE spark_executor_tasks_completed_total counter
			spark_executor_tasks_completed_total{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_tasks_failed_total Total number of failed tasks on the executor
			# TYPE spark_executor_tasks_failed_total counter
			spark_executor_tasks_failed_total{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_tasks_max Maximum number of tasks that can be run concurrently in this executor
			# TYPE spark_executor_tasks_max gauge
			spark_executor_tasks_max{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_tasks_total Total number of tasks the executor has been assigned
			# TYPE spark_executor_tasks_total counter
			spark_executor_tasks_total{application_id="update",application_name="update",executor_id="0"} 0
`
		assert.NotNil(tt, collector)
		assert.NoError(tt, testutil.CollectAndCompare(collector, strings.NewReader(expectedOutput)))

		info.Executors = append(info.Executors, sparkapiclient.Executor{
			ID:          "added-executor",
			IsActive:    true,
			ActiveTasks: 100,
			FailedTasks: 200,
			MaxMemory:   2000,
			TotalCores:  128,
		})

		info.Executors = append(info.Executors, sparkapiclient.Executor{
			ID:          "ignored-executor",
			IsActive:    false,
			ActiveTasks: 100,
			FailedTasks: 200,
			MaxMemory:   2000,
			TotalCores:  128,
		})

		collector, err = registry.Register(info)
		require.NoError(tt, err)
		assert.NotNil(tt, collector)

		expectedOutput = `
			# HELP spark_app_duration_seconds Spark application running duration in seconds
			# TYPE spark_app_duration_seconds gauge
			spark_app_duration_seconds{application_id="update",application_name="update"} 0
			# HELP spark_app_info Spark application version information
			# TYPE spark_app_info gauge
			spark_app_info{application_id="update",application_name="update",version=""} 1
			# HELP spark_executor_cores_total Total amount of cpu cores available to executor
			# TYPE spark_executor_cores_total gauge
			spark_executor_cores_total{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_cores_total{application_id="update",application_name="update",executor_id="added-executor"} 128
			# HELP spark_executor_count Current executor count for the application
			# TYPE spark_executor_count gauge
			spark_executor_count{application_id="update",application_name="update"} 2
			# HELP spark_executor_disk_used_bytes Total amount of bytes of space used by executor
			# TYPE spark_executor_disk_used_bytes gauge
			spark_executor_disk_used_bytes{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_disk_used_bytes{application_id="update",application_name="update",executor_id="added-executor"} 0
			# HELP spark_executor_gc_time_total_milliseconds Total elapsed time the JVM spent in garbage collection
			# TYPE spark_executor_gc_time_total_milliseconds counter
			spark_executor_gc_time_total_milliseconds{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_gc_time_total_milliseconds{application_id="update",application_name="update",executor_id="added-executor"} 0
			# HELP spark_executor_info General executor info
			# TYPE spark_executor_info gauge
			spark_executor_info{active="false",add_time="",application_id="update",application_name="update",blacklisted="false",executor_id="ignored-executor",removed_time=""} 1
			spark_executor_info{active="true",add_time="",application_id="update",application_name="update",blacklisted="false",executor_id="0",removed_time=""} 1
			spark_executor_info{active="true",add_time="",application_id="update",application_name="update",blacklisted="false",executor_id="added-executor",removed_time=""} 1
			# HELP spark_executor_input_bytes_total Total amount of bytes processed by executor
			# TYPE spark_executor_input_bytes_total counter
			spark_executor_input_bytes_total{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_input_bytes_total{application_id="update",application_name="update",executor_id="added-executor"} 0
			# HELP spark_executor_memory_bytes_max Total amount of memory available for storage
			# TYPE spark_executor_memory_bytes_max gauge
			spark_executor_memory_bytes_max{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_memory_bytes_max{application_id="update",application_name="update",executor_id="added-executor"} 2000
			# HELP spark_executor_memory_used_bytes Total amount of bytes of memory used by executor
			# TYPE spark_executor_memory_used_bytes gauge
			spark_executor_memory_used_bytes{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_memory_used_bytes{application_id="update",application_name="update",executor_id="added-executor"} 0
			# HELP spark_executor_shuffle_read_bytes_total Total shuffle read bytes
			# TYPE spark_executor_shuffle_read_bytes_total counter
			spark_executor_shuffle_read_bytes_total{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_shuffle_read_bytes_total{application_id="update",application_name="update",executor_id="added-executor"} 0
			# HELP spark_executor_shuffle_write_bytes_total Total shuffle write bytes
			# TYPE spark_executor_shuffle_write_bytes_total counter
			spark_executor_shuffle_write_bytes_total{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_shuffle_write_bytes_total{application_id="update",application_name="update",executor_id="added-executor"} 0
			# HELP spark_executor_tasks_active_count Current count of active tasks on the executor
			# TYPE spark_executor_tasks_active_count gauge
			spark_executor_tasks_active_count{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_tasks_active_count{application_id="update",application_name="update",executor_id="added-executor"} 100
			# HELP spark_executor_tasks_completed_total Total number of tasks the executor has completed
			# TYPE spark_executor_tasks_completed_total counter
			spark_executor_tasks_completed_total{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_tasks_completed_total{application_id="update",application_name="update",executor_id="added-executor"} 0
			# HELP spark_executor_tasks_failed_total Total number of failed tasks on the executor
			# TYPE spark_executor_tasks_failed_total counter
			spark_executor_tasks_failed_total{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_tasks_failed_total{application_id="update",application_name="update",executor_id="added-executor"} 200
			# HELP spark_executor_tasks_max Maximum number of tasks that can be run concurrently in this executor
			# TYPE spark_executor_tasks_max gauge
			spark_executor_tasks_max{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_tasks_max{application_id="update",application_name="update",executor_id="added-executor"} 0
			# HELP spark_executor_tasks_total Total number of tasks the executor has been assigned
			# TYPE spark_executor_tasks_total counter
			spark_executor_tasks_total{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_tasks_total{application_id="update",application_name="update",executor_id="added-executor"} 0
`
		assert.NoError(tt, testutil.CollectAndCompare(collector, strings.NewReader(expectedOutput)))
	})
	t.Run("RecordsApplicationSparkMetrics", func(tt *testing.T) {
		info := &sparkapi.ApplicationInfo{
			ID:              "some.id",
			ApplicationName: "some.name",
			Attempts: []sparkapiclient.Attempt{
				{
					Duration: time.Unix(0, 0).Unix(),
				},
			},
			Metrics: sparkapiclient.Metrics{
				Gauges: map[string]sparkapiclient.GaugeValue{
					"test-app-id.driver.DAGScheduler.job.activeJobs": {Value: int64(100)},
				},
				Counters: map[string]sparkapiclient.CounterValue{
					"test-app-id.driver.appStatus.jobs.failedJobs": {Count: int64(10)},
				},
			},
		}
		collector, err := registry.Register(info)
		require.NoError(tt, err)
		assert.NotNil(tt, collector)

		expectedOutput := `
			# HELP spark_DAGScheduler_job_activeJobs spark_DAGScheduler_job_activeJobs
			# TYPE spark_DAGScheduler_job_activeJobs gauge
			spark_DAGScheduler_job_activeJobs{application_id="some.id",application_name="some.name"} 100
			# HELP spark_appStatus_jobs_failedJobs spark_appStatus_jobs_failedJobs
			# TYPE spark_appStatus_jobs_failedJobs counter
			spark_appStatus_jobs_failedJobs{application_id="some.id",application_name="some.name"} 10
`
		assert.NoError(tt, testutil.CollectAndCompare(collector, strings.NewReader(expectedOutput), "spark_DAGScheduler_job_activeJobs", "spark_appStatus_jobs_failedJobs"))
	})
}
