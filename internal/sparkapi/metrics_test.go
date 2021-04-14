package sparkapi

import (
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	sparkapiclient "github.com/spotinst/wave-operator/internal/sparkapi/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplicationRegistry(t *testing.T) {
	t.Run("ErrorWhenNoAppSpecified", func(tt *testing.T) {
		err := registry.Register(nil)
		require.Error(tt, err)
		assert.True(tt, errors.Is(err, ErrNoApp))
	})
	t.Run("ErrorWhenNoAppIDSpecified", func(tt *testing.T) {
		info := &ApplicationInfo{}
		err := registry.Register(info)
		require.Error(tt, err)
		assert.True(tt, errors.Is(err, ErrNoAppID))
	})
	t.Run("RegistersApplicationAndCreatesCollector", func(tt *testing.T) {
		info := &ApplicationInfo{
			ID:              "some-id",
			ApplicationName: "some-name",
		}
		require.NoError(tt, registry.Register(info))

		collector := registry[info.ID]
		assert.NotNil(tt, collector)

		expectedOutput := `
			# HELP spark_attempts_total Count of total attempts
        	# TYPE spark_attempts_total counter
        	spark_attempts_total{application_id="some-id",application_name="some-name"} 0
        	# HELP spark_executor_count Current executor count for the application
        	# TYPE spark_executor_count gauge
        	spark_executor_count{application_id="some-id",application_name="some-name"} 0
`

		assert.NoError(tt, testutil.CollectAndCompare(collector, strings.NewReader(expectedOutput)))
	})
	t.Run("UpdatesCurrentlyRegisteredApplication", func(tt *testing.T) {
		info := &ApplicationInfo{
			ID:              "update",
			ApplicationName: "update",
		}
		info.Executors = []sparkapiclient.Executor{
			{
				ID: "0",
			},
		}

		require.NoError(tt, registry.Register(info))
		collector := registry[info.ID]

		expectedOutput := `
			# HELP spark_attempts_total Count of total attempts
			# TYPE spark_attempts_total counter
			spark_attempts_total{application_id="update",application_name="update"} 0
			# HELP spark_executor_active_tasks Current count of active tasks on the executor
			# TYPE spark_executor_active_tasks gauge
			spark_executor_active_tasks{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_completed_tasks_total Total number of tasks the executor has completed
			# TYPE spark_executor_completed_tasks_total counter
			spark_executor_completed_tasks_total{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_cores_total Total amount of cpu cores available to executor
			# TYPE spark_executor_cores_total gauge
			spark_executor_cores_total{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_count Current executor count for the application
			# TYPE spark_executor_count gauge
			spark_executor_count{application_id="update",application_name="update"} 1
			# HELP spark_executor_disk_used_bytes_total Total amount of bytes of space used by executor
			# TYPE spark_executor_disk_used_bytes_total counter
			spark_executor_disk_used_bytes_total{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_failed_tasks_total Total number of failed tasks on the executor
			# TYPE spark_executor_failed_tasks_total counter
			spark_executor_failed_tasks_total{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_input_bytes_total Total amount of bytes processed by executor
			# TYPE spark_executor_input_bytes_total counter
			spark_executor_input_bytes_total{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_memory_used_bytes_total Total amount of bytes of memory used by executor
			# TYPE spark_executor_memory_used_bytes_total counter
			spark_executor_memory_used_bytes_total{application_id="update",application_name="update",executor_id="0"} 0
			# HELP spark_executor_tasks_total Total number of tasks the executor has been assigned
			# TYPE spark_executor_tasks_total counter
			spark_executor_tasks_total{application_id="update",application_name="update",executor_id="0"} 0
`
		assert.NotNil(tt, collector)
		assert.NoError(tt, testutil.CollectAndCompare(collector, strings.NewReader(expectedOutput)))

		info.Executors = append(info.Executors, sparkapiclient.Executor{
			ID:          "added-executor",
			ActiveTasks: 100,
			FailedTasks: 200,
			MaxMemory:   2000,
			TotalCores:  128,
		})

		require.NoError(tt, registry.Register(info))
		collector = registry[info.ID]
		assert.NotNil(tt, collector)

		expectedOutput = `
			# HELP spark_attempts_total Count of total attempts
			# TYPE spark_attempts_total counter
			spark_attempts_total{application_id="update",application_name="update"} 0
			# HELP spark_executor_active_tasks Current count of active tasks on the executor
			# TYPE spark_executor_active_tasks gauge
			spark_executor_active_tasks{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_active_tasks{application_id="update",application_name="update",executor_id="added-executor"} 100
			# HELP spark_executor_completed_tasks_total Total number of tasks the executor has completed
			# TYPE spark_executor_completed_tasks_total counter
			spark_executor_completed_tasks_total{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_completed_tasks_total{application_id="update",application_name="update",executor_id="added-executor"} 0
			# HELP spark_executor_cores_total Total amount of cpu cores available to executor
			# TYPE spark_executor_cores_total gauge
			spark_executor_cores_total{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_cores_total{application_id="update",application_name="update",executor_id="added-executor"} 128
			# HELP spark_executor_count Current executor count for the application
			# TYPE spark_executor_count gauge
			spark_executor_count{application_id="update",application_name="update"} 2
			# HELP spark_executor_disk_used_bytes_total Total amount of bytes of space used by executor
			# TYPE spark_executor_disk_used_bytes_total counter
			spark_executor_disk_used_bytes_total{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_disk_used_bytes_total{application_id="update",application_name="update",executor_id="added-executor"} 0
			# HELP spark_executor_failed_tasks_total Total number of failed tasks on the executor
			# TYPE spark_executor_failed_tasks_total counter
			spark_executor_failed_tasks_total{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_failed_tasks_total{application_id="update",application_name="update",executor_id="added-executor"} 200
			# HELP spark_executor_input_bytes_total Total amount of bytes processed by executor
			# TYPE spark_executor_input_bytes_total counter
			spark_executor_input_bytes_total{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_input_bytes_total{application_id="update",application_name="update",executor_id="added-executor"} 0
			# HELP spark_executor_memory_used_bytes_total Total amount of bytes of memory used by executor
			# TYPE spark_executor_memory_used_bytes_total counter
			spark_executor_memory_used_bytes_total{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_memory_used_bytes_total{application_id="update",application_name="update",executor_id="added-executor"} 0
			# HELP spark_executor_tasks_total Total number of tasks the executor has been assigned
			# TYPE spark_executor_tasks_total counter
			spark_executor_tasks_total{application_id="update",application_name="update",executor_id="0"} 0
			spark_executor_tasks_total{application_id="update",application_name="update",executor_id="added-executor"} 0
`
		assert.NoError(tt, testutil.CollectAndCompare(collector, strings.NewReader(expectedOutput)))
	})
}
