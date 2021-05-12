package client

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/spotinst/wave-operator/internal/sparkapi/client/transport/mock_transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDriverMetrics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	t.Run("whenSuccessful", func(tt *testing.T) {
		m := mock_transport.NewMockClient(ctrl)
		m.EXPECT().Get("metrics/json").Return(getMetricsResponse(), nil).Times(1)

		client := &driver{&client{m}}
		metrics, err := client.GetMetrics()
		require.NoError(tt, err)
		assert.NotNil(tt, metrics)

		assert.NotEmpty(tt, metrics.Gauges)
		assert.NotEmpty(tt, metrics.Counters)

		assert.Contains(tt, metrics.Gauges, "test-app-id.driver.BlockManager.memory.maxMem_MB")
		assert.Equal(tt, int64(350), metrics.Gauges["test-app-id.driver.BlockManager.memory.maxMem_MB"].Value)

		assert.Contains(tt, metrics.Counters, "test-app-id.driver.LiveListenerBus.numEventsPosted")
		assert.Equal(tt, int64(3720), metrics.Counters["test-app-id.driver.LiveListenerBus.numEventsPosted"].Count)
	})
	t.Run("returnsEmptyMetricsObjectOnError", func(tt *testing.T) {
		m := mock_transport.NewMockClient(ctrl)
		m.EXPECT().Get("metrics/json").Return(nil, errors.New("failed-to-get-metrics")).Times(1)

		client := &driver{&client{m}}
		metrics, err := client.GetMetrics()
		require.Error(tt, err)
		assert.NotNil(tt, metrics)

		assert.Empty(tt, metrics.Gauges)
		assert.Empty(tt, metrics.Counters)
	})
}

func TestDriverStreamingStatistics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	t.Run("whenSuccessful", func(tt *testing.T) {
		m := mock_transport.NewMockClient(ctrl)
		m.EXPECT().Get("api/v1/applications/spark-123/streaming/statistics").Return(getStreamingStatisticsResponse(), nil).Times(1)

		client := &driver{&client{m}}
		stats, err := client.GetStreamingStatistics("spark-123")
		require.NoError(tt, err)
		assert.NotNil(tt, stats)

		assert.Equal(tt, "10/10/2020", stats.StartTime)
		assert.Equal(tt, int64(999), stats.BatchDuration)
		assert.Equal(tt, int64(666), stats.AvgProcessingTime)
	})
	t.Run("whenError", func(tt *testing.T) {
		m := mock_transport.NewMockClient(ctrl)
		m.EXPECT().Get("api/v1/applications/spark-123/streaming/statistics").Return(nil, errors.New("streaming-statistics-err")).Times(1)

		client := &driver{&client{m}}
		stats, err := client.GetStreamingStatistics("spark-123")
		require.Error(tt, err)
		assert.Nil(tt, stats)
	})
}

func getStreamingStatisticsResponse() []byte {
	return []byte(`{
	"startTime": "10/10/2020",
	"batchDuration": 999,	
	"avgProcessingTime": 666
}`)
}

func getMetricsResponse() []byte {
	return []byte(`{
  "gauges": {
    "test-app-id.driver.BlockManager.disk.diskSpaceUsed_MB": {
      "value": 0
    },
    "test-app-id.driver.BlockManager.memory.maxMem_MB": {
      "value": 350
    },
    "test-app-id.driver.BlockManager.memory.maxOffHeapMem_MB": {
      "value": 0
    },
    "test-app-id.driver.BlockManager.memory.maxOnHeapMem_MB": {
      "value": 350
    },
    "test-app-id.driver.BlockManager.memory.memUsed_MB": {
      "value": 0
    },
    "test-app-id.driver.BlockManager.memory.offHeapMemUsed_MB": {
      "value": 0
    },
    "test-app-id.driver.BlockManager.memory.onHeapMemUsed_MB": {
      "value": 0
    },
    "test-app-id.driver.BlockManager.memory.remainingMem_MB": {
      "value": 350
    },
    "test-app-id.driver.BlockManager.memory.remainingOffHeapMem_MB": {
      "value": 0
    },
    "test-app-id.driver.BlockManager.memory.remainingOnHeapMem_MB": {
      "value": 350
    },
    "test-app-id.driver.DAGScheduler.job.activeJobs": {
      "value": 1
    },
    "test-app-id.driver.DAGScheduler.job.allJobs": {
      "value": 1
    },
    "test-app-id.driver.DAGScheduler.stage.failedStages": {
      "value": 0
    },
    "test-app-id.driver.DAGScheduler.stage.runningStages": {
      "value": 1
    },
    "test-app-id.driver.DAGScheduler.stage.waitingStages": {
      "value": 0
    },
    "test-app-id.driver.ExecutorMetrics.DirectPoolMemory": {
      "value": 143799
    },
    "test-app-id.driver.ExecutorMetrics.JVMHeapMemory": {
      "value": 53594776
    },
    "test-app-id.driver.ExecutorMetrics.JVMOffHeapMemory": {
      "value": 120888096
    },
    "test-app-id.driver.ExecutorMetrics.MajorGCCount": {
      "value": 4
    },
    "test-app-id.driver.ExecutorMetrics.MajorGCTime": {
      "value": 206
    },
    "test-app-id.driver.ExecutorMetrics.MappedPoolMemory": {
      "value": 0
    },
    "test-app-id.driver.ExecutorMetrics.MinorGCCount": {
      "value": 168
    },
    "test-app-id.driver.ExecutorMetrics.MinorGCTime": {
      "value": 597
    },
    "test-app-id.driver.ExecutorMetrics.OffHeapExecutionMemory": {
      "value": 0
    },
    "test-app-id.driver.ExecutorMetrics.OffHeapStorageMemory": {
      "value": 0
    },
    "test-app-id.driver.ExecutorMetrics.OffHeapUnifiedMemory": {
      "value": 0
    },
    "test-app-id.driver.ExecutorMetrics.OnHeapExecutionMemory": {
      "value": 0
    },
    "test-app-id.driver.ExecutorMetrics.OnHeapStorageMemory": {
      "value": 10600
    },
    "test-app-id.driver.ExecutorMetrics.OnHeapUnifiedMemory": {
      "value": 10600
    },
    "test-app-id.driver.ExecutorMetrics.ProcessTreeJVMRSSMemory": {
      "value": 0
    },
    "test-app-id.driver.ExecutorMetrics.ProcessTreeJVMVMemory": {
      "value": 0
    },
    "test-app-id.driver.ExecutorMetrics.ProcessTreeOtherRSSMemory": {
      "value": 0
    },
    "test-app-id.driver.ExecutorMetrics.ProcessTreeOtherVMemory": {
      "value": 0
    },
    "test-app-id.driver.ExecutorMetrics.ProcessTreePythonRSSMemory": {
      "value": 0
    },
    "test-app-id.driver.ExecutorMetrics.ProcessTreePythonVMemory": {
      "value": 0
    },
    "test-app-id.driver.JVMCPU.jvmCpuTime": {
      "value": 35320000000
    },
    "test-app-id.driver.LiveListenerBus.queue.appStatus.size": {
      "value": 0
    },
    "test-app-id.driver.LiveListenerBus.queue.eventLog.size": {
      "value": 0
    },
    "test-app-id.driver.LiveListenerBus.queue.executorManagement.size": {
      "value": 0
    },
    "test-app-id.driver.LiveListenerBus.queue.shared.size": {
      "value": 0
    },
    "test-app-id.driver.appStatus.jobDuration": {
      "value": 0
    }
  },
  "counters": {
    "test-app-id.driver.HiveExternalCatalog.fileCacheHits": {
      "count": 0
    },
    "test-app-id.driver.HiveExternalCatalog.filesDiscovered": {
      "count": 0
    },
    "test-app-id.driver.HiveExternalCatalog.hiveClientCalls": {
      "count": 0
    },
    "test-app-id.driver.HiveExternalCatalog.parallelListingJobCount": {
      "count": 0
    },
    "test-app-id.driver.HiveExternalCatalog.partitionsFetched": {
      "count": 0
    },
    "test-app-id.driver.LiveListenerBus.numEventsPosted": {
      "count": 3720
    },
    "test-app-id.driver.LiveListenerBus.queue.appStatus.numDroppedEvents": {
      "count": 0
    },
    "test-app-id.driver.LiveListenerBus.queue.eventLog.numDroppedEvents": {
      "count": 0
    },
    "test-app-id.driver.LiveListenerBus.queue.executorManagement.numDroppedEvents": {
      "count": 0
    },
    "test-app-id.driver.LiveListenerBus.queue.shared.numDroppedEvents": {
      "count": 0
    },
    "test-app-id.driver.appStatus.jobs.failedJobs": {
      "count": 0
    },
    "test-app-id.driver.appStatus.jobs.succeededJobs": {
      "count": 0
    },
    "test-app-id.driver.appStatus.stages.completedStages": {
      "count": 0
    },
    "test-app-id.driver.appStatus.stages.failedStages": {
      "count": 0
    },
    "test-app-id.driver.appStatus.stages.skippedStages": {
      "count": 0
    },
    "test-app-id.driver.appStatus.tasks.blackListedExecutors": {
      "count": 0
    },
    "test-app-id.driver.appStatus.tasks.completedTasks": {
      "count": 0
    },
    "test-app-id.driver.appStatus.tasks.failedTasks": {
      "count": 0
    },
    "test-app-id.driver.appStatus.tasks.killedTasks": {
      "count": 0
    },
    "test-app-id.driver.appStatus.tasks.skippedTasks": {
      "count": 0
    },
    "test-app-id.driver.appStatus.tasks.unblackListedExecutors": {
      "count": 0
    }
  },
  "histograms": {
    "test-app-id.driver.CodeGenerator.compilationTime": {
      "count": 0,
      "max": 0,
      "mean": 0.0,
      "min": 0,
      "p50": 0.0,
      "p75": 0.0,
      "p95": 0.0,
      "p98": 0.0,
      "p99": 0.0,
      "p999": 0.0,
      "stddev": 0.0
    },
    "test-app-id.driver.CodeGenerator.generatedClassSize": {
      "count": 0,
      "max": 0,
      "mean": 0.0,
      "min": 0,
      "p50": 0.0,
      "p75": 0.0,
      "p95": 0.0,
      "p98": 0.0,
      "p99": 0.0,
      "p999": 0.0,
      "stddev": 0.0
    },
    "test-app-id.driver.CodeGenerator.generatedMethodSize": {
      "count": 0,
      "max": 0,
      "mean": 0.0,
      "min": 0,
      "p50": 0.0,
      "p75": 0.0,
      "p95": 0.0,
      "p98": 0.0,
      "p99": 0.0,
      "p999": 0.0,
      "stddev": 0.0
    },
    "test-app-id.driver.CodeGenerator.sourceCodeSize": {
      "count": 0,
      "max": 0,
      "mean": 0.0,
      "min": 0,
      "p50": 0.0,
      "p75": 0.0,
      "p95": 0.0,
      "p98": 0.0,
      "p99": 0.0,
      "p999": 0.0,
      "stddev": 0.0
    }
  },
  "meters": {},
  "timers": {
    "test-app-id.driver.DAGScheduler.messageProcessingTime": {
      "count": 3669,
      "max": 25.063910999999997,
      "mean": 0.21188361708872067,
      "min": 0.003685,
      "p50": 0.064265,
      "p75": 0.29304399999999997,
      "p95": 0.461406,
      "p98": 0.699678,
      "p99": 1.325732,
      "p999": 8.369219,
      "stddev": 0.5842080814061481,
      "m15_rate": 3.8017209318778717,
      "m1_rate": 26.558069606836415,
      "m5_rate": 9.987576415735385,
      "mean_rate": 19.30636090379898,
      "duration_units": "milliseconds",
      "rate_units": "calls/second"
    },
    "test-app-id.driver.LiveListenerBus.listenerProcessingTime.org.apache.spark.HeartbeatReceiver": {
      "count": 3720,
      "max": 3.2790019999999998,
      "mean": 0.001305704819771357,
      "min": 0.000169,
      "p50": 0.0006839999999999999,
      "p75": 0.0010199999999999999,
      "p95": 0.001606,
      "p98": 0.0019279999999999998,
      "p99": 0.002196,
      "p999": 0.011106999999999999,
      "stddev": 0.04023162364914937,
      "m15_rate": 4.283584458967211,
      "m1_rate": 26.664137424886757,
      "m5_rate": 10.302127700001574,
      "mean_rate": 19.403132330629735,
      "duration_units": "milliseconds",
      "rate_units": "calls/second"
    },
    "test-app-id.driver.LiveListenerBus.listenerProcessingTime.org.apache.spark.scheduler.EventLoggingListener": {
      "count": 3720,
      "max": 55.973414,
      "mean": 0.1768803407989222,
      "min": 0.0012029999999999999,
      "p50": 0.177566,
      "p75": 0.23929299999999998,
      "p95": 0.30355899999999997,
      "p98": 0.35956899999999997,
      "p99": 0.630545,
      "p999": 1.975325,
      "stddev": 0.7134346305885066,
      "m15_rate": 4.375744253664815,
      "m1_rate": 26.376056871526306,
      "m5_rate": 10.238500653739987,
      "mean_rate": 19.70064138870666,
      "duration_units": "milliseconds",
      "rate_units": "calls/second"
    },
    "test-app-id.driver.LiveListenerBus.listenerProcessingTime.org.apache.spark.sql.execution.ui.SQLAppStatusListener": {
      "count": 3711,
      "max": 0.274263,
      "mean": 0.004217025627978158,
      "min": 0.0007459999999999999,
      "p50": 0.003401,
      "p75": 0.005752999999999999,
      "p95": 0.010879999999999999,
      "p98": 0.013118999999999999,
      "p99": 0.022727999999999998,
      "p999": 0.038999,
      "stddev": 0.005476910619543726,
      "m15_rate": 8.064017129281753,
      "m1_rate": 26.635462197842614,
      "m5_rate": 12.822418902561386,
      "mean_rate": 23.22986989719561,
      "duration_units": "milliseconds",
      "rate_units": "calls/second"
    },
    "test-app-id.driver.LiveListenerBus.listenerProcessingTime.org.apache.spark.sql.util.ExecutionListenerBus": {
      "count": 3710,
      "max": 0.014027,
      "mean": 0.0005763559844105249,
      "min": 0.000078,
      "p50": 0.000463,
      "p75": 0.000708,
      "p95": 0.001249,
      "p98": 0.0015539999999999998,
      "p99": 0.001782,
      "p999": 0.014027,
      "stddev": 0.0006192912013153608,
      "m15_rate": 10.283361677689928,
      "m1_rate": 26.945367085731927,
      "m5_rate": 14.446906546451412,
      "mean_rate": 23.362054857948262,
      "duration_units": "milliseconds",
      "rate_units": "calls/second"
    },
    "test-app-id.driver.LiveListenerBus.listenerProcessingTime.org.apache.spark.status.AppStatusListener": {
      "count": 3720,
      "max": 11.716474,
      "mean": 0.0793503077661801,
      "min": 0.025439,
      "p50": 0.063758,
      "p75": 0.084615,
      "p95": 0.14127399999999998,
      "p98": 0.205205,
      "p99": 0.306257,
      "p999": 1.422944,
      "stddev": 0.160838716250108,
      "m15_rate": 4.248814188865737,
      "m1_rate": 26.524692143862694,
      "m5_rate": 10.218090414417386,
      "mean_rate": 19.30243036564498,
      "duration_units": "milliseconds",
      "rate_units": "calls/second"
    },
    "test-app-id.driver.LiveListenerBus.queue.appStatus.listenerProcessingTime": {
      "count": 3720,
      "max": 7.832085999999999,
      "mean": 0.09979824618257434,
      "min": 0.033866,
      "p50": 0.0812,
      "p75": 0.10407799999999999,
      "p95": 0.167233,
      "p98": 0.244417,
      "p99": 0.316858,
      "p999": 1.90132,
      "stddev": 0.20456501505973215,
      "m15_rate": 4.248824933643075,
      "m1_rate": 26.525038217599114,
      "m5_rate": 10.218163665071895,
      "mean_rate": 19.30229091964672,
      "duration_units": "milliseconds",
      "rate_units": "calls/second"
    },
    "test-app-id.driver.LiveListenerBus.queue.eventLog.listenerProcessingTime": {
      "count": 3720,
      "max": 7.761673,
      "mean": 0.191777077956686,
      "min": 0.002892,
      "p50": 0.150946,
      "p75": 0.237256,
      "p95": 0.318845,
      "p98": 0.450563,
      "p99": 0.6922349999999999,
      "p999": 7.761673,
      "stddev": 0.46371658753473943,
      "m15_rate": 4.375744253664815,
      "m1_rate": 26.376056871526306,
      "m5_rate": 10.238500653739987,
      "mean_rate": 19.700481558918103,
      "duration_units": "milliseconds",
      "rate_units": "calls/second"
    },
    "test-app-id.driver.LiveListenerBus.queue.executorManagement.listenerProcessingTime": {
      "count": 3720,
      "max": 0.112484,
      "mean": 0.006879324757426812,
      "min": 0.000931,
      "p50": 0.006547,
      "p75": 0.008536,
      "p95": 0.012728,
      "p98": 0.016659999999999998,
      "p99": 0.021671,
      "p999": 0.112484,
      "stddev": 0.005586949192045638,
      "m15_rate": 4.283584458967211,
      "m1_rate": 26.664137424886757,
      "m5_rate": 10.302127700001574,
      "mean_rate": 19.4029185475355,
      "duration_units": "milliseconds",
      "rate_units": "calls/second"
    },
    "test-app-id.driver.LiveListenerBus.queue.shared.listenerProcessingTime": {
      "count": 3710,
      "max": 0.041361999999999996,
      "mean": 0.006475188783940848,
      "min": 0.001008,
      "p50": 0.006548,
      "p75": 0.008202,
      "p95": 0.011575,
      "p98": 0.014627,
      "p99": 0.020061,
      "p999": 0.034006999999999996,
      "stddev": 0.0035399506367425775,
      "m15_rate": 10.283361677689928,
      "m1_rate": 26.945367085731927,
      "m5_rate": 14.446906546451412,
      "mean_rate": 23.361845536773547,
      "duration_units": "milliseconds",
      "rate_units": "calls/second"
    }
  }
}`)
}
