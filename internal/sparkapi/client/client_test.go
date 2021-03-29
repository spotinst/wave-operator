package client

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/spotinst/wave-operator/internal/sparkapi/client/transport/mock_transport"
)

func TestGetApplication(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("whenError", func(tt *testing.T) {

		m := mock_transport.NewMockClient(ctrl)
		m.EXPECT().Get("api/v1/applications/spark-123").Return(nil, fmt.Errorf("test error")).Times(1)

		client := &client{DriverClient, m}

		res, err := client.GetApplication("spark-123")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "test error")
		assert.Nil(tt, res)
	})

	t.Run("whenSuccessful", func(tt *testing.T) {

		m := mock_transport.NewMockClient(ctrl)
		m.EXPECT().Get("api/v1/applications/spark-123").Return(getApplicationResponse(), nil).Times(1)

		client := &client{DriverClient, m}

		res, err := client.GetApplication("spark-123")
		assert.NoError(tt, err)
		assert.Equal(tt, "Spark Pi", res.Name)
		assert.Equal(tt, "spark-123", res.ID)
		assert.Equal(tt, 1, len(res.Attempts))
		attempt := res.Attempts[0]
		assert.Equal(tt, "3.0.0", attempt.AppSparkVersion)
		assert.Equal(tt, true, attempt.Completed)
		assert.Equal(tt, int64(1606238361000), attempt.LastUpdatedEpoch)
		assert.Equal(tt, int64(1606238359963), attempt.EndTimeEpoch)
		assert.Equal(tt, int64(1606238338797), attempt.StartTimeEpoch)
		assert.Equal(tt, int64(21166), attempt.Duration)
		assert.Equal(tt, "root", attempt.SparkUser)
	})

}

func TestGetStages(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("whenError", func(tt *testing.T) {

		m := mock_transport.NewMockClient(ctrl)
		m.EXPECT().Get("api/v1/applications/spark-123/stages").Return(nil, fmt.Errorf("test error")).Times(1)

		client := &client{DriverClient, m}

		res, err := client.GetStages("spark-123")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "test error")
		assert.Nil(tt, res)
	})

	t.Run("whenSuccessful", func(tt *testing.T) {

		m := mock_transport.NewMockClient(ctrl)
		m.EXPECT().Get("api/v1/applications/spark-123/stages").Return(getStagesResponse(), nil).Times(1)

		client := &client{DriverClient, m}

		res, err := client.GetStages("spark-123")
		assert.NoError(tt, err)
		assert.Equal(tt, 1, len(res))
		stage := res[0]
		assert.Equal(tt, int64(5555), stage.OutputBytes)
		assert.Equal(tt, int64(3333), stage.InputBytes)
		assert.Equal(tt, int64(147527368), stage.ExecutorCpuTime)
		assert.Equal(tt, 9, stage.AttemptID)
		assert.Equal(tt, 7, stage.StageID)

	})

}

func TestGetEnvironment(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("whenError", func(tt *testing.T) {

		m := mock_transport.NewMockClient(ctrl)
		m.EXPECT().Get("api/v1/applications/spark-123/environment").Return(nil, fmt.Errorf("test error")).Times(1)

		client := &client{DriverClient, m}

		res, err := client.GetEnvironment("spark-123")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "test error")
		assert.Nil(tt, res)
	})

	t.Run("whenSuccessful", func(tt *testing.T) {

		m := mock_transport.NewMockClient(ctrl)
		m.EXPECT().Get("api/v1/applications/spark-123/environment").Return(getEnvironmentResponse(), nil).Times(1)

		client := &client{DriverClient, m}

		res, err := client.GetEnvironment("spark-123")
		assert.NoError(tt, err)
		assert.Equal(tt, 5, len(res.SparkProperties))
		for _, prop := range res.SparkProperties {
			assert.Equal(tt, 2, len(prop))
		}
	})

}

func TestGetAllExecutors(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("whenError", func(tt *testing.T) {

		m := mock_transport.NewMockClient(ctrl)
		m.EXPECT().Get("api/v1/applications/spark-123/allexecutors").Return(nil, fmt.Errorf("test error")).Times(1)

		client := &client{DriverClient, m}

		res, err := client.GetAllExecutors("spark-123")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "test error")
		assert.Nil(tt, res)
	})

	t.Run("whenSuccessful", func(tt *testing.T) {

		m := mock_transport.NewMockClient(ctrl)
		m.EXPECT().Get("api/v1/applications/spark-123/allexecutors").Return(getExecutorsResponse(), nil).Times(1)

		client := &client{DriverClient, m}

		res, err := client.GetAllExecutors("spark-123")
		assert.NoError(tt, err)
		assert.Equal(tt, 3, len(res))
		for _, exec := range res {
			id := exec.ID
			assert.True(tt, id == "driver" || id == "1" || id == "2")
			assert.NotEmpty(tt, exec.AddTime)

			if id == "1" {
				assert.False(tt, exec.IsActive)
				assert.NotEmpty(tt, exec.RemoveTime)
				assert.NotEmpty(tt, exec.RemoveReason)
			} else {
				assert.True(tt, exec.IsActive)
				assert.Empty(tt, exec.RemoveTime)
				assert.Empty(tt, exec.RemoveReason)
			}

			if id == "2" {
				assert.Nil(tt, exec.PeakMemoryMetrics)
			} else {
				assert.NotNil(tt, exec.PeakMemoryMetrics)
			}
		}
	})
}

func getApplicationResponse() []byte {
	return []byte(`{
    "id": "spark-123",
    "name": "Spark Pi",
    "attempts": [
        {
            "startTime": "2020-11-24T17:18:58.797GMT",
            "endTime": "2020-11-24T17:19:19.963GMT",
            "lastUpdated": "2020-11-24T17:19:21.000GMT",
            "duration": 21166,
            "sparkUser": "root",
            "completed": true,
            "appSparkVersion": "3.0.0",
            "startTimeEpoch": 1606238338797,
            "endTimeEpoch": 1606238359963,
            "lastUpdatedEpoch": 1606238361000
        }
    ]
}`)
}

func getStagesResponse() []byte {
	return []byte(`[
    {
        "status": "COMPLETE",
        "stageID": 7,
        "attemptID": 9,
        "numTasks": 2,
        "numActiveTasks": 0,
        "numCompleteTasks": 2,
        "numFailedTasks": 0,
        "numKilledTasks": 0,
        "numCompletedIndices": 2,
        "executorRunTime": 177,
        "executorCpuTime": 147527368,
        "submissionTime": "2020-11-24T17:19:18.512GMT",
        "firstTaskLaunchedTime": "2020-11-24T17:19:18.779GMT",
        "completionTime": "2020-11-24T17:19:19.899GMT",
        "inputBytes": 3333,
        "inputRecords": 0,
        "outputBytes": 5555,
        "outputRecords": 0,
        "shuffleReadBytes": 0,
        "shuffleReadRecords": 0,
        "shuffleWriteBytes": 0,
        "shuffleWriteRecords": 0,
        "memoryBytesSpilled": 0,
        "diskBytesSpilled": 0,
        "name": "reduce at SparkPi.scala:38",
        "details": "org.apache.spark.rdd.RDD.reduce(RDD.scala:1076)\norg.apache.spark.examples.SparkPi$.main(SparkPi.scala:38)\norg.apache.spark.examples.SparkPi.main(SparkPi.scala)\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\njava.lang.reflect.Method.invoke(Method.java:498)\norg.apache.spark.deploy.JavaMainApplication.start(SparkApplication.scala:52)\norg.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:928)\norg.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:180)\norg.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:203)\norg.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:90)\norg.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:1007)\norg.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:1016)\norg.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)",
        "schedulingPool": "default",
        "rddIDs": [
            1,
            0
        ],
        "accumulatorUpdates": [],
        "killedTasksSummary": {}
    }
]`)
}

func getEnvironmentResponse() []byte {
	return []byte(`{
    "runtime": {
        "javaVersion": "1.8.0_252 (Oracle Corporation)",
        "javaHome": "/usr/local/openjdk-8",
        "scalaVersion": "version 2.12.10"
    },
    "sparkProperties": [
        [
            "spark.kubernetes.executor.label.sparkoperator.k8s.io/submission-id",
            "29944748-5813-463d-8f77-7feaca52ade2"
        ],
        [
            "spark.kubernetes.submission.waitAppCompletion",
            "false"
        ],
        [
            "spark.kubernetes.namespace",
            "spark-jobs"
        ],
        [
            "spark.eventLog.enabled",
            "true"
        ],
        [
            "spark.app.name",
            "Spark Pi"
        ]
    ],
    "systemProperties": [
        [
            "java.io.tmpdir",
            "/tmp"
        ],
        [
            "SPARK_SUBMIT",
            "true"
        ],
        [
            "sun.io.unicode.encoding",
            "UnicodeLittle"
        ]
    ],
    "classpathEntries": [
        [
            "/opt/spark/jars/velocity-1.5.jar",
            "System Classpath"
        ],
        [
            "/opt/spark/jars/spark-core_2.12-3.0.0.jar",
            "System Classpath"
        ],
        [
            "/opt/spark/jars/zjsonpatch-0.3.0.jar",
            "System Classpath"
        ]
    ]
}`)
}

func getExecutorsResponse() []byte {
	return []byte(`[ {
  "id" : "1",
  "hostPort" : "192.168.95.154:33465",
  "isActive" : false,
  "rddBlocks" : 0,
  "memoryUsed" : 332440,
  "diskUsed" : 0,
  "totalCores" : 1,
  "maxTasks" : 1,
  "activeTasks" : 2,
  "failedTasks" : 0,
  "completedTasks" : 336912,
  "totalTasks" : 336914,
  "totalDuration" : 7307466,
  "totalGCTime" : 84747,
  "totalInputBytes" : 0,
  "totalShuffleRead" : 3608932,
  "totalShuffleWrite" : 1818286,
  "isBlacklisted" : false,
  "maxMemory" : 122644070,
  "addTime" : "2021-02-17T12:16:25.750GMT",
  "removeTime" : "2021-02-17T12:16:25.750GMT",
  "removeReason" : "removed by driver",
  "executorLogs" : { },
  "memoryMetrics" : {
    "usedOnHeapStorageMemory" : 332440,
    "usedOffHeapStorageMemory" : 0,
    "totalOnHeapStorageMemory" : 122644070,
    "totalOffHeapStorageMemory" : 0
  },
  "blacklistedInStages" : [ ],
  "peakMemoryMetrics" : {
    "JVMHeapMemory" : 496777336,
    "JVMOffHeapMemory" : 113661040,
    "OnHeapExecutionMemory" : 8650752,
    "OffHeapExecutionMemory" : 0,
    "OnHeapStorageMemory" : 25279425,
    "OffHeapStorageMemory" : 0,
    "OnHeapUnifiedMemory" : 33930177,
    "OffHeapUnifiedMemory" : 0,
    "DirectPoolMemory" : 24832,
    "MappedPoolMemory" : 0,
    "ProcessTreeJVMVMemory" : 0,
    "ProcessTreeJVMRSSMemory" : 0,
    "ProcessTreePythonVMemory" : 0,
    "ProcessTreePythonRSSMemory" : 0,
    "ProcessTreeOtherVMemory" : 0,
    "ProcessTreeOtherRSSMemory" : 0,
    "MinorGCCount" : 23489,
    "MinorGCTime" : 92228,
    "MajorGCCount" : 5,
    "MajorGCTime" : 457
  },
  "attributes" : { },
  "resources" : { }
}, {
  "id" : "2",
  "hostPort" : "192.168.69.190:45371",
  "isActive" : true,
  "rddBlocks" : 0,
  "memoryUsed" : 0,
  "diskUsed" : 0,
  "totalCores" : 1,
  "maxTasks" : 1,
  "activeTasks" : 0,
  "failedTasks" : 0,
  "completedTasks" : 1660,
  "totalTasks" : 1660,
  "totalDuration" : 29593,
  "totalGCTime" : 372,
  "totalInputBytes" : 0,
  "totalShuffleRead" : 0,
  "totalShuffleWrite" : 1793162,
  "isBlacklisted" : false,
  "maxMemory" : 122644070,
  "addTime" : "2021-02-17T12:18:34.533GMT",
  "executorLogs" : { },
  "memoryMetrics" : {
    "usedOnHeapStorageMemory" : 0,
    "usedOffHeapStorageMemory" : 0,
    "totalOnHeapStorageMemory" : 122644070,
    "totalOffHeapStorageMemory" : 0
  },
  "blacklistedInStages" : [ ],
  "attributes" : { },
  "resources" : { }
}, {
  "id" : "driver",
  "hostPort" : "spark-ea65c077afeb5011-driver-svc.spark-jobs.svc:7079",
  "isActive" : true,
  "rddBlocks" : 0,
  "memoryUsed" : 408249,
  "diskUsed" : 0,
  "totalCores" : 0,
  "maxTasks" : 0,
  "activeTasks" : 0,
  "failedTasks" : 0,
  "completedTasks" : 0,
  "totalTasks" : 0,
  "totalDuration" : 0,
  "totalGCTime" : 0,
  "totalInputBytes" : 0,
  "totalShuffleRead" : 0,
  "totalShuffleWrite" : 0,
  "isBlacklisted" : false,
  "maxMemory" : 122644070,
  "addTime" : "2021-02-17T12:16:20.254GMT",
  "executorLogs" : { },
  "memoryMetrics" : {
    "usedOnHeapStorageMemory" : 408249,
    "usedOffHeapStorageMemory" : 0,
    "totalOnHeapStorageMemory" : 122644070,
    "totalOffHeapStorageMemory" : 0
  },
  "blacklistedInStages" : [ ],
  "peakMemoryMetrics" : {
    "JVMHeapMemory" : 258348320,
    "JVMOffHeapMemory" : 210167256,
    "OnHeapExecutionMemory" : 0,
    "OffHeapExecutionMemory" : 0,
    "OnHeapStorageMemory" : 18955457,
    "OffHeapStorageMemory" : 0,
    "OnHeapUnifiedMemory" : 18955457,
    "OffHeapUnifiedMemory" : 0,
    "DirectPoolMemory" : 250566,
    "MappedPoolMemory" : 0,
    "ProcessTreeJVMVMemory" : 0,
    "ProcessTreeJVMRSSMemory" : 0,
    "ProcessTreePythonVMemory" : 0,
    "ProcessTreePythonRSSMemory" : 0,
    "ProcessTreeOtherVMemory" : 0,
    "ProcessTreeOtherRSSMemory" : 0,
    "MinorGCCount" : 3831,
    "MinorGCTime" : 34745,
    "MajorGCCount" : 15,
    "MajorGCTime" : 2413
  },
  "attributes" : { },
  "resources" : { }
} ]`)
}
