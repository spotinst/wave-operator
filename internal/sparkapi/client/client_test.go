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

		client := &client{m}

		res, err := client.GetApplication("spark-123")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "test error")
		assert.Nil(tt, res)
	})

	t.Run("whenSuccessful", func(tt *testing.T) {

		m := mock_transport.NewMockClient(ctrl)
		m.EXPECT().Get("api/v1/applications/spark-123").Return(getApplicationResponse(), nil).Times(1)

		client := &client{m}

		res, err := client.GetApplication("spark-123")
		assert.NoError(tt, err)
		assert.Equal(tt, "Spark Pi", res.Name)
		assert.Equal(tt, "spark-123", res.Id)
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

		client := &client{m}

		res, err := client.GetStages("spark-123")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "test error")
		assert.Nil(tt, res)
	})

	t.Run("whenSuccessful", func(tt *testing.T) {

		m := mock_transport.NewMockClient(ctrl)
		m.EXPECT().Get("api/v1/applications/spark-123/stages").Return(getStagesResponse(), nil).Times(1)

		client := &client{m}

		res, err := client.GetStages("spark-123")
		assert.NoError(tt, err)
		assert.Equal(tt, 1, len(res))
		stage := res[0]
		assert.Equal(tt, int64(5555), stage.OutputBytes)
		assert.Equal(tt, int64(3333), stage.InputBytes)
		assert.Equal(tt, int64(147527368), stage.ExecutorCpuTime)
		assert.Equal(tt, 9, stage.AttemptId)
		assert.Equal(tt, 7, stage.StageId)

	})

}

func TestGetEnvironment(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("whenError", func(tt *testing.T) {

		m := mock_transport.NewMockClient(ctrl)
		m.EXPECT().Get("api/v1/applications/spark-123/environment").Return(nil, fmt.Errorf("test error")).Times(1)

		client := &client{m}

		res, err := client.GetEnvironment("spark-123")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "test error")
		assert.Nil(tt, res)
	})

	t.Run("whenSuccessful", func(tt *testing.T) {

		m := mock_transport.NewMockClient(ctrl)
		m.EXPECT().Get("api/v1/applications/spark-123/environment").Return(getEnvironmentResponse(), nil).Times(1)

		client := &client{m}

		res, err := client.GetEnvironment("spark-123")
		assert.NoError(tt, err)
		assert.Equal(tt, 5, len(res.SparkProperties))
		for _, prop := range res.SparkProperties {
			assert.Equal(tt, 2, len(prop))
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
        "stageId": 7,
        "attemptId": 9,
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
        "rddIds": [
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
