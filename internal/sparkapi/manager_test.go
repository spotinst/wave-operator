package sparkapi

import (
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/spotinst/wave-operator/catalog"
	sparkapiclient "github.com/spotinst/wave-operator/internal/sparkapi/client"
	"github.com/spotinst/wave-operator/internal/sparkapi/client/mock_client"
)

func TestGetSparkApiClient(t *testing.T) {

	logger := getTestLogger()

	t.Run("whenDriverAvailable", func(tt *testing.T) {

		svc := newHistoryServerService()
		pod := newRunningDriverPod()

		clientSet := k8sfake.NewSimpleClientset(svc, pod)

		clientType, c, err := getSparkApiClient(clientSet, pod, logger)
		assert.NoError(tt, err)
		assert.NotNil(tt, c)
		assert.Equal(tt, driverClient, clientType)

	})

	t.Run("whenDriverNotAvailableHistoryServerAvailable", func(tt *testing.T) {

		svc := newHistoryServerService()
		pod := newRunningDriverPod()
		pod.Status.Phase = corev1.PodSucceeded // Driver not running

		clientSet := k8sfake.NewSimpleClientset(svc, pod)

		clientType, c, err := getSparkApiClient(clientSet, pod, logger)
		assert.NoError(tt, err)
		assert.NotNil(tt, c)
		assert.Equal(tt, historyServerClient, clientType)

	})

	t.Run("whenHistoryServerNotAvailableDriverNotAvailable", func(tt *testing.T) {

		pod := newRunningDriverPod()
		pod.Status.Phase = corev1.PodSucceeded // Driver not running

		clientSet := k8sfake.NewSimpleClientset(pod)

		clientType, c, err := getSparkApiClient(clientSet, pod, logger)
		assert.Error(tt, err)
		assert.Nil(tt, c)
		assert.Equal(tt, sparkClientType(""), clientType)

	})

}

func TestGetApplicationInfo(t *testing.T) {

	logger := getTestLogger()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	applicationID := "spark-123"

	t.Run("whenError", func(tt *testing.T) {

		m := mock_client.NewMockClient(ctrl)
		m.EXPECT().GetApplication(applicationID).Return(getApplicationResponse(), nil).Times(1)
		m.EXPECT().GetEnvironment(applicationID).Return(nil, fmt.Errorf("test error")).Times(1)
		m.EXPECT().GetStages(applicationID).Return(getStagesResponse(), nil).Times(0)
		m.EXPECT().GetAllExecutors(applicationID).Return(getExecutorsResponse(), nil).Times(0)

		manager := &manager{
			client: m,
			logger: getTestLogger(),
		}

		_, err := manager.GetApplicationInfo(applicationID, -1, logger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "test error")

	})

	t.Run("whenSuccessful", func(tt *testing.T) {

		m := mock_client.NewMockClient(ctrl)
		m.EXPECT().GetApplication(applicationID).Return(getApplicationResponse(), nil).Times(1)
		m.EXPECT().GetEnvironment(applicationID).Return(getEnvironmentResponse(), nil).Times(1)
		m.EXPECT().GetStages(applicationID).Return(getStagesResponse(), nil).Times(1)
		m.EXPECT().GetAllExecutors(applicationID).Return(getExecutorsResponse(), nil).Times(1)
		m.EXPECT().GetStreamingStatistics(applicationID).Times(0)

		manager := &manager{
			client: m,
			logger: getTestLogger(),
		}

		res, err := manager.GetApplicationInfo(applicationID, -1, logger)
		assert.NoError(tt, err)

		assert.Equal(tt, "my-test-application", res.ApplicationName)

		assert.Equal(tt, int64(900), res.TotalNewExecutorCpuTime)
		assert.Equal(tt, int64(500), res.TotalNewInputBytes)
		assert.Equal(tt, int64(700), res.TotalNewOutputBytes)
		assert.Equal(tt, 2, res.MaxProcessedStageID)

		assert.Equal(tt, 2, len(res.SparkProperties))
		assert.Equal(tt, "val1", res.SparkProperties["prop1"])
		assert.Equal(tt, "val2", res.SparkProperties["prop2"])

		assert.Equal(tt, 2, len(res.Attempts))
		assert.Equal(tt, getApplicationResponse().Attempts[0], res.Attempts[0])
		assert.Equal(tt, getApplicationResponse().Attempts[1], res.Attempts[1])

		assert.Equal(tt, 3, len(res.Executors))
		assert.Equal(tt, getExecutorsResponse()[0], res.Executors[0])
		assert.Equal(tt, getExecutorsResponse()[1], res.Executors[1])
		assert.Equal(tt, getExecutorsResponse()[2], res.Executors[2])

		assert.Equal(tt, WorkloadType(""), res.WorkloadType)
	})

	t.Run("whenDriverClient_notSparkStreaming", func(tt *testing.T) {

		m := mock_client.NewMockClient(ctrl)
		m.EXPECT().GetApplication(applicationID).Return(getApplicationResponse(), nil).Times(1)
		m.EXPECT().GetEnvironment(applicationID).Return(getEnvironmentResponse(), nil).Times(1)
		m.EXPECT().GetStages(applicationID).Return(getStagesResponse(), nil).Times(1)
		m.EXPECT().GetAllExecutors(applicationID).Return(getExecutorsResponse(), nil).Times(1)
		m.EXPECT().GetStreamingStatistics(applicationID).Return(nil, fmt.Errorf("404 not found")).Times(1)

		manager := &manager{
			clientType: driverClient,
			client:     m,
			logger:     getTestLogger(),
		}

		res, err := manager.GetApplicationInfo(applicationID, -1, logger)
		assert.NoError(tt, err)

		assert.Equal(tt, WorkloadType(""), res.WorkloadType)
	})

	t.Run("whenDriverClient_sparkStreaming", func(tt *testing.T) {

		m := mock_client.NewMockClient(ctrl)
		m.EXPECT().GetApplication(applicationID).Return(getApplicationResponse(), nil).Times(1)
		m.EXPECT().GetEnvironment(applicationID).Return(getEnvironmentResponse(), nil).Times(1)
		m.EXPECT().GetStages(applicationID).Return(getStagesResponse(), nil).Times(1)
		m.EXPECT().GetAllExecutors(applicationID).Return(getExecutorsResponse(), nil).Times(1)
		m.EXPECT().GetStreamingStatistics(applicationID).Return(getStreamingStatisticsResponse(), nil).Times(1)

		manager := &manager{
			clientType: driverClient,
			client:     m,
			logger:     getTestLogger(),
		}

		res, err := manager.GetApplicationInfo(applicationID, -1, logger)
		assert.NoError(tt, err)

		assert.Equal(tt, SparkStreaming, res.WorkloadType)
	})

	t.Run("whenHistoryServerClient_dontCheckSparkStreaming", func(tt *testing.T) {

		m := mock_client.NewMockClient(ctrl)
		m.EXPECT().GetApplication(applicationID).Return(getApplicationResponse(), nil).Times(1)
		m.EXPECT().GetEnvironment(applicationID).Return(getEnvironmentResponse(), nil).Times(1)
		m.EXPECT().GetStages(applicationID).Return(getStagesResponse(), nil).Times(1)
		m.EXPECT().GetAllExecutors(applicationID).Return(getExecutorsResponse(), nil).Times(1)
		m.EXPECT().GetStreamingStatistics(applicationID).Times(0)

		manager := &manager{
			clientType: historyServerClient,
			client:     m,
			logger:     getTestLogger(),
		}

		res, err := manager.GetApplicationInfo(applicationID, -1, logger)
		assert.NoError(tt, err)

		assert.Equal(tt, WorkloadType(""), res.WorkloadType)
	})

}

func TestStageAggregation(t *testing.T) {

	logger := getTestLogger()

	const inputBytesPerStage = 100
	const outputBytesPerStage = 150
	const cpuTimePerStage = 200

	getStages := func(statuses []string) []sparkapiclient.Stage {
		stages := make([]sparkapiclient.Stage, len(statuses))
		for i := 0; i < len(statuses); i++ {
			stage := sparkapiclient.Stage{
				Status:          statuses[i],
				StageID:         i,
				AttemptID:       1,
				InputBytes:      inputBytesPerStage,
				OutputBytes:     outputBytesPerStage,
				ExecutorCpuTime: cpuTimePerStage,
			}
			stages[i] = stage
		}
		return stages
	}

	type testCase struct {
		statuses               []string
		oldMaxProcessedStageID int
		expectedResult         stageWindowAggregationResult
		message                string
	}

	testCases := []testCase{
		{
			statuses:               []string{},
			oldMaxProcessedStageID: -1,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     0,
				totalNewInputBytes:      0,
				totalNewExecutorCpuTime: 0,
				newMaxProcessedStageID:  -1,
			},
			message: "whenNoStagesReceived",
		},
		{
			statuses:               []string{"COMPLETE", "COMPLETE", "COMPLETE", "ACTIVE"},
			oldMaxProcessedStageID: -1,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     3 * outputBytesPerStage,
				totalNewInputBytes:      3 * inputBytesPerStage,
				totalNewExecutorCpuTime: 3 * cpuTimePerStage,
				newMaxProcessedStageID:  2,
			},
			message: "whenStagesCompleteInOrder_1",
		},
		{
			statuses:               []string{"COMPLETE", "COMPLETE", "COMPLETE", "ACTIVE"},
			oldMaxProcessedStageID: 0,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     2 * outputBytesPerStage,
				totalNewInputBytes:      2 * inputBytesPerStage,
				totalNewExecutorCpuTime: 2 * cpuTimePerStage,
				newMaxProcessedStageID:  2,
			},
			message: "whenStagesCompleteInOrder_2",
		},
		{
			statuses:               []string{"COMPLETE", "COMPLETE", "COMPLETE", "ACTIVE"},
			oldMaxProcessedStageID: 1,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     1 * outputBytesPerStage,
				totalNewInputBytes:      1 * inputBytesPerStage,
				totalNewExecutorCpuTime: 1 * cpuTimePerStage,
				newMaxProcessedStageID:  2,
			},
			message: "whenStagesCompleteInOrder_3",
		},
		{
			statuses:               []string{"COMPLETE", "COMPLETE", "COMPLETE", "ACTIVE"},
			oldMaxProcessedStageID: 2,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     0,
				totalNewInputBytes:      0,
				totalNewExecutorCpuTime: 0,
				newMaxProcessedStageID:  2,
			},
			message: "whenStagesCompleteInOrder_4",
		},
		{
			statuses:               []string{"COMPLETE", "COMPLETE", "COMPLETE", "COMPLETE"},
			oldMaxProcessedStageID: 2,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     1 * outputBytesPerStage,
				totalNewInputBytes:      1 * inputBytesPerStage,
				totalNewExecutorCpuTime: 1 * cpuTimePerStage,
				newMaxProcessedStageID:  3,
			},
			message: "whenStagesCompleteInOrder_5",
		},
		{
			statuses:               []string{"COMPLETE", "COMPLETE", "COMPLETE", "COMPLETE"},
			oldMaxProcessedStageID: -1,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     4 * outputBytesPerStage,
				totalNewInputBytes:      4 * inputBytesPerStage,
				totalNewExecutorCpuTime: 4 * cpuTimePerStage,
				newMaxProcessedStageID:  3,
			},
			message: "whenStagesCompleteInOrder_6",
		},
		{
			statuses:               []string{"COMPLETE", "COMPLETE", "ACTIVE", "SKIPPED", "ACTIVE", "COMPLETE"},
			oldMaxProcessedStageID: -1,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     2 * outputBytesPerStage,
				totalNewInputBytes:      2 * inputBytesPerStage,
				totalNewExecutorCpuTime: 2 * cpuTimePerStage,
				newMaxProcessedStageID:  1,
			},
			message: "whenStagesCompleteOutOfOrder_1",
		},
		{
			statuses:               []string{"COMPLETE", "COMPLETE", "ACTIVE", "SKIPPED", "ACTIVE", "COMPLETE"},
			oldMaxProcessedStageID: 0,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     1 * outputBytesPerStage,
				totalNewInputBytes:      1 * inputBytesPerStage,
				totalNewExecutorCpuTime: 1 * cpuTimePerStage,
				newMaxProcessedStageID:  1,
			},
			message: "whenStagesCompleteOutOfOrder_2",
		},
		{
			statuses:               []string{"COMPLETE", "COMPLETE", "ACTIVE", "SKIPPED", "ACTIVE", "COMPLETE"},
			oldMaxProcessedStageID: 1,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     0,
				totalNewInputBytes:      0,
				totalNewExecutorCpuTime: 0,
				newMaxProcessedStageID:  1,
			},
			message: "whenStagesCompleteOutOfOrder_3",
		},
		{
			statuses:               []string{"COMPLETE", "COMPLETE", "COMPLETE", "SKIPPED", "ACTIVE", "COMPLETE"},
			oldMaxProcessedStageID: 1,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     2 * outputBytesPerStage,
				totalNewInputBytes:      2 * inputBytesPerStage,
				totalNewExecutorCpuTime: 2 * cpuTimePerStage,
				newMaxProcessedStageID:  3,
			},
			message: "whenStagesCompleteOutOfOrder_4",
		},
		{
			statuses:               []string{"COMPLETE", "COMPLETE", "COMPLETE", "SKIPPED", "COMPLETE", "COMPLETE"},
			oldMaxProcessedStageID: 1,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     4 * outputBytesPerStage,
				totalNewInputBytes:      4 * inputBytesPerStage,
				totalNewExecutorCpuTime: 4 * cpuTimePerStage,
				newMaxProcessedStageID:  5,
			},
			message: "whenStagesCompleteOutOfOrder_5",
		},
		{
			statuses:               []string{"COMPLETE", "FAILED", "SKIPPED", "PENDING", "ACTIVE", "COMPLETE"},
			oldMaxProcessedStageID: -1,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     3 * outputBytesPerStage,
				totalNewInputBytes:      3 * inputBytesPerStage,
				totalNewExecutorCpuTime: 3 * cpuTimePerStage,
				newMaxProcessedStageID:  2,
			},
			message: "whenStagesCompleteOutOfOrder_6",
		},
		{
			statuses:               []string{"COMPLETE", "FAILED", "SKIPPED", "PENDING", "ACTIVE", "COMPLETE"},
			oldMaxProcessedStageID: 100,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     0,
				totalNewInputBytes:      0,
				totalNewExecutorCpuTime: 0,
				newMaxProcessedStageID:  100,
			},
			message: "whenOnlyOldStagesReceived",
		},
	}

	for _, tc := range testCases {

		stages := getStages(tc.statuses)
		res := aggregateStagesWindow(stages, tc.oldMaxProcessedStageID, logger)

		assert.Equal(t, tc.expectedResult.newMaxProcessedStageID, res.newMaxProcessedStageID, tc.message)
		assert.Equal(t, tc.expectedResult.totalNewExecutorCpuTime, res.totalNewExecutorCpuTime, tc.message)
		assert.Equal(t, tc.expectedResult.totalNewInputBytes, res.totalNewInputBytes, tc.message)
		assert.Equal(t, tc.expectedResult.totalNewOutputBytes, res.totalNewOutputBytes, tc.message)
	}

	t.Run("whenStagesReceivedOutOfOrder", func(tt *testing.T) {

		stages := getStages([]string{"COMPLETE", "COMPLETE", "COMPLETE", "ACTIVE", "COMPLETE", "ACTIVE"})
		stages[0], stages[3] = stages[3], stages[0]
		stages[2], stages[5] = stages[5], stages[2]

		res := aggregateStagesWindow(stages, -1, logger)
		assert.Equal(t, 2, res.newMaxProcessedStageID)
		assert.Equal(t, int64(3*cpuTimePerStage), res.totalNewExecutorCpuTime)
		assert.Equal(t, int64(3*inputBytesPerStage), res.totalNewInputBytes)
		assert.Equal(t, int64(3*outputBytesPerStage), res.totalNewOutputBytes)
	})

	t.Run("whenStagesMissed", func(tt *testing.T) {

		// Should not log error, no stages received
		aggregateStagesWindow([]sparkapiclient.Stage{}, -1, logger)

		stages := getStages([]string{"COMPLETE", "COMPLETE"})

		// Should not log error, no stages seen before
		aggregateStagesWindow(stages, -1, logger)

		stages[0].StageID = 4
		stages[1].StageID = 5

		// Should not log error
		aggregateStagesWindow(stages, 3, logger)
		// Should not log error
		aggregateStagesWindow(stages, 4, logger)
		// Should not log error, no new stages received
		aggregateStagesWindow(stages, 5, logger)
		// Should not log error, no new stages received
		aggregateStagesWindow(stages, 6, logger)
		// Should log error, we missed stage 0
		aggregateStagesWindow(stages, -1, logger)
		// Should log error, we missed stage 1
		aggregateStagesWindow(stages, 0, logger)
		// Should log error, we missed stage 2
		aggregateStagesWindow(stages, 1, logger)
		// Should log error, we missed stage 3
		aggregateStagesWindow(stages, 2, logger)
	})
}

func getTestLogger() logr.Logger {
	return zap.New(zap.UseDevMode(true))
}

func getApplicationResponse() *sparkapiclient.Application {
	return &sparkapiclient.Application{
		ID:   "spark-123",
		Name: "my-test-application",
		Attempts: []sparkapiclient.Attempt{
			{
				StartTimeEpoch:   100,
				EndTimeEpoch:     200,
				LastUpdatedEpoch: 300,
				Duration:         400,
				SparkUser:        "spark-user",
				Completed:        false,
				AppSparkVersion:  "3.0.0",
			},
			{
				StartTimeEpoch:   500,
				EndTimeEpoch:     600,
				LastUpdatedEpoch: 700,
				Duration:         800,
				SparkUser:        "spark-user-2",
				Completed:        true,
				AppSparkVersion:  "9.0.0",
			},
		},
	}
}

func getStagesResponse() []sparkapiclient.Stage {
	return []sparkapiclient.Stage{
		{
			Status:          "COMPLETE",
			StageID:         1,
			AttemptID:       2,
			InputBytes:      100,
			OutputBytes:     200,
			ExecutorCpuTime: 300,
		},
		{
			Status:          "COMPLETE",
			StageID:         2,
			AttemptID:       3,
			InputBytes:      400,
			OutputBytes:     500,
			ExecutorCpuTime: 600,
		},
	}
}

func getEnvironmentResponse() *sparkapiclient.Environment {
	return &sparkapiclient.Environment{
		SparkProperties: [][]string{
			{
				"prop1", "val1",
			},
			{
				"prop2", "val2",
			},
			{
				"prop3", "val3", "thisShouldBeIgnored",
			},
		},
	}
}

func getExecutorsResponse() []sparkapiclient.Executor {
	return []sparkapiclient.Executor{
		{
			ID:      "driver",
			AddTime: "2020-12-14T14:07:27.142GMT",
		},
		{
			ID:      "1",
			AddTime: "2020-12-14T15:17:37.142GMT",
		},
		{
			ID:      "2",
			AddTime: "2020-12-14T16:27:47.142GMT",
		},
	}
}

func getStreamingStatisticsResponse() *sparkapiclient.StreamingStatistics {
	return &sparkapiclient.StreamingStatistics{
		StartTime:         "2020-12-14T16:27:47.142GMT",
		BatchDuration:     9999,
		AvgProcessingTime: 3333,
	}
}

func newHistoryServerService() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: catalog.SystemNamespace,
			Labels: map[string]string{
				appNameLabel: historyServerAppNameLabelValue,
			},
		},
	}
}

func newRunningDriverPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			DeletionTimestamp: nil,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:  SparkDriverContainerName,
					Ready: true,
				},
			},
		},
	}
}
