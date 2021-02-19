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

		c, err := getSparkApiClient(clientSet, pod, logger)
		assert.NoError(tt, err)
		assert.NotNil(tt, c)

	})

	t.Run("whenDriverNotAvailableHistoryServerAvailable", func(tt *testing.T) {

		svc := newHistoryServerService()
		pod := newRunningDriverPod()
		pod.Status.Phase = corev1.PodSucceeded // Driver not running

		clientSet := k8sfake.NewSimpleClientset(svc, pod)

		c, err := getSparkApiClient(clientSet, pod, logger)
		assert.NoError(tt, err)
		assert.NotNil(tt, c)

	})

	t.Run("whenHistoryServerNotAvailableDriverNotAvailable", func(tt *testing.T) {

		pod := newRunningDriverPod()
		pod.Status.Phase = corev1.PodSucceeded // Driver not running

		clientSet := k8sfake.NewSimpleClientset(pod)

		c, err := getSparkApiClient(clientSet, pod, logger)
		assert.Error(tt, err)
		assert.Nil(tt, c)

	})

}

func TestGetApplicationInfo(t *testing.T) {

	logger := getTestLogger()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	applicationId := "spark-123"

	t.Run("whenError", func(tt *testing.T) {

		m := mock_client.NewMockClient(ctrl)
		m.EXPECT().GetApplication(applicationId).Return(getApplicationResponse(), nil).Times(1)
		m.EXPECT().GetEnvironment(applicationId).Return(nil, fmt.Errorf("test error")).Times(1)
		m.EXPECT().GetStages(applicationId).Return(getStagesResponse(), nil).Times(0)
		m.EXPECT().GetAllExecutors(applicationId).Return(getExecutorsResponse(), nil).Times(0)

		manager := &manager{
			client: m,
			logger: getTestLogger(),
		}

		_, err := manager.GetApplicationInfo(applicationId, -1, logger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "test error")

	})

	t.Run("whenSuccessful", func(tt *testing.T) {

		m := mock_client.NewMockClient(ctrl)
		m.EXPECT().GetApplication(applicationId).Return(getApplicationResponse(), nil).Times(1)
		m.EXPECT().GetEnvironment(applicationId).Return(getEnvironmentResponse(), nil).Times(1)
		m.EXPECT().GetStages(applicationId).Return(getStagesResponse(), nil).Times(1)
		m.EXPECT().GetAllExecutors(applicationId).Return(getExecutorsResponse(), nil).Times(1)

		manager := &manager{
			client: m,
			logger: getTestLogger(),
		}

		res, err := manager.GetApplicationInfo(applicationId, -1, logger)
		assert.NoError(tt, err)

		assert.Equal(tt, "my-test-application", res.ApplicationName)

		assert.Equal(tt, int64(900), res.TotalNewExecutorCpuTime)
		assert.Equal(tt, int64(500), res.TotalNewInputBytes)
		assert.Equal(tt, int64(700), res.TotalNewOutputBytes)

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
	})

}

func TestStageAggregation(t *testing.T) {

	logger := getTestLogger()

	getStagesResponse := func(statuses []string) []sparkapiclient.Stage {
		stages := make([]sparkapiclient.Stage, len(statuses))
		for i := 0; i < len(statuses); i++ {
			stage := sparkapiclient.Stage{
				Status:          statuses[i],
				StageId:         i,
				AttemptId:       1,
				InputBytes:      100,
				OutputBytes:     100,
				ExecutorCpuTime: 100,
			}
			stages[i] = stage
		}
		return stages
	}

	type testCase struct {
		statuses               []string
		oldMaxProcessedStageId int
		expectedResult         stageWindowAggregationResult
		message                string
	}

	testCases := []testCase{
		{
			statuses:               []string{},
			oldMaxProcessedStageId: -1,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     0,
				totalNewInputBytes:      0,
				totalNewExecutorCpuTime: 0,
				newMaxProcessedStageId:  -1,
			},
			message: "whenNoStagesReceived",
		},
		{
			statuses:               []string{"COMPLETED", "COMPLETED", "COMPLETED", "ACTIVE"},
			oldMaxProcessedStageId: -1,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     300,
				totalNewInputBytes:      300,
				totalNewExecutorCpuTime: 300,
				newMaxProcessedStageId:  2,
			},
			message: "whenStagesCompleteInOrder_1",
		},
		{
			statuses:               []string{"COMPLETED", "COMPLETED", "COMPLETED", "ACTIVE"},
			oldMaxProcessedStageId: 0,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     200,
				totalNewInputBytes:      200,
				totalNewExecutorCpuTime: 200,
				newMaxProcessedStageId:  2,
			},
			message: "whenStagesCompleteInOrder_2",
		},
		{
			statuses:               []string{"COMPLETED", "COMPLETED", "COMPLETED", "ACTIVE"},
			oldMaxProcessedStageId: 1,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     100,
				totalNewInputBytes:      100,
				totalNewExecutorCpuTime: 100,
				newMaxProcessedStageId:  2,
			},
			message: "whenStagesCompleteInOrder_3",
		},
		{
			statuses:               []string{"COMPLETED", "COMPLETED", "COMPLETED", "ACTIVE"},
			oldMaxProcessedStageId: 2,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     0,
				totalNewInputBytes:      0,
				totalNewExecutorCpuTime: 0,
				newMaxProcessedStageId:  2,
			},
			message: "whenStagesCompleteInOrder_4",
		},
		{
			statuses:               []string{"COMPLETED", "COMPLETED", "COMPLETED", "COMPLETED"},
			oldMaxProcessedStageId: 2,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     100,
				totalNewInputBytes:      100,
				totalNewExecutorCpuTime: 100,
				newMaxProcessedStageId:  3,
			},
			message: "whenStagesCompleteInOrder_5",
		},
		{
			statuses:               []string{"COMPLETED", "COMPLETED", "COMPLETED", "COMPLETED"},
			oldMaxProcessedStageId: -1,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     400,
				totalNewInputBytes:      400,
				totalNewExecutorCpuTime: 400,
				newMaxProcessedStageId:  3,
			},
			message: "whenStagesCompleteInOrder_6",
		},
		{
			statuses:               []string{"COMPLETED", "COMPLETED", "ACTIVE", "SKIPPED", "ACTIVE", "COMPLETED"},
			oldMaxProcessedStageId: -1,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     200,
				totalNewInputBytes:      200,
				totalNewExecutorCpuTime: 200,
				newMaxProcessedStageId:  1,
			},
			message: "whenStagesCompleteOutOfOrder_1",
		},
		{
			statuses:               []string{"COMPLETED", "COMPLETED", "ACTIVE", "SKIPPED", "ACTIVE", "COMPLETED"},
			oldMaxProcessedStageId: 0,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     100,
				totalNewInputBytes:      100,
				totalNewExecutorCpuTime: 100,
				newMaxProcessedStageId:  1,
			},
			message: "whenStagesCompleteOutOfOrder_2",
		},
		{
			statuses:               []string{"COMPLETED", "COMPLETED", "ACTIVE", "SKIPPED", "ACTIVE", "COMPLETED"},
			oldMaxProcessedStageId: 1,
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     0,
				totalNewInputBytes:      0,
				totalNewExecutorCpuTime: 0,
				newMaxProcessedStageId:  1,
			},
			message: "whenStagesCompleteOutOfOrder_3",
		},
	}

	for _, tc := range testCases {

		stages := getStagesResponse(tc.statuses)
		res := aggregateStagesWindow(stages, tc.oldMaxProcessedStageId, logger)

		assert.Equal(t, tc.expectedResult.newMaxProcessedStageId, res.newMaxProcessedStageId, tc.message)
		assert.Equal(t, tc.expectedResult.totalNewExecutorCpuTime, res.totalNewExecutorCpuTime, tc.message)
		assert.Equal(t, tc.expectedResult.totalNewInputBytes, res.totalNewInputBytes, tc.message)
		assert.Equal(t, tc.expectedResult.totalNewOutputBytes, res.totalNewOutputBytes, tc.message)
	}
}

func getTestLogger() logr.Logger {
	return zap.New(zap.UseDevMode(true))
}

func getApplicationResponse() *sparkapiclient.Application {
	return &sparkapiclient.Application{
		Id:   "spark-123",
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
			StageId:         1,
			AttemptId:       2,
			InputBytes:      100,
			OutputBytes:     200,
			ExecutorCpuTime: 300,
		},
		{
			StageId:         2,
			AttemptId:       3,
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
			Id:      "driver",
			AddTime: "2020-12-14T14:07:27.142GMT",
		},
		{
			Id:      "1",
			AddTime: "2020-12-14T15:17:37.142GMT",
		},
		{
			Id:      "2",
			AddTime: "2020-12-14T16:27:47.142GMT",
		},
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
