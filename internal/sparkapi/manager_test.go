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
	"github.com/spotinst/wave-operator/internal/config"
	sparkapiclient "github.com/spotinst/wave-operator/internal/sparkapi/client"
	"github.com/spotinst/wave-operator/internal/sparkapi/client/mock_client"
)

func TestGetSparkApiClient(t *testing.T) {

	logger := getTestLogger()

	t.Run("whenDriverAvailable", func(tt *testing.T) {

		svc := newHistoryServerService()
		pod := newRunningDriverPod(true)

		clientSet := k8sfake.NewSimpleClientset(svc, pod)

		c, err := getSparkApiClient(clientSet, pod, logger)
		assert.NoError(tt, err)
		assert.NotNil(tt, c)
		assert.Equal(tt, sparkapiclient.DriverClient, c.GetClientType())

	})

	t.Run("whenDriverAvailable_eventLogSyncOff", func(tt *testing.T) {

		svc := newHistoryServerService()
		pod := newRunningDriverPod(false)

		clientSet := k8sfake.NewSimpleClientset(svc, pod)

		c, err := getSparkApiClient(clientSet, pod, logger)
		assert.NoError(tt, err)
		assert.NotNil(tt, c)
		assert.Equal(tt, sparkapiclient.DriverClient, c.GetClientType())

	})

	t.Run("whenDriverNotAvailableHistoryServerAvailable", func(tt *testing.T) {

		svc := newHistoryServerService()
		pod := newRunningDriverPod(true)
		pod.Status.Phase = corev1.PodSucceeded // Driver not running

		clientSet := k8sfake.NewSimpleClientset(svc, pod)

		c, err := getSparkApiClient(clientSet, pod, logger)
		assert.NoError(tt, err)
		assert.NotNil(tt, c)
		assert.Equal(tt, sparkapiclient.HistoryServerClient, c.GetClientType())

	})

	t.Run("whenDriverNotAvailable_eventLogSyncOff", func(tt *testing.T) {

		svc := newHistoryServerService()
		pod := newRunningDriverPod(false)
		pod.Status.Phase = corev1.PodSucceeded // Driver not running

		clientSet := k8sfake.NewSimpleClientset(svc, pod)

		c, err := getSparkApiClient(clientSet, pod, logger)
		assert.Error(tt, err)
		assert.Nil(tt, c)
		assert.True(tt, IsApiNotAvailableError(err))

	})

	t.Run("whenHistoryServerNotAvailableDriverNotAvailable", func(tt *testing.T) {

		pod := newRunningDriverPod(true)
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

		_, err := manager.GetApplicationInfo(applicationID, NewStageMetricsAggregatorState(), logger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "test error")

	})

	t.Run("whenSuccessful", func(tt *testing.T) {

		m := mock_client.NewMockClient(ctrl)
		m.EXPECT().GetApplication(applicationID).Return(getApplicationResponse(), nil).Times(1)
		m.EXPECT().GetEnvironment(applicationID).Return(getEnvironmentResponse(), nil).Times(1)
		m.EXPECT().GetStages(applicationID).Return(getStagesResponse(), nil).Times(1)
		m.EXPECT().GetAllExecutors(applicationID).Return(getExecutorsResponse(), nil).Times(1)
		m.EXPECT().GetClientType().Times(1)
		m.EXPECT().GetStreamingStatistics(applicationID).Times(0)

		manager := &manager{
			client: m,
			logger: getTestLogger(),
		}

		res, err := manager.GetApplicationInfo(applicationID, NewStageMetricsAggregatorState(), logger)
		assert.NoError(tt, err)

		assert.Equal(tt, "my-test-application", res.ApplicationName)

		assert.Equal(tt, int64(900), res.TotalNewExecutorCpuTime)
		assert.Equal(tt, int64(500), res.TotalNewInputBytes)
		assert.Equal(tt, int64(700), res.TotalNewOutputBytes)
		assert.Equal(tt, 2, res.MetricsAggregatorState.MaxProcessedFinalizedStageID)

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
		m.EXPECT().GetClientType().Return(sparkapiclient.DriverClient).Times(1)
		m.EXPECT().GetStreamingStatistics(applicationID).Return(nil, fmt.Errorf("404 not found")).Times(1)

		manager := &manager{
			client: m,
			logger: getTestLogger(),
		}

		res, err := manager.GetApplicationInfo(applicationID, NewStageMetricsAggregatorState(), logger)
		assert.NoError(tt, err)

		assert.Equal(tt, WorkloadType(""), res.WorkloadType)
	})

	t.Run("whenDriverClient_sparkStreaming", func(tt *testing.T) {

		m := mock_client.NewMockClient(ctrl)
		m.EXPECT().GetApplication(applicationID).Return(getApplicationResponse(), nil).Times(1)
		m.EXPECT().GetEnvironment(applicationID).Return(getEnvironmentResponse(), nil).Times(1)
		m.EXPECT().GetStages(applicationID).Return(getStagesResponse(), nil).Times(1)
		m.EXPECT().GetAllExecutors(applicationID).Return(getExecutorsResponse(), nil).Times(1)
		m.EXPECT().GetClientType().Return(sparkapiclient.DriverClient).Times(1)
		m.EXPECT().GetStreamingStatistics(applicationID).Return(getStreamingStatisticsResponse(), nil).Times(1)

		manager := &manager{
			client: m,
			logger: getTestLogger(),
		}

		res, err := manager.GetApplicationInfo(applicationID, NewStageMetricsAggregatorState(), logger)
		assert.NoError(tt, err)

		assert.Equal(tt, SparkStreaming, res.WorkloadType)
	})

	t.Run("whenHistoryServerClient_dontCheckSparkStreaming", func(tt *testing.T) {

		m := mock_client.NewMockClient(ctrl)
		m.EXPECT().GetApplication(applicationID).Return(getApplicationResponse(), nil).Times(1)
		m.EXPECT().GetEnvironment(applicationID).Return(getEnvironmentResponse(), nil).Times(1)
		m.EXPECT().GetStages(applicationID).Return(getStagesResponse(), nil).Times(1)
		m.EXPECT().GetAllExecutors(applicationID).Return(getExecutorsResponse(), nil).Times(1)
		m.EXPECT().GetClientType().Return(sparkapiclient.HistoryServerClient).Times(1)
		m.EXPECT().GetStreamingStatistics(applicationID).Times(0)

		manager := &manager{
			client: m,
			logger: getTestLogger(),
		}

		res, err := manager.GetApplicationInfo(applicationID, NewStageMetricsAggregatorState(), logger)
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
		statuses       []string
		oldState       StageMetricsAggregatorState
		expectedResult stageWindowAggregationResult
		message        string
	}

	testCases := []testCase{
		{
			statuses: []string{},
			oldState: NewStageMetricsAggregatorState(),
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     0,
				totalNewInputBytes:      0,
				totalNewExecutorCpuTime: 0,
				newState:                NewStageMetricsAggregatorState(),
			},
			message: "whenNoStagesReceived",
		},
		{
			statuses: []string{"COMPLETE", "COMPLETE", "COMPLETE", "ACTIVE"},
			oldState: NewStageMetricsAggregatorState(),
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     4 * outputBytesPerStage,
				totalNewInputBytes:      4 * inputBytesPerStage,
				totalNewExecutorCpuTime: 4 * cpuTimePerStage,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 2,
					ActiveStageMetrics: map[int]StageMetrics{
						3: {
							OutputBytes: outputBytesPerStage,
							InputBytes:  inputBytesPerStage,
							CPUTime:     cpuTimePerStage,
						},
					},
					PendingStages: []int{},
				},
			},
			message: "whenStagesCompleteInOrder_1",
		},
		{
			statuses: []string{"COMPLETE", "COMPLETE", "COMPLETE", "ACTIVE"},
			oldState: StageMetricsAggregatorState{
				MaxProcessedFinalizedStageID: 0,
				ActiveStageMetrics:           make(map[int]StageMetrics),
			},
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     3 * outputBytesPerStage,
				totalNewInputBytes:      3 * inputBytesPerStage,
				totalNewExecutorCpuTime: 3 * cpuTimePerStage,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 2,
					ActiveStageMetrics: map[int]StageMetrics{
						3: {
							OutputBytes: outputBytesPerStage,
							InputBytes:  inputBytesPerStage,
							CPUTime:     cpuTimePerStage,
						},
					},
					PendingStages: []int{},
				},
			},
			message: "whenStagesCompleteInOrder_2",
		},
		{
			statuses: []string{"COMPLETE", "COMPLETE", "COMPLETE", "ACTIVE"},
			oldState: StageMetricsAggregatorState{
				MaxProcessedFinalizedStageID: 1,
				ActiveStageMetrics:           make(map[int]StageMetrics),
			},
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     2 * outputBytesPerStage,
				totalNewInputBytes:      2 * inputBytesPerStage,
				totalNewExecutorCpuTime: 2 * cpuTimePerStage,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 2,
					ActiveStageMetrics: map[int]StageMetrics{
						3: {
							OutputBytes: outputBytesPerStage,
							InputBytes:  inputBytesPerStage,
							CPUTime:     cpuTimePerStage,
						},
					},
					PendingStages: []int{},
				},
			},
			message: "whenStagesCompleteInOrder_3",
		},
		{
			statuses: []string{"COMPLETE", "COMPLETE", "COMPLETE", "ACTIVE"},
			oldState: StageMetricsAggregatorState{
				MaxProcessedFinalizedStageID: 2,
				ActiveStageMetrics:           make(map[int]StageMetrics),
			},
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     1 * outputBytesPerStage,
				totalNewInputBytes:      1 * inputBytesPerStage,
				totalNewExecutorCpuTime: 1 * cpuTimePerStage,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 2,
					ActiveStageMetrics: map[int]StageMetrics{
						3: {
							OutputBytes: outputBytesPerStage,
							InputBytes:  inputBytesPerStage,
							CPUTime:     cpuTimePerStage,
						},
					},
					PendingStages: []int{},
				},
			},
			message: "whenStagesCompleteInOrder_4",
		},
		{
			statuses: []string{"COMPLETE", "COMPLETE", "COMPLETE", "COMPLETE"},
			oldState: StageMetricsAggregatorState{
				MaxProcessedFinalizedStageID: 2,
				ActiveStageMetrics:           make(map[int]StageMetrics),
			},
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     1 * outputBytesPerStage,
				totalNewInputBytes:      1 * inputBytesPerStage,
				totalNewExecutorCpuTime: 1 * cpuTimePerStage,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 3,
					ActiveStageMetrics:           map[int]StageMetrics{},
					PendingStages:                []int{},
				},
			},
			message: "whenStagesCompleteInOrder_5",
		},
		{
			statuses: []string{"COMPLETE", "COMPLETE", "COMPLETE", "COMPLETE"},
			oldState: StageMetricsAggregatorState{
				MaxProcessedFinalizedStageID: 3,
				ActiveStageMetrics:           make(map[int]StageMetrics),
			},
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     0,
				totalNewInputBytes:      0,
				totalNewExecutorCpuTime: 0,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 3,
					ActiveStageMetrics:           map[int]StageMetrics{},
					PendingStages:                []int{},
				},
			},
			message: "whenStagesCompleteInOrder_6",
		},
		{
			statuses: []string{"COMPLETE", "COMPLETE", "COMPLETE", "COMPLETE"},
			oldState: NewStageMetricsAggregatorState(),
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     4 * outputBytesPerStage,
				totalNewInputBytes:      4 * inputBytesPerStage,
				totalNewExecutorCpuTime: 4 * cpuTimePerStage,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 3,
					ActiveStageMetrics:           map[int]StageMetrics{},
					PendingStages:                []int{},
				},
			},
			message: "whenStagesCompleteInOrder_7",
		},
		{
			statuses: []string{"COMPLETE", "COMPLETE", "ACTIVE", "SKIPPED", "ACTIVE", "COMPLETE"},
			oldState: NewStageMetricsAggregatorState(),
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     6 * outputBytesPerStage,
				totalNewInputBytes:      6 * inputBytesPerStage,
				totalNewExecutorCpuTime: 6 * cpuTimePerStage,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 5,
					ActiveStageMetrics: map[int]StageMetrics{
						2: {
							OutputBytes: outputBytesPerStage,
							InputBytes:  inputBytesPerStage,
							CPUTime:     cpuTimePerStage,
						},
						4: {
							OutputBytes: outputBytesPerStage,
							InputBytes:  inputBytesPerStage,
							CPUTime:     cpuTimePerStage,
						},
					},
					PendingStages: []int{},
				},
			},
			message: "whenStagesCompleteOutOfOrder_1",
		},
		{
			statuses: []string{"COMPLETE", "COMPLETE", "ACTIVE", "SKIPPED", "ACTIVE", "COMPLETE"},
			oldState: StageMetricsAggregatorState{
				MaxProcessedFinalizedStageID: 0,
				ActiveStageMetrics:           map[int]StageMetrics{},
			},
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     5 * outputBytesPerStage,
				totalNewInputBytes:      5 * inputBytesPerStage,
				totalNewExecutorCpuTime: 5 * cpuTimePerStage,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 5,
					ActiveStageMetrics: map[int]StageMetrics{
						2: {
							OutputBytes: outputBytesPerStage,
							InputBytes:  inputBytesPerStage,
							CPUTime:     cpuTimePerStage,
						},
						4: {
							OutputBytes: outputBytesPerStage,
							InputBytes:  inputBytesPerStage,
							CPUTime:     cpuTimePerStage,
						},
					},
					PendingStages: []int{},
				},
			},
			message: "whenStagesCompleteOutOfOrder_2",
		},
		{
			statuses: []string{"COMPLETE", "COMPLETE", "ACTIVE", "SKIPPED", "ACTIVE", "COMPLETE"},
			oldState: StageMetricsAggregatorState{
				MaxProcessedFinalizedStageID: 1,
				ActiveStageMetrics:           map[int]StageMetrics{},
			},
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     4 * outputBytesPerStage,
				totalNewInputBytes:      4 * inputBytesPerStage,
				totalNewExecutorCpuTime: 4 * cpuTimePerStage,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 5,
					ActiveStageMetrics: map[int]StageMetrics{
						2: {
							OutputBytes: outputBytesPerStage,
							InputBytes:  inputBytesPerStage,
							CPUTime:     cpuTimePerStage,
						},
						4: {
							OutputBytes: outputBytesPerStage,
							InputBytes:  inputBytesPerStage,
							CPUTime:     cpuTimePerStage,
						},
					},
					PendingStages: []int{},
				},
			},
			message: "whenStagesCompleteOutOfOrder_3",
		},
		{
			statuses: []string{"COMPLETE", "COMPLETE", "COMPLETE", "SKIPPED", "ACTIVE", "COMPLETE"},
			oldState: StageMetricsAggregatorState{
				MaxProcessedFinalizedStageID: 1,
				ActiveStageMetrics:           map[int]StageMetrics{},
			},
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     4 * outputBytesPerStage,
				totalNewInputBytes:      4 * inputBytesPerStage,
				totalNewExecutorCpuTime: 4 * cpuTimePerStage,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 5,
					ActiveStageMetrics: map[int]StageMetrics{
						4: {
							OutputBytes: outputBytesPerStage,
							InputBytes:  inputBytesPerStage,
							CPUTime:     cpuTimePerStage,
						},
					},
					PendingStages: []int{},
				},
			},
			message: "whenStagesCompleteOutOfOrder_4",
		},
		{
			statuses: []string{"COMPLETE", "COMPLETE", "COMPLETE", "SKIPPED", "COMPLETE", "COMPLETE"},
			oldState: StageMetricsAggregatorState{
				MaxProcessedFinalizedStageID: 1,
				ActiveStageMetrics:           map[int]StageMetrics{},
			},
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     4 * outputBytesPerStage,
				totalNewInputBytes:      4 * inputBytesPerStage,
				totalNewExecutorCpuTime: 4 * cpuTimePerStage,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 5,
					ActiveStageMetrics:           map[int]StageMetrics{},
					PendingStages:                []int{},
				},
			},
			message: "whenStagesCompleteOutOfOrder_5",
		},
		{
			statuses: []string{"COMPLETE", "FAILED", "SKIPPED", "PENDING", "COMPLETE", "ACTIVE"},
			oldState: NewStageMetricsAggregatorState(),
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     5 * outputBytesPerStage,
				totalNewInputBytes:      5 * inputBytesPerStage,
				totalNewExecutorCpuTime: 5 * cpuTimePerStage,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 4,
					ActiveStageMetrics: map[int]StageMetrics{
						5: {
							OutputBytes: outputBytesPerStage,
							InputBytes:  inputBytesPerStage,
							CPUTime:     cpuTimePerStage,
						},
					},
					PendingStages: []int{3},
				},
			},
			message: "whenStagesCompleteOutOfOrder_6",
		},
		{
			statuses: []string{"COMPLETE", "FAILED", "SKIPPED", "COMPLETE", "COMPLETE", "COMPLETE"},
			oldState: StageMetricsAggregatorState{
				MaxProcessedFinalizedStageID: 100,
				ActiveStageMetrics:           map[int]StageMetrics{},
			},
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     0,
				totalNewInputBytes:      0,
				totalNewExecutorCpuTime: 0,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 100,
					ActiveStageMetrics:           map[int]StageMetrics{},
					PendingStages:                []int{},
				},
			},
			message: "whenOnlyOldStagesReceived",
		},
		{
			statuses: []string{"COMPLETE", "FAILED", "SKIPPED", "PENDING", "ACTIVE", "COMPLETE"},
			oldState: StageMetricsAggregatorState{
				MaxProcessedFinalizedStageID: 100,
				ActiveStageMetrics: map[int]StageMetrics{
					4: {
						OutputBytes: outputBytesPerStage,
						InputBytes:  inputBytesPerStage,
						CPUTime:     cpuTimePerStage,
					},
				},
			},
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     0,
				totalNewInputBytes:      0,
				totalNewExecutorCpuTime: 0,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 100,
					ActiveStageMetrics: map[int]StageMetrics{
						4: {
							OutputBytes: outputBytesPerStage,
							InputBytes:  inputBytesPerStage,
							CPUTime:     cpuTimePerStage,
						},
					},
					PendingStages: []int{3},
				},
			},
			message: "shouldNotAddActiveStagesTwice",
		},
		{
			statuses: []string{"COMPLETE", "FAILED", "SKIPPED", "PENDING", "ACTIVE", "COMPLETE", "ACTIVE"},
			oldState: StageMetricsAggregatorState{
				MaxProcessedFinalizedStageID: 2,
				ActiveStageMetrics:           map[int]StageMetrics{},
			},
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     3 * outputBytesPerStage,
				totalNewInputBytes:      3 * inputBytesPerStage,
				totalNewExecutorCpuTime: 3 * cpuTimePerStage,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 5,
					ActiveStageMetrics: map[int]StageMetrics{
						4: {
							OutputBytes: outputBytesPerStage,
							InputBytes:  inputBytesPerStage,
							CPUTime:     cpuTimePerStage,
						},
						6: {
							OutputBytes: outputBytesPerStage,
							InputBytes:  inputBytesPerStage,
							CPUTime:     cpuTimePerStage,
						},
					},
					PendingStages: []int{3},
				},
			},
			message: "whenStagesCompleteOutOfOrder_oldStagesFinalize_1",
		},
		{
			statuses: []string{"COMPLETE", "FAILED", "SKIPPED", "ACTIVE", "COMPLETE", "COMPLETE", "COMPLETE"},
			oldState: StageMetricsAggregatorState{
				MaxProcessedFinalizedStageID: 5,
				ActiveStageMetrics: map[int]StageMetrics{
					4: {
						OutputBytes: 10,
						InputBytes:  20,
						CPUTime:     30,
					},
					6: {
						OutputBytes: outputBytesPerStage,
						InputBytes:  inputBytesPerStage,
						CPUTime:     cpuTimePerStage,
					},
				},
			},
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     (outputBytesPerStage - 10) + outputBytesPerStage,
				totalNewInputBytes:      (inputBytesPerStage - 20) + inputBytesPerStage,
				totalNewExecutorCpuTime: (cpuTimePerStage - 30) + cpuTimePerStage,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 6,
					ActiveStageMetrics: map[int]StageMetrics{
						3: {
							OutputBytes: outputBytesPerStage,
							InputBytes:  inputBytesPerStage,
							CPUTime:     cpuTimePerStage,
						},
					},
					PendingStages: []int{},
				},
			},
			message: "whenStagesCompleteOutOfOrder_oldStagesFinalize_2",
		},
		{
			statuses: []string{"COMPLETE", "FAILED", "PENDING", "COMPLETE"},
			oldState: NewStageMetricsAggregatorState(),
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     3 * outputBytesPerStage,
				totalNewInputBytes:      3 * inputBytesPerStage,
				totalNewExecutorCpuTime: 3 * cpuTimePerStage,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 3,
					ActiveStageMetrics:           map[int]StageMetrics{},
					PendingStages:                []int{2},
				},
			},
			message: "whenPendingStageOutOfOrder_1",
		},
		{
			statuses: []string{"COMPLETE", "FAILED", "COMPLETE", "COMPLETE"},
			oldState: StageMetricsAggregatorState{
				MaxProcessedFinalizedStageID: 3,
				ActiveStageMetrics:           map[int]StageMetrics{},
				PendingStages:                []int{2},
			},
			expectedResult: stageWindowAggregationResult{
				totalNewOutputBytes:     1 * outputBytesPerStage,
				totalNewInputBytes:      1 * inputBytesPerStage,
				totalNewExecutorCpuTime: 1 * cpuTimePerStage,
				newState: StageMetricsAggregatorState{
					MaxProcessedFinalizedStageID: 3,
					ActiveStageMetrics:           map[int]StageMetrics{},
					PendingStages:                []int{},
				},
			},
			message: "whenPendingStageOutOfOrder_2",
		},
	}

	for _, tc := range testCases {

		stages := getStages(tc.statuses)
		aggregator := newStageMetricsAggregator(logger, tc.oldState)
		res := aggregator.processWindow(stages)

		assert.Equal(t, tc.expectedResult.newState, res.newState, tc.message)
		assert.Equal(t, tc.expectedResult.totalNewExecutorCpuTime, res.totalNewExecutorCpuTime, tc.message)
		assert.Equal(t, tc.expectedResult.totalNewInputBytes, res.totalNewInputBytes, tc.message)
		assert.Equal(t, tc.expectedResult.totalNewOutputBytes, res.totalNewOutputBytes, tc.message)
	}

	t.Run("whenStagesReceivedOutOfOrder", func(tt *testing.T) {

		stages := getStages([]string{"COMPLETE", "COMPLETE", "COMPLETE", "ACTIVE", "COMPLETE", "ACTIVE"})
		stages[0], stages[3] = stages[3], stages[0]
		stages[2], stages[5] = stages[5], stages[2]

		aggregator := newStageMetricsAggregator(logger, NewStageMetricsAggregatorState())
		res := aggregator.processWindow(stages)
		assert.Equal(tt, 4, res.newState.MaxProcessedFinalizedStageID)
		assert.Equal(tt, map[int]StageMetrics{
			3: {
				OutputBytes: outputBytesPerStage,
				InputBytes:  inputBytesPerStage,
				CPUTime:     cpuTimePerStage,
			},
			5: {
				OutputBytes: outputBytesPerStage,
				InputBytes:  inputBytesPerStage,
				CPUTime:     cpuTimePerStage,
			},
		}, res.newState.ActiveStageMetrics)
		assert.Equal(tt, int64(6*cpuTimePerStage), res.totalNewExecutorCpuTime)
		assert.Equal(tt, int64(6*inputBytesPerStage), res.totalNewInputBytes)
		assert.Equal(tt, int64(6*outputBytesPerStage), res.totalNewOutputBytes)
	})

	t.Run("whenStagesMissed", func(tt *testing.T) {

		// Manual verification of error logging

		// Should not log error, no stages received
		newStageMetricsAggregator(logger, NewStageMetricsAggregatorState()).processWindow([]sparkapiclient.Stage{})

		stages := getStages([]string{"COMPLETE", "COMPLETE"})

		// Should not log error, no stages seen before
		newStageMetricsAggregator(logger, NewStageMetricsAggregatorState()).processWindow(stages)

		stages[0].StageID = 4
		stages[1].StageID = 5

		// Should not log error
		newStageMetricsAggregator(logger, StageMetricsAggregatorState{MaxProcessedFinalizedStageID: 3}).processWindow(stages)
		// Should not log error
		newStageMetricsAggregator(logger, StageMetricsAggregatorState{MaxProcessedFinalizedStageID: 4}).processWindow(stages)
		// Should not log error, no new stages received
		newStageMetricsAggregator(logger, StageMetricsAggregatorState{MaxProcessedFinalizedStageID: 5}).processWindow(stages)
		// Should not log error, no new stages received
		newStageMetricsAggregator(logger, StageMetricsAggregatorState{MaxProcessedFinalizedStageID: 6}).processWindow(stages)
		// Should log error, we missed stage 0
		newStageMetricsAggregator(logger, StageMetricsAggregatorState{MaxProcessedFinalizedStageID: -1}).processWindow(stages)
		// Should log error, we missed stage 1
		newStageMetricsAggregator(logger, StageMetricsAggregatorState{MaxProcessedFinalizedStageID: 0}).processWindow(stages)
		// Should log error, we missed stage 2
		newStageMetricsAggregator(logger, StageMetricsAggregatorState{MaxProcessedFinalizedStageID: 1}).processWindow(stages)
		// Should log error, we missed stage 3
		newStageMetricsAggregator(logger, StageMetricsAggregatorState{MaxProcessedFinalizedStageID: 2}).processWindow(stages)
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

func newRunningDriverPod(eventLogSyncEnabled bool) *corev1.Pod {
	pod := &corev1.Pod{
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
	if eventLogSyncEnabled {
		pod.Annotations = make(map[string]string)
		pod.Annotations[config.WaveConfigAnnotationSyncEventLogs] = "true"
	}
	return pod
}
