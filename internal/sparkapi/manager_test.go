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
		assert.Implements(tt, (*sparkapiclient.DriverClient)(nil), c)
	})

	t.Run("whenDriverAvailable_eventLogSyncOff", func(tt *testing.T) {

		svc := newHistoryServerService()
		pod := newRunningDriverPod(false)

		clientSet := k8sfake.NewSimpleClientset(svc, pod)

		c, err := getSparkApiClient(clientSet, pod, logger)
		assert.NoError(tt, err)
		assert.NotNil(tt, c)
		assert.Implements(tt, (*sparkapiclient.DriverClient)(nil), c)
	})

	t.Run("whenDriverNotAvailableHistoryServerAvailable", func(tt *testing.T) {

		svc := newHistoryServerService()
		pod := newRunningDriverPod(true)
		pod.Status.Phase = corev1.PodSucceeded // Driver not running

		clientSet := k8sfake.NewSimpleClientset(svc, pod)

		c, err := getSparkApiClient(clientSet, pod, logger)
		assert.NoError(tt, err)
		assert.NotNil(tt, c)
		assert.Implements(tt, (*sparkapiclient.Client)(nil), c)

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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	applicationID := "spark-123"

	t.Run("whenError", func(tt *testing.T) {

		m := mock_client.NewMockClient(ctrl)
		m.EXPECT().GetApplication(applicationID).Return(getApplicationResponse(), nil).Times(1)
		m.EXPECT().GetEnvironment(applicationID).Return(nil, fmt.Errorf("test error")).Times(1)
		m.EXPECT().GetAllExecutors(applicationID).Return(getExecutorsResponse(), nil).Times(0)

		manager := &manager{
			client: m,
			logger: getTestLogger(),
		}

		_, err := manager.GetApplicationInfo(applicationID)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "test error")

	})

	t.Run("whenSuccessful", func(tt *testing.T) {

		m := mock_client.NewMockClient(ctrl)
		m.EXPECT().GetApplication(applicationID).Return(getApplicationResponse(), nil).Times(1)
		m.EXPECT().GetEnvironment(applicationID).Return(getEnvironmentResponse(), nil).Times(1)
		m.EXPECT().GetAllExecutors(applicationID).Return(getExecutorsResponse(), nil).Times(1)

		manager := &manager{
			client: m,
			logger: getTestLogger(),
		}

		res, err := manager.GetApplicationInfo(applicationID)
		assert.NoError(tt, err)

		assert.Equal(tt, "my-test-application", res.ApplicationName)

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

		m := mock_client.NewMockDriverClient(ctrl)
		m.EXPECT().GetApplication(applicationID).Return(getApplicationResponse(), nil).Times(1)
		m.EXPECT().GetEnvironment(applicationID).Return(getEnvironmentResponse(), nil).Times(1)
		m.EXPECT().GetAllExecutors(applicationID).Return(getExecutorsResponse(), nil).Times(1)
		m.EXPECT().GetMetrics().Return(getMetricsResponse(), nil).Times(1)
		m.EXPECT().GetStreamingStatistics(applicationID).Return(nil, fmt.Errorf("404 not found")).Times(1)

		manager := &manager{
			client: m,
			logger: getTestLogger(),
		}

		res, err := manager.GetApplicationInfo(applicationID)
		assert.NoError(tt, err)

		assert.Equal(tt, WorkloadType(""), res.WorkloadType)
	})

	t.Run("whenDriverClient_sparkStreaming", func(tt *testing.T) {

		m := mock_client.NewMockDriverClient(ctrl)
		m.EXPECT().GetApplication(applicationID).Return(getApplicationResponse(), nil).Times(1)
		m.EXPECT().GetEnvironment(applicationID).Return(getEnvironmentResponse(), nil).Times(1)
		m.EXPECT().GetAllExecutors(applicationID).Return(getExecutorsResponse(), nil).Times(1)
		m.EXPECT().GetMetrics().Return(getMetricsResponse(), nil).Times(1)
		m.EXPECT().GetStreamingStatistics(applicationID).Return(getStreamingStatisticsResponse(), nil).Times(1)

		manager := &manager{
			client: m,
			logger: getTestLogger(),
		}

		res, err := manager.GetApplicationInfo(applicationID)
		assert.NoError(tt, err)

		assert.Equal(tt, SparkStreaming, res.WorkloadType)
	})

	t.Run("whenHistoryServerClient_dontCheckSparkStreaming", func(tt *testing.T) {

		m := mock_client.NewMockClient(ctrl)
		m.EXPECT().GetApplication(applicationID).Return(getApplicationResponse(), nil).Times(1)
		m.EXPECT().GetEnvironment(applicationID).Return(getEnvironmentResponse(), nil).Times(1)
		m.EXPECT().GetAllExecutors(applicationID).Return(getExecutorsResponse(), nil).Times(1)

		manager := &manager{
			client: m,
			logger: getTestLogger(),
		}

		res, err := manager.GetApplicationInfo(applicationID)
		assert.NoError(tt, err)

		assert.Equal(tt, WorkloadType(""), res.WorkloadType)
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

func getMetricsResponse() sparkapiclient.Metrics {
	return sparkapiclient.Metrics{
		Gauges: map[string]sparkapiclient.GaugeValue{
			"test-gauge.driver.BlockManager.maxMem": {
				Value: int64(200),
			},
		},
		Counters: map[string]sparkapiclient.CounterValue{
			"test-counter.driver.LiveListener.MaxSurge": {
				Count: int64(2000),
			},
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
