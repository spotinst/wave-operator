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

	sparkapiclient "github.com/spotinst/wave-operator/internal/sparkapi/client"
	"github.com/spotinst/wave-operator/internal/sparkapi/client/mock_client"
)

func TestNewManager(t *testing.T) {

	t.Run("whenHistoryServerClient", func(tt *testing.T) {

		svc := newHistoryServerService()
		pod := newRunningDriverPod()

		clientSet := k8sfake.NewSimpleClientset(svc)

		m, err := NewManager(clientSet, pod, getTestLogger())
		assert.NoError(tt, err)
		assert.NotNil(tt, m)

	})

	t.Run("whenDriverPodClient", func(tt *testing.T) {

		pod := newRunningDriverPod()

		clientSet := k8sfake.NewSimpleClientset(pod)

		m, err := NewManager(clientSet, pod, getTestLogger())
		assert.NoError(tt, err)
		assert.NotNil(tt, m)

	})

	t.Run("whenError", func(tt *testing.T) {

		pod := newRunningDriverPod()
		pod.Status.Phase = corev1.PodSucceeded // Driver not running

		clientSet := k8sfake.NewSimpleClientset(pod)

		manager, err := NewManager(clientSet, pod, getTestLogger())
		assert.Nil(tt, manager)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "could not get spark api client")

	})
}

func TestGetApplicationInfo(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	applicationId := "spark-123"

	t.Run("whenError", func(tt *testing.T) {

		m := mock_client.NewMockClient(ctrl)
		m.EXPECT().GetApplication(applicationId).Return(getApplicationResponse(), nil).Times(1)
		m.EXPECT().GetEnvironment(applicationId).Return(nil, fmt.Errorf("test error")).Times(1)
		m.EXPECT().GetStages(applicationId).Return(getStagesResponse(), nil).Times(0)

		manager := &manager{
			client: m,
			logger: getTestLogger(),
		}

		_, err := manager.GetApplicationInfo(applicationId)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "test error")

	})

	t.Run("whenSuccessful", func(tt *testing.T) {

		m := mock_client.NewMockClient(ctrl)
		m.EXPECT().GetApplication(applicationId).Return(getApplicationResponse(), nil).Times(1)
		m.EXPECT().GetEnvironment(applicationId).Return(getEnvironmentResponse(), nil).Times(1)
		m.EXPECT().GetStages(applicationId).Return(getStagesResponse(), nil).Times(1)

		manager := &manager{
			client: m,
			logger: getTestLogger(),
		}

		res, err := manager.GetApplicationInfo(applicationId)
		assert.NoError(tt, err)

		assert.Equal(tt, "my-test-application", res.ApplicationName)

		assert.Equal(tt, int64(900), res.TotalExecutorCpuTime)
		assert.Equal(tt, int64(500), res.TotalInputBytes)
		assert.Equal(tt, int64(700), res.TotalOutputBytes)

		assert.Equal(tt, 2, len(res.SparkProperties))
		assert.Equal(tt, "val1", res.SparkProperties["prop1"])
		assert.Equal(tt, "val2", res.SparkProperties["prop2"])

		assert.Equal(tt, 2, len(res.Attempts))
		assert.Equal(tt, getApplicationResponse().Attempts[0], res.Attempts[0])
		assert.Equal(tt, getApplicationResponse().Attempts[1], res.Attempts[1])
	})
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

func newHistoryServerService() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      historyServerServiceName,
			Namespace: systemNamespace,
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
					Name:  sparkDriverContainerName,
					Ready: true,
				},
			},
		},
	}
}
