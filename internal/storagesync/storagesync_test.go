package storagesync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/spotinst/wave-operator/internal/sparkapi"
)

func TestShouldStopSync(t *testing.T) {

	t.Run("whenDriverRunning_storageSyncRunning_shouldNotStopSync", func(tt *testing.T) {

		containerStatuses := []corev1.ContainerStatus{
			getRunningContainerStatus(SyncContainerName),
			getRunningContainerStatus(sparkapi.SparkDriverContainerName),
		}

		pod := getTestPod(containerStatuses)
		res := ShouldStopSync(pod)
		assert.False(tt, res)

	})

	t.Run("whenDriverRunning_storageSyncTerminated_shouldNotStopSync", func(tt *testing.T) {

		containerStatuses := []corev1.ContainerStatus{
			getTerminatedContainerStatus(SyncContainerName, time.Now()),
			getRunningContainerStatus(sparkapi.SparkDriverContainerName),
		}

		pod := getTestPod(containerStatuses)
		res := ShouldStopSync(pod)
		assert.False(tt, res)

	})

	t.Run("whenDriverTerminated_storageSyncRunning_timeoutNotPassed_shouldNotStopSync", func(tt *testing.T) {

		containerStatuses := []corev1.ContainerStatus{
			getRunningContainerStatus(SyncContainerName),
			getTerminatedContainerStatus(sparkapi.SparkDriverContainerName, time.Now()),
		}

		pod := getTestPod(containerStatuses)
		res := ShouldStopSync(pod)
		assert.False(tt, res)

	})

	t.Run("whenDriverTerminated_storageSyncRunning_timeoutPassed_shouldStopSync", func(tt *testing.T) {

		containerStatuses := []corev1.ContainerStatus{
			getRunningContainerStatus(SyncContainerName),
			getTerminatedContainerStatus(sparkapi.SparkDriverContainerName, time.Now().Add(-syncTimeout)),
		}

		pod := getTestPod(containerStatuses)
		res := ShouldStopSync(pod)
		assert.True(tt, res)

	})

	t.Run("whenDriverWaiting_storageSyncRunning_shouldNotStopSync", func(tt *testing.T) {

		containerStatuses := []corev1.ContainerStatus{
			getRunningContainerStatus(SyncContainerName),
			getWaitingContainerStatus(sparkapi.SparkDriverContainerName),
		}

		pod := getTestPod(containerStatuses)
		res := ShouldStopSync(pod)
		assert.False(tt, res)

	})

	t.Run("whenDriverTerminated_storageSyncTerminated_timeoutPassed_shouldNotStopSync", func(tt *testing.T) {

		containerStatuses := []corev1.ContainerStatus{
			getTerminatedContainerStatus(SyncContainerName, time.Now()),
			getTerminatedContainerStatus(sparkapi.SparkDriverContainerName, time.Now().Add(-syncTimeout)),
		}

		pod := getTestPod(containerStatuses)
		res := ShouldStopSync(pod)
		assert.False(tt, res)

	})

}

func getRunningContainerStatus(containerName string) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name: containerName,
		State: corev1.ContainerState{
			Running: &corev1.ContainerStateRunning{},
		},
	}
}

func getWaitingContainerStatus(containerName string) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name: containerName,
		State: corev1.ContainerState{
			Waiting: &corev1.ContainerStateWaiting{},
		},
	}
}

func getTerminatedContainerStatus(containerName string, terminationTime time.Time) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name: containerName,
		State: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{
				ExitCode: int32(0),
				FinishedAt: metav1.Time{
					Time: terminationTime,
				},
			},
		},
	}
}

func getTestPod(containerStatuses []corev1.ContainerStatus) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "testPod",
		},
		Status: corev1.PodStatus{
			ContainerStatuses: containerStatuses,
		},
	}
}
