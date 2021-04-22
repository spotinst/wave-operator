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

	t.Run("whenDriverRunning_storageSyncNotRunning_shouldNotStopSync", func(tt *testing.T) {

		containerStatuses := []corev1.ContainerStatus{
			getTerminatedContainerStatus(SyncContainerName, 0, time.Now()),
			getRunningContainerStatus(sparkapi.SparkDriverContainerName),
		}

		pod := getTestPod(containerStatuses)
		res := ShouldStopSync(pod)
		assert.False(tt, res)

	})

	t.Run("whenDriverNotRunning_storageSyncRunning_driverFailed_timeoutNotPassed_shouldNotStopSync", func(tt *testing.T) {

		containerStatuses := []corev1.ContainerStatus{
			getRunningContainerStatus(SyncContainerName),
			getTerminatedContainerStatus(sparkapi.SparkDriverContainerName, 1, time.Now()),
		}

		pod := getTestPod(containerStatuses)
		res := ShouldStopSync(pod)
		assert.False(tt, res)

	})

	t.Run("whenDriverNotRunning_storageSyncRunning_driverFailed_timeoutPassed_shouldStopSync", func(tt *testing.T) {

		containerStatuses := []corev1.ContainerStatus{
			getRunningContainerStatus(SyncContainerName),
			getTerminatedContainerStatus(sparkapi.SparkDriverContainerName, 1, time.Now().Add(-syncTimeoutError)),
		}

		pod := getTestPod(containerStatuses)
		res := ShouldStopSync(pod)
		assert.True(tt, res)

	})

	t.Run("whenDriverNotRunning_storageSyncRunning_driverSucceeded_timeoutNotPassed_shouldNotStopSync", func(tt *testing.T) {

		containerStatuses := []corev1.ContainerStatus{
			getRunningContainerStatus(SyncContainerName),
			getTerminatedContainerStatus(sparkapi.SparkDriverContainerName, 0, time.Now()),
		}

		pod := getTestPod(containerStatuses)
		res := ShouldStopSync(pod)
		assert.False(tt, res)

	})

	t.Run("whenDriverNotRunning_storageSyncRunning_driverSucceeded_timeoutPassed_shouldStopSync", func(tt *testing.T) {

		containerStatuses := []corev1.ContainerStatus{
			getRunningContainerStatus(SyncContainerName),
			getTerminatedContainerStatus(sparkapi.SparkDriverContainerName, 0, time.Now().Add(-syncTimeoutSuccess)),
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

	t.Run("whenDriverNotRunning_storageSyncNotRunning_driverFailed_timeoutPassed_shouldNotStopSync", func(tt *testing.T) {

		containerStatuses := []corev1.ContainerStatus{
			getTerminatedContainerStatus(SyncContainerName, 0, time.Now()),
			getTerminatedContainerStatus(sparkapi.SparkDriverContainerName, 1, time.Now().Add(-syncTimeoutError)),
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

func getTerminatedContainerStatus(containerName string, exitCode int, terminationTime time.Time) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name: containerName,
		State: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{
				ExitCode: int32(exitCode),
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
