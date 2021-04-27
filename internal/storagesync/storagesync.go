package storagesync

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/spotinst/wave-operator/internal/sparkapi"
)

const (
	Port              int32  = 23174
	SyncContainerName string = "storage-sync"

	// The syncTimeouts specify how long we wait after driver termination
	// before we tell the storage sync container to stop.
	// Note that an in-progress sync operation will not be terminated
	// even if we tell the storage sync container to stop, so this value
	// specifies how long we should wait for event logs to appear and the sync to start,
	// and in the case of repeated sync failures, how long we should wait before
	// giving up.
	syncTimeoutSuccess = 5 * time.Minute
	// Let's have a shorter timeout for failed applications that may not write any logs
	syncTimeoutError = 1 * time.Minute
)

func ShouldStopSync(pod *corev1.Pod) bool {

	var storageSyncRunning bool
	var driverTerminated bool
	var driverTerminationTime metav1.Time
	var driverFailed bool

	for _, containerStatus := range pod.Status.ContainerStatuses {
		switch containerStatus.Name {
		case SyncContainerName:
			if containerStatus.State.Running != nil {
				storageSyncRunning = true
			}
		case sparkapi.SparkDriverContainerName:
			if containerStatus.State.Terminated != nil {
				driverTerminated = true
				driverTerminationTime = containerStatus.State.Terminated.FinishedAt
				if containerStatus.State.Terminated.ExitCode != 0 {
					driverFailed = true
				}
			}
		}
	}

	if storageSyncRunning && driverTerminated {
		// Let's allow the storage sync container a bit of time
		// before we tell it to stop, in case it is able to finish on its own
		currentTime := time.Now().Unix()
		var timeoutTime int64
		if driverFailed {
			timeoutTime = driverTerminationTime.Add(syncTimeoutError).Unix()
		} else {
			timeoutTime = driverTerminationTime.Add(syncTimeoutSuccess).Unix()
		}
		if currentTime >= timeoutTime {
			return true
		}
	}

	return false
}

func StopSync(pod *corev1.Pod) error {

	var client = &http.Client{
		Timeout: time.Second * 5,
	}

	ip := pod.Status.PodIP
	if ip == "" {
		return fmt.Errorf("could not get pod IP")
	}

	url := fmt.Sprintf("http://%s:%s/stop", ip, strconv.Itoa(int(Port)))

	resp, err := client.Post(url, "", nil)
	if err != nil {
		return fmt.Errorf("request failed, %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("got status code: %d", resp.StatusCode)
	}

	return nil
}
