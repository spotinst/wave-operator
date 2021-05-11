//go:generate mockgen -destination=mock_client/driver_mock.go . DriverClient

package client

import (
	"encoding/json"
	"fmt"

	"github.com/spotinst/wave-operator/internal/sparkapi/client/transport"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

const driverPort = "4040"

type DriverClient interface {
	Client
	GetStreamingStatistics(applicationID string) (*StreamingStatistics, error)
	GetMetrics() (*Metrics, error)
}

type driver struct {
	*client
}

func NewDriverPodClient(pod *corev1.Pod, clientSet kubernetes.Interface) DriverClient {
	tc := transport.NewProxyClient(transport.Pod, pod.Name, pod.Namespace, driverPort, clientSet)
	c := &driver{
		client: &client{
			transportClient: tc,
		},
	}
	return c
}

func (dc *driver) GetStreamingStatistics(applicationID string) (*StreamingStatistics, error) {

	path := dc.getStreamingStatisticsURLPath(applicationID)
	resp, err := dc.transportClient.Get(path)
	if err != nil {
		return nil, err
	}

	streamingStatistics := &StreamingStatistics{}
	err = json.Unmarshal(resp, streamingStatistics)
	if err != nil {
		return nil, err
	}

	return streamingStatistics, nil
}

func (dc *driver) GetMetrics() (*Metrics, error) {
	resp, err := dc.transportClient.Get("metrics/json")
	if err != nil {
		return nil, err
	}

	metrics := new(Metrics)
	err = json.Unmarshal(resp, metrics)
	if err != nil {
		return nil, err
	}

	return metrics, nil
}

func (dc *driver) getStreamingStatisticsURLPath(applicationID string) string {
	return fmt.Sprintf("%s/applications/%s/streaming/statistics", apiVersionUrl, applicationID)
}
