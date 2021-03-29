package client

import (
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/spotinst/wave-operator/internal/sparkapi/client/transport"
)

const (
	apiVersionUrl     = "api/v1"
	driverPort        = "4040"
	historyServerPort = "18080"

	DriverClient        ClientType = "driver"
	HistoryServerClient ClientType = "history-server"
)

type ClientType string

type Client interface {
	GetClientType() ClientType
	GetApplication(applicationID string) (*Application, error)
	GetEnvironment(applicationID string) (*Environment, error)
	GetStages(applicationID string) ([]Stage, error)
	GetAllExecutors(applicationID string) ([]Executor, error)
	GetStreamingStatistics(applicationID string) (*StreamingStatistics, error)
}

type client struct {
	clientType      ClientType
	transportClient transport.Client
}

func NewDriverPodClient(pod *corev1.Pod, clientSet kubernetes.Interface) Client {
	tc := transport.NewProxyClient(transport.Pod, pod.Name, pod.Namespace, driverPort, clientSet)
	c := &client{
		clientType:      DriverClient,
		transportClient: tc,
	}
	return c
}

func NewHistoryServerClient(service *corev1.Service, clientSet kubernetes.Interface) Client {
	tc := transport.NewProxyClient(transport.Service, service.Name, service.Namespace, historyServerPort, clientSet)
	c := &client{
		clientType:      HistoryServerClient,
		transportClient: tc,
	}
	return c
}

func (c client) GetClientType() ClientType {
	return c.clientType
}

func (c client) GetApplication(applicationID string) (*Application, error) {

	path := c.getApplicationURLPath(applicationID)
	resp, err := c.transportClient.Get(path)
	if err != nil {
		return nil, err
	}

	application := &Application{}
	err = json.Unmarshal(resp, application)
	if err != nil {
		return nil, err
	}

	return application, nil
}

func (c client) GetEnvironment(applicationID string) (*Environment, error) {

	path := c.getEnvironmentURLPath(applicationID)
	resp, err := c.transportClient.Get(path)
	if err != nil {
		return nil, err
	}

	environment := &Environment{}
	err = json.Unmarshal(resp, environment)
	if err != nil {
		return nil, err
	}

	return environment, nil
}

func (c client) GetStages(applicationID string) ([]Stage, error) {

	path := c.getStagesURLPath(applicationID)
	resp, err := c.transportClient.Get(path)
	if err != nil {
		return nil, err
	}

	stages := make([]Stage, 0)
	err = json.Unmarshal(resp, &stages)
	if err != nil {
		return nil, err
	}

	return stages, nil
}

func (c client) GetAllExecutors(applicationID string) ([]Executor, error) {

	path := c.getAllExecutorsURLPath(applicationID)
	resp, err := c.transportClient.Get(path)
	if err != nil {
		return nil, err
	}

	executors := make([]Executor, 0)
	err = json.Unmarshal(resp, &executors)
	if err != nil {
		return nil, err
	}

	return executors, nil
}

func (c client) GetStreamingStatistics(applicationID string) (*StreamingStatistics, error) {

	path := c.getStreamingStatisticsURLPath(applicationID)
	resp, err := c.transportClient.Get(path)
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

func (c client) getEnvironmentURLPath(applicationID string) string {
	return fmt.Sprintf("%s/applications/%s/environment", apiVersionUrl, applicationID)
}

func (c client) getApplicationURLPath(applicationID string) string {
	return fmt.Sprintf("%s/applications/%s", apiVersionUrl, applicationID)
}

func (c client) getStagesURLPath(applicationID string) string {
	return fmt.Sprintf("%s/applications/%s/stages", apiVersionUrl, applicationID)
}

func (c client) getAllExecutorsURLPath(applicationID string) string {
	return fmt.Sprintf("%s/applications/%s/allexecutors", apiVersionUrl, applicationID)
}

func (c client) getStreamingStatisticsURLPath(applicationID string) string {
	return fmt.Sprintf("%s/applications/%s/streaming/statistics", apiVersionUrl, applicationID)
}
