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
)

type Client interface {
	GetApplication(applicationId string) (*Application, error)
	GetEnvironment(applicationId string) (*Environment, error)
	GetStages(applicationId string) ([]Stage, error)
	GetAllExecutors(applicationId string) ([]Executor, error)
}

type client struct {
	transportClient transport.Client
}

func NewDriverPodClient(pod *corev1.Pod, clientSet kubernetes.Interface) Client {
	tc := transport.NewProxyClient(transport.Pod, pod.Name, pod.Namespace, driverPort, clientSet)
	c := &client{
		transportClient: tc,
	}
	return c
}

func NewHistoryServerClient(service *corev1.Service, clientSet kubernetes.Interface) Client {
	tc := transport.NewProxyClient(transport.Service, service.Name, service.Namespace, historyServerPort, clientSet)
	c := &client{
		transportClient: tc,
	}
	return c
}

func (c client) GetApplication(applicationId string) (*Application, error) {

	path := c.getApplicationURLPath(applicationId)
	resp, err := c.transportClient.Get(path)
	if err != nil {
		return nil, err
	}

	application := &Application{}
	err = json.Unmarshal(resp, &application)
	if err != nil {
		return nil, err
	}

	return application, nil
}

func (c client) GetEnvironment(applicationId string) (*Environment, error) {

	path := c.getEnvironmentURLPath(applicationId)
	resp, err := c.transportClient.Get(path)
	if err != nil {
		return nil, err
	}

	environment := &Environment{}
	err = json.Unmarshal(resp, &environment)
	if err != nil {
		return nil, err
	}

	return environment, nil
}

func (c client) GetStages(applicationId string) ([]Stage, error) {

	path := c.getStagesURLPath(applicationId)
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

func (c client) GetAllExecutors(applicationId string) ([]Executor, error) {

	path := c.getAllExecutorsURLPath(applicationId)
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

func (c client) getEnvironmentURLPath(applicationId string) string {
	return fmt.Sprintf("%s/applications/%s/environment", apiVersionUrl, applicationId)
}

func (c client) getApplicationURLPath(applicationId string) string {
	return fmt.Sprintf("%s/applications/%s", apiVersionUrl, applicationId)
}

func (c client) getStagesURLPath(applicationId string) string {
	return fmt.Sprintf("%s/applications/%s/stages", apiVersionUrl, applicationId)
}

func (c client) getAllExecutorsURLPath(applicationId string) string {
	return fmt.Sprintf("%s/applications/%s/allexecutors", apiVersionUrl, applicationId)
}
