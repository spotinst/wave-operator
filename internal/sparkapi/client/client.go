package client

import (
	"encoding/json"
	"fmt"
	"github.com/spotinst/wave-operator/internal/sparkapi/client/transport"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
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
}

type client struct {
	transportClient transport.TransportClient
}

func NewHistoryServerClient(host string) Client {
	tc := transport.NewHTTPClient(host, historyServerPort)
	c := &client{
		transportClient: tc,
	}
	return c
}

func NewDriverPodClient(pod *corev1.Pod, restConfig *rest.Config) (Client, error) {
	tc, err := transport.NewPodProxyClient(pod, restConfig, driverPort)
	if err != nil {
		return nil, fmt.Errorf("could not create pod proxy client, %w", err)
	}

	c := &client{
		transportClient: tc,
	}

	return c, nil
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

func (c client) getEnvironmentURLPath(applicationId string) string {
	return fmt.Sprintf("%s/applications/%s/environment", apiVersionUrl, applicationId)
}

func (c client) getApplicationURLPath(applicationId string) string {
	return fmt.Sprintf("%s/applications/%s", apiVersionUrl, applicationId)
}

func (c client) getStagesURLPath(applicationId string) string {
	return fmt.Sprintf("%s/applications/%s/stages", apiVersionUrl, applicationId)
}
