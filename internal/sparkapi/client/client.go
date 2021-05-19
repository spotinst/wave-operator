//go:generate mockgen -destination=mock_client/client_mock.go . Client

package client

import (
	"encoding/json"
	"fmt"

	"github.com/spotinst/wave-operator/internal/sparkapi/client/transport"
)

const (
	apiVersionUrl = "api/v1"
)

type Client interface {
	GetApplication(applicationID string) (*Application, error)
	GetEnvironment(applicationID string) (*Environment, error)
	GetStages(applicationID string) ([]Stage, error)
	GetAllExecutors(applicationID string) ([]Executor, error)
}

type client struct {
	transportClient transport.Client
}

func (c *client) GetApplication(applicationID string) (*Application, error) {

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

func (c *client) GetEnvironment(applicationID string) (*Environment, error) {

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

func (c *client) GetStages(applicationID string) ([]Stage, error) {

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

func (c *client) GetAllExecutors(applicationID string) ([]Executor, error) {

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

func (c *client) getEnvironmentURLPath(applicationID string) string {
	return fmt.Sprintf("%s/applications/%s/environment", apiVersionUrl, applicationID)
}

func (c *client) getApplicationURLPath(applicationID string) string {
	return fmt.Sprintf("%s/applications/%s", apiVersionUrl, applicationID)
}

func (c *client) getStagesURLPath(applicationID string) string {
	return fmt.Sprintf("%s/applications/%s/stages", apiVersionUrl, applicationID)
}

func (c *client) getAllExecutorsURLPath(applicationID string) string {
	return fmt.Sprintf("%s/applications/%s/allexecutors", apiVersionUrl, applicationID)
}
