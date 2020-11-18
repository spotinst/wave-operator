package sparkapi

import (
	"encoding/json"
	"fmt"
	"github.com/go-logr/logr"
	"io/ioutil"
	"net/http"
)

const (
	apiVersionUrl = "/api/v1"
)

type Client interface {
	GetApplication(applicationId string) (*Application, error)
	GetEnvironment(applicationId string) (*Environment, error)
	GetStages(applicationId string) ([]Stage, error)
}

type client struct {
	host string
	log  logr.Logger
}

func NewClient(host string, log logr.Logger) Client {
	c := client{
		host: host,
		log:  log,
	}
	return c
}

func (c client) doGet(url string) ([]byte, error) {
	client := &http.Client{}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("got http status %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func (c client) GetApplication(applicationId string) (*Application, error) {

	resp, err := c.doGet(c.getApplicationURL(applicationId))
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

	resp, err := c.doGet(c.getEnvironmentURL(applicationId))
	if err != nil {
		return nil, err
	}

	apiEnvironment := &apiEnvironment{}
	err = json.Unmarshal(resp, &apiEnvironment)
	if err != nil {
		return nil, err
	}

	environment, err := c.translateEnvironment(apiEnvironment)
	if err != nil {
		return nil, fmt.Errorf("could not translate api environment, %w", err)
	}

	return environment, nil
}

func (c client) GetStages(applicationId string) ([]Stage, error) {

	resp, err := c.doGet(c.getStagesURL(applicationId))
	if err != nil {
		return nil, err
	}

	var stages []Stage
	err = json.Unmarshal(resp, &stages)
	if err != nil {
		return nil, err
	}

	return stages, nil
}

func (c client) getEnvironmentURL(applicationId string) string {
	return fmt.Sprintf("%s%s/applications/%s/environment", c.host, apiVersionUrl, applicationId)
}

func (c client) getApplicationURL(applicationId string) string {
	return fmt.Sprintf("%s%s/applications/%s", c.host, apiVersionUrl, applicationId)
}

func (c client) getStagesURL(applicationId string) string {
	return fmt.Sprintf("%s%s/applications/%s/stages", c.host, apiVersionUrl, applicationId)
}

func (c client) translateEnvironment(apiEnvironment *apiEnvironment) (*Environment, error) {

	if apiEnvironment == nil {
		return nil, fmt.Errorf("api environment is nil")
	}

	if apiEnvironment.SparkProperties == nil {
		return nil, fmt.Errorf("spark properties is nil")
	}

	environment := &Environment{}
	environment.SparkProperties = make(map[string]string, len(apiEnvironment.SparkProperties))

	for _, apiProperty := range apiEnvironment.SparkProperties {
		if len(apiProperty) == 2 {
			environment.SparkProperties[apiProperty[0]] = apiProperty[1]
		} else {
			// Ignore, just log error
			err := fmt.Errorf("got api property of length %d, wanted 2: %s", len(apiProperty), apiProperty)
			c.log.Error(err, "spark properties parse error")
		}
	}

	return environment, nil
}
