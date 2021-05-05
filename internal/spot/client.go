package spot

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"

	"github.com/go-logr/logr"
	"github.com/spotinst/spotinst-sdk-go/spotinst"
	"github.com/spotinst/spotinst-sdk-go/spotinst/client"
	"github.com/spotinst/wave-operator/api/v1alpha1"
)

const (
	queryAccountId         = "accountId"
	queryClusterIdentifier = "clusterIdentifier"
)

type ApplicationGetter interface {
	GetSparkApplication(ctx context.Context, ID string) (string, error)
}

type ApplicationSaver interface {
	SaveApplication(app *v1alpha1.SparkApplication) error
}

type Client struct {
	logger  logr.Logger
	cluster string
	spot    *client.Client
}

func (c *Client) GetSparkApplication(ctx context.Context, ID string) (string, error) {
	req := client.NewRequest(http.MethodGet, fmt.Sprintf("/wave/spark/application/%s", ID))
	resp, err := c.spot.Do(ctx, req)
	if err != nil {
		return "", err
	}

	body, _ := httputil.DumpResponse(resp, true)
	return string(body), nil
}

func (c *Client) SaveApplication(app *v1alpha1.SparkApplication) error {
	c.logger.Info("Persisting spark spark application", "id", app.Spec.ApplicationID, "name", app.Spec.ApplicationName, "heritage", app.Spec.Heritage)
	return nil
}

func NewClient(config *spotinst.Config, cluster string, logger logr.Logger) *Client {
	return &Client{
		logger:  logger,
		cluster: cluster,
		spot:    client.New(config),
	}
}
