package spot

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"

	"github.com/go-logr/logr"
	"github.com/spotinst/spotinst-sdk-go/spotinst"
	"github.com/spotinst/wave-operator/api/v1alpha1"
)

const (
	queryAccountId         = "accountId"
	queryClusterIdentifier = "clusterIdentifier"

	contentTypeProtobuf = "application/x-protobuf"
)

type ApplicationGetter interface {
	GetSparkApplication(ctx context.Context, ID string) (string, error)
}

type ApplicationSaver interface {
	SaveApplication(app *v1alpha1.SparkApplication) error
}

type Client struct {
	logger     logr.Logger
	cluster    string
	httpClient *http.Client
}

func (c *Client) GetSparkApplication(ctx context.Context, ID string) (string, error) {
	resp, err := c.httpClient.Get(fmt.Sprintf("/wave/spark/application/%s", ID))
	if err != nil {
		return "", err
	}

	body, _ := httputil.DumpResponse(resp, true)
	return string(body), nil
}

func (c *Client) SaveApplication(app *v1alpha1.SparkApplication) error {
	c.logger.Info("Persisting spark spark application", "id", app.Spec.ApplicationID, "name", app.Spec.ApplicationName, "heritage", app.Spec.Heritage)

	//req, := c.httpClient.Post("mcs/kubernetes/topology/bigdata/spark/application", contentTypeProtobuf)
	return nil
}

func NewClient(config *spotinst.Config, cluster string, logger logr.Logger) *Client {
	return &Client{
		logger:     logger,
		cluster:    cluster,
		httpClient: config.HTTPClient,
	}
}
