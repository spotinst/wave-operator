package spot

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"

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
	SaveApplication(app v1alpha1.SparkApplication)
}

type Client struct {
	spot   *client.Client
}

func (c *Client) GetSparkApplication(ID string) (string, error) {
	req := client.NewRequest(http.MethodGet, fmt.Sprintf("/wave/spark/application/%s", ID))
	resp, err := c.spot.Do(context.TODO(), req)
	if err != nil {
		return "", err
	}

	body, _ := httputil.DumpResponse(resp, true)
	return string(body), nil
}

func (c *Client) SaveApplication(app v1alpha1.SparkApplication) {
}

func NewClient(config *spotinst.Config, cluster string) *Client {
	return &Client{
		spot:   client.New(config),
	}
}
