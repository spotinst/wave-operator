package spot

import (
	"context"
	"fmt"
	"net/http"

	"github.com/spotinst/spotinst-sdk-go/spotinst"
	"github.com/spotinst/spotinst-sdk-go/spotinst/client"
	"github.com/spotinst/wave-operator/api/v1alpha1"
)

const (
	queryAccountId         = "accountId"
	queryClusterIdentifier = "clusterIdentifier"
)

type ApplicationGetter interface {
	GetSparkApplication(ID string) (string, error)
}

type ApplicationSaver interface {
	SaveApplication(app v1alpha1.SparkApplication)
}

type Client struct {
	spot   *client.Client
}

func (c *Client) GetSparkApplication(ID string) (string, error) {
	req := client.NewRequest(http.MethodGet, fmt.Sprintf("/wave/spark/application/%s", ID))
	_, err := c.spot.Do(context.TODO(), req)
	if err != nil {
		return "", err
	}

	return "", nil
}

func (c *Client) SaveApplication(app v1alpha1.SparkApplication) {
}

func NewClient(config *spotinst.Config, cluster string) *Client {
	return &Client{
		spot:   client.New(config),
	}
}

type spotTransport struct {
	base    http.RoundTripper
	config  *spotinst.Config
	cluster string
}

func (s *spotTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	creds, err := s.config.Credentials.Get()
	if err != nil {
		return nil, err
	}

	query := req.URL.Query()
	query.Set(queryAccountId, creds.Account)
	query.Set(queryClusterIdentifier, s.cluster)
	req.URL.RawQuery = query.Encode()

	// Set request base URL.
	req.URL.Host = s.config.BaseURL.Host
	req.URL.Scheme = s.config.BaseURL.Scheme

	// Set request headers.
	req.Host = s.config.BaseURL.Host
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", creds.Token))
	req.Header.Add("User-Agent", s.config.UserAgent)

	return s.base.RoundTrip(req)
}

func AuthTransport(base http.RoundTripper, config *spotinst.Config, cluster string) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}

	return &spotTransport{
		base:    base,
		config:  config,
		cluster: cluster,
	}
}
