package spot

import (
	"fmt"
	"net/http"

	"github.com/spotinst/spotinst-sdk-go/spotinst"
)

const (
	queryAccountId         = "accountId"
	queryClusterIdentifier = "clusterIdentifier"
)

type apiTransport struct {
	base       http.RoundTripper
	config     *spotinst.Config
	identifier string
	cluster    string
}

func (a *apiTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	creds, err := a.config.Credentials.Get()
	if err != nil {
		return nil, err
	}

	query := req.URL.Query()
	query.Set(queryAccountId, creds.Account)
	query.Set(queryClusterIdentifier, a.cluster,
	)
	req.URL.RawQuery = query.Encode()

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", creds.Token))

	// Set request base URL.
	req.URL.Host = a.config.BaseURL.Host
	req.URL.Scheme = a.config.BaseURL.Scheme

	// Set request headers.
	req.Host = a.config.BaseURL.Host
	req.Header.Add("User-Agent", a.config.UserAgent)

	// Set the unique identifier for this kubernetes cluster
	req.Header.Set("kubernetesUniqueIdentifier", a.identifier)
	return a.base.RoundTrip(req)
}

func ApiTransport(base http.RoundTripper, config *spotinst.Config, identifier string, cluster string) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}

	return &apiTransport{
		base:       base,
		config:     config,
		identifier: identifier,
		cluster:    cluster,
	}
}
