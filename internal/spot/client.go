package spot

import (
	"fmt"
	"net/http"

	"github.com/go-logr/logr"

	"github.com/spotinst/wave-operator/internal/ocean"
)

type Client struct {
	logger     logr.Logger
	httpClient *http.Client
}

func NewClient(logger logr.Logger) (*Client, error) {

	creds, err := getCredentials()
	if err != nil {
		return nil, fmt.Errorf("could not get credentials, %w", err)
	}

	baseURL, err := getBaseURL()
	if err != nil {
		return nil, fmt.Errorf("could not get base url, %w", err)
	}

	clusterIdentifier, err := ocean.GetClusterIdentifier()
	if err != nil {
		return nil, fmt.Errorf("could not get cluster identifier, %w", err)
	}

	clusterUniqueIdentifier, err := ocean.GetClusterUniqueIdentifier()
	if err != nil {
		return nil, fmt.Errorf("could not get cluster unique identifier, %w", err)
	}

	return &Client{
		logger: logger,
		httpClient: &http.Client{
			Transport: ApiTransport(nil, baseURL, creds, clusterIdentifier, clusterUniqueIdentifier),
		},
	}, nil
}
