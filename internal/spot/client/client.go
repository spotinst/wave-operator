package client

import (
	"fmt"
	"net/http"
	"time"

	"github.com/go-logr/logr"

	"github.com/spotinst/wave-operator/internal/ocean"
	"github.com/spotinst/wave-operator/internal/spot/client/config"
)

const (
	queryAccountId                  = "accountId"
	queryClusterIdentifier          = "clusterIdentifier"
	queryKubernetesUniqueIdentifier = "kubernetesUniqueIdentifier"

	requestTimeout = 15 * time.Second
)

type Client struct {
	logger                  logr.Logger
	httpClient              *http.Client
	clusterIdentifier       string
	clusterUniqueIdentifier string
}

func NewClient(logger logr.Logger) (*Client, error) {

	creds, err := config.GetCredentials()
	if err != nil {
		return nil, fmt.Errorf("could not get credentials, %w", err)
	}

	baseURL, err := config.GetBaseURL()
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
		logger:                  logger,
		clusterIdentifier:       clusterIdentifier,
		clusterUniqueIdentifier: clusterUniqueIdentifier,
		httpClient: &http.Client{
			Timeout:   requestTimeout,
			Transport: ApiTransport(nil, baseURL, creds),
		},
	}, nil
}
