package client

import (
	"fmt"
	"net/http"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/client-go/kubernetes"

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

func NewClient(kc kubernetes.Interface, logger logr.Logger) (*Client, error) {

	cfg, err := config.GetConfig(kc, logger)
	if err != nil {
		return nil, fmt.Errorf("could not get config, %w", err)
	}

	return &Client{
		logger:                  logger,
		clusterIdentifier:       cfg.ClusterIdentifier,
		clusterUniqueIdentifier: cfg.ClusterUniqueIdentifier,
		httpClient: &http.Client{
			// TODO(thorsteinn) Proxies
			Timeout:   requestTimeout,
			Transport: ApiTransport(nil, cfg.BaseURL, cfg.Creds),
		},
	}, nil
}
