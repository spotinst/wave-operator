package client

import (
	"net/http"

	"github.com/spotinst/wave-operator/internal/logger"
)

type transportTestFunc func(req *http.Request) (*http.Response, error)

func (f transportTestFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

var clientWithTestTransport = func(testTransport transportTestFunc) *Client {
	return &Client{
		logger:                  logger.New(),
		clusterIdentifier:       "my-cluster",
		clusterUniqueIdentifier: "my-uuid",
		httpClient: &http.Client{
			Timeout:   requestTimeout,
			Transport: testTransport,
		},
	}
}
