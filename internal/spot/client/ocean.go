//go:generate mockgen -destination=mock_client/ocean_mock.go . OceanClient

package client

import (
	"encoding/json"
	"io"
)

type OceanClient interface {
	OceanClusterGetter
}

type OceanClusterGetter interface {
	GetAllOceanClusters() ([]*OceanCluster, error)
}

type OceanCluster struct {
	ID                  string              `json:"id"`
	Name                string              `json:"name"`
	ControllerClusterId string              `json:"controllerClusterId"`
	Region              string              `json:"region"`
	Compute             OceanClusterCompute `json:"compute"`
}

type OceanClusterCompute struct {
	InstanceTypes OceanClusterInstanceTypes `json:"instanceTypes"`
}

type OceanClusterInstanceTypes struct {
	Whitelist []string `json:"whitelist"`
	Blacklist []string `json:"blacklist"`
}

func (c *Client) GetAllOceanClusters() ([]*OceanCluster, error) {
	resp, err := RequireOK(c.httpClient.Get("ocean/aws/k8s/cluster"))
	if err != nil {
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var response Response
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}

	clusters := make([]*OceanCluster, len(response.Response.Items))
	for i, item := range response.Response.Items {
		c := new(OceanCluster)
		if err := json.Unmarshal(item, c); err != nil {
			return nil, err
		}
		clusters[i] = c
	}

	return clusters, nil
}
