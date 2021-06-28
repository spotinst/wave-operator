//go:generate mockgen -destination=mock_client/aws_mock.go . AWSClient

package client

import (
	"encoding/json"
	"io"
	"net/http"
)

type AWSClient interface {
	InstanceTypesGetter
}

type InstanceTypesGetter interface {
	GetAvailableInstanceTypesInRegion(region string) ([]*InstanceType, error)
}

type InstanceType struct {
	InstanceType string `json:"instanceType"`
}

func (c *Client) GetAvailableInstanceTypesInRegion(region string) ([]*InstanceType, error) {
	req, err := http.NewRequest(http.MethodGet, "aws/ec2/spotType", nil)
	if err != nil {
		return nil, err
	}
	query := req.URL.Query()
	query.Set("region", region)
	req.URL.RawQuery = query.Encode()

	resp, err := RequireOK(c.httpClient.Do(req))
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

	instanceTypes := make([]*InstanceType, len(response.Response.Items))
	for i, item := range response.Response.Items {
		it := new(InstanceType)
		if err := json.Unmarshal(item, it); err != nil {
			return nil, err
		}
		instanceTypes[i] = it
	}

	return instanceTypes, nil
}
