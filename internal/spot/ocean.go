package spot

import (
	"context"
	"fmt"
	"net/http/httputil"
)

type OceanClient interface {
	OceanClusterGetter
}

type OceanClusterGetter interface {
	GetOceanCluster(ctx context.Context, ID string) (string, error)
}

func (c *Client) GetOceanCluster(ctx context.Context, ID string) (string, error) {
	resp, err := c.httpClient.Get(fmt.Sprintf("ocean/aws/k8s/cluster/%s", ID))
	if err != nil {
		return "", err
	}

	body, _ := httputil.DumpResponse(resp, true)
	return string(body), nil
}
