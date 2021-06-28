package client

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetAvailableInstanceTypesInRegion(t *testing.T) {

	t.Run("whenSuccessful", func(tt *testing.T) {

		region := "my-region"

		testTransport := transportTestFunc(func(req *http.Request) (*http.Response, error) {
			assert.Equal(tt, region, req.URL.Query().Get("region"))
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewBufferString(getInstanceTypesInRegionResponse)),
			}, nil
		})

		c := clientWithTestTransport(testTransport)

		res, err := c.GetAvailableInstanceTypesInRegion(region)
		require.NoError(tt, err)

		expected := []*InstanceType{
			{
				InstanceType: "hi1.4xlarge",
			},
			{
				InstanceType: "r5d.4xlarge",
			},
			{
				InstanceType: "r5ad.12xlarge",
			},
			{
				InstanceType: "r4.4xlarge",
			},
		}

		assert.Equal(tt, expected, res)

	})

}

var getInstanceTypesInRegionResponse = `
{
  "request": {
    "id": "cd645d74-8b38-4d37-85d7-78f78b75bcd5",
    "url": "/aws/ec2/spotType?region=us-west-2",
    "method": "GET",
    "timestamp": "2021-06-10T08:33:59.441Z"
  },
  "response": {
    "status": {
      "code": 200,
      "message": "OK"
    },
    "kind": "spotinst:aws:ec2:spot:type",
    "items": [
      {
        "instanceType": "hi1.4xlarge"
      },
      {
        "instanceType": "r5d.4xlarge"
      },
      {
        "instanceType": "r5ad.12xlarge"
      },
      {
        "instanceType": "r4.4xlarge"
      }
    ],
    "count": 4
  }
}
`
