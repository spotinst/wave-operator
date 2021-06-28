package client

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetAllOceanClusters(t *testing.T) {

	t.Run("whenSuccessful", func(tt *testing.T) {

		testTransport := transportTestFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewBufferString(getOceanClustersResponse)),
			}, nil
		})

		c := clientWithTestTransport(testTransport)

		res, err := c.GetAllOceanClusters()
		require.NoError(tt, err)

		expected := []*OceanCluster{
			{
				ID:                  "o-e818XXXXe661",
				Name:                "wave-cluster-04",
				ControllerClusterId: "wave-cluster-04",
				Region:              "us-west-2",
				Compute: OceanClusterCompute{
					InstanceTypes: OceanClusterInstanceTypes{
						Whitelist: nil,
						Blacklist: []string{
							"c3.xlarge",
							"c3.4xlarge",
							"c3.2xlarge",
							"z1d.3xlarge",
						},
					},
				},
			},
			{
				ID:                  "o-12345",
				Name:                "bigdata-test-ekctl",
				ControllerClusterId: "bigdata-test-ekctl",
				Region:              "us-west-2",
				Compute: OceanClusterCompute{
					InstanceTypes: OceanClusterInstanceTypes{
						Whitelist: []string{
							"c5.large",
							"c6g.large",
							"t2.large",
						},
						Blacklist: nil,
					},
				},
			},
		}

		assert.Equal(tt, expected, res)

	})

}

var getOceanClustersResponse = `
{
  "request": {
    "id": "2f905bdd-adce-4383-ba79-2eabd9f2cd20",
    "url": "/ocean/aws/k8s/cluster?accountId=act-12f6b1b9",
    "method": "GET",
    "timestamp": "2021-06-10T08:51:01.471Z"
  },
  "response": {
    "status": {
      "code": 200,
      "message": "OK"
    },
    "kind": "spotinst:ocean:aws:k8s",
    "items": [
      {
        "id": "o-e818XXXXe661",
        "name": "wave-cluster-04",
        "controllerClusterId": "wave-cluster-04",
        "region": "us-west-2",
        "autoScaler": {
          "isEnabled": true,
          "cooldown": 300,
          "down": {
            "maxScaleDownPercentage": 10
          },
          "headroom": {
            "cpuPerUnit": 2,
            "memoryPerUnit": 64,
            "gpuPerUnit": 0,
            "numOfUnits": 2
          },
          "isAutoConfig": true,
          "autoHeadroomPercentage": 5,
          "resourceLimits": {
            "maxVCpu": 20000,
            "maxMemoryGib": 100000
          }
        },
        "capacity": {
          "minimum": 0,
          "maximum": 10,
          "target": 1
        },
        "strategy": {
          "utilizeReservedInstances": true,
          "fallbackToOd": true,
          "spotPercentage": 60
        },
        "compute": {
          "subnetIds": [
            "subnet-0635dc54cXXXXXXX3",
            "subnet-093d8809c977XXXX4",
            "subnet-05b45b67XXXXXXXX2"
          ],
          "instanceTypes": {
            "blacklist": [
              "c3.xlarge",
              "c3.4xlarge",
              "c3.2xlarge",
              "z1d.3xlarge"
            ]
          },
          "launchSpecification": {
            "securityGroupIds": [
              "sg-024c3XXXXXXXXXXXb",
              "sg-07XXXXXXXXXXXXXX4"
            ],
            "iamInstanceProfile": {
              "arn": "arn:.."
            },
            "keyPair": "eksctl...",
            "imageId": "ami-0cdd9XXXXXXXXdb",
            "userData": "H4sIA",
            "tags": [
              {
                "tagKey": "alpha.eksctl.io/nodegroup-name",
                "tagValue": "ocean"
              },
              {
                "tagKey": "eksctl.io/v1alpha2/nodegroup-name",
                "tagValue": "ocean"
              }
            ],
            "rootVolumeSize": 80,
            "useAsTemplateOnly": false
          }
        },
        "scheduling": {
          "shutdownHours": {
            "timeWindows": [
              "Mon:18:00-Tue:06:00",
              "Tue:18:00-Wed:06:00",
              "Wed:18:00-Thu:06:00",
              "Thu:18:00-Fri:06:00",
              "Fri:18:00-Sun:06:00",
              "Sun:18:00-Mon:06:00"
            ],
            "isEnabled": true
          }
        },
        "createdAt": "2021-04-13T07:28:55.000Z",
        "updatedAt": "2021-06-01T06:07:20.000Z"
      },
      {
        "id": "o-12345",
        "name": "bigdata-test-ekctl",
        "controllerClusterId": "bigdata-test-ekctl",
        "region": "us-west-2",
        "autoScaler": {
          "isEnabled": true,
          "cooldown": 300,
          "down": {
            "maxScaleDownPercentage": 10
          },
          "headroom": {
            "cpuPerUnit": 2,
            "memoryPerUnit": 64,
            "gpuPerUnit": 0,
            "numOfUnits": 2
          },
          "isAutoConfig": true,
          "autoHeadroomPercentage": 5,
          "resourceLimits": {
            "maxVCpu": 20000,
            "maxMemoryGib": 100000
          }
        },
        "capacity": {
          "minimum": 0,
          "maximum": 10,
          "target": 1
        },
        "strategy": {
          "utilizeReservedInstances": true,
          "fallbackToOd": true,
          "spotPercentage": 60
        },
        "compute": {
          "subnetIds": [
            "subnet-02e3cb75XXXXXXX23",
            "subnet-0c54133961eXXXX09",
            "subnet-0144af5074cXXXX0a"
          ],
          "instanceTypes": {
            "whitelist": [
              "c5.large",
              "c6g.large",
              "t2.large"
            ]
          },
          "launchSpecification": {
            "securityGroupIds": [
              "sg-09e76b036c13f0ee4",
              "sg-0fbd69bd85052f6d3"
            ],
            "iamInstanceProfile": {
              "arn": "whatever"
            },
            "keyPair": "my-keys",
            "imageId": "ami-0cdXXXXa797fd5db",
            "userData": "H4s=...",
            "tags": [
              {
                "tagKey": "alpha.eksctl.io/nodegroup-name",
                "tagValue": "ocean"
              },
              {
                "tagKey": "environment",
                "tagValue": "big-data-labs"
              }
            ],
            "rootVolumeSize": 80,
            "useAsTemplateOnly": false
          }
        },
        "scheduling": {
          "shutdownHours": {
            "timeWindows": [
              "Mon:16:30-Tue:07:30",
              "Tue:16:30-Wed:07:30",
              "Wed:16:30-Thu:07:30",
              "Thu:16:30-Fri:07:30",
              "Fri:16:30-Mon:07:30"
            ],
            "isEnabled": true
          }
        },
        "createdAt": "2021-04-06T15:00:52.000Z",
        "updatedAt": "2021-05-06T16:30:55.000Z"
      }
    ],
    "count": 2
  }
}
`
