package config

import (
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/spotinst/wave-operator/internal/logger"
	"github.com/spotinst/wave-operator/internal/spot/client"
	"github.com/spotinst/wave-operator/internal/spot/client/mock_client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func ManualTestLoadInstanceTypes(t *testing.T) {

	c, err := client.NewClient(logger.New())
	require.NoError(t, err)

	m := NewInstanceTypeManager(c, "thorsteinn-wave-19", logger.New())
	res, err := m.GetAllowedInstanceTypes()
	assert.NoError(t, err)
	fmt.Println(res)

}

func TestGetAllowedInstanceTypes_whenSuccessful(t *testing.T) {

	type testCase struct {
		instanceTypesInRegion []*client.InstanceType
		whitelist             []string
		blacklist             []string
		expected              InstanceTypes
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testFunc := func(tt *testing.T, tc testCase) {

		clusterIdentifier := "my-test-cluster"
		region := "us-west-99"

		oceanClustersResponse := []*client.OceanCluster{
			{
				ID:                  "o-something",
				Name:                "whatever",
				ControllerClusterId: clusterIdentifier,
				Region:              region,
				Compute: client.OceanClusterCompute{
					InstanceTypes: client.OceanClusterInstanceTypes{
						Whitelist: tc.whitelist,
						Blacklist: tc.blacklist,
					},
				},
			},
		}

		mockOceanClient := mock_client.NewMockOceanClient(ctrl)
		mockOceanClient.EXPECT().GetAllOceanClusters().Return(oceanClustersResponse, nil).Times(1)

		mockAWSClient := mock_client.NewMockAWSClient(ctrl)
		mockAWSClient.EXPECT().GetAvailableInstanceTypesInRegion(region).Return(tc.instanceTypesInRegion, nil).Times(1)

		manager := &manager{
			clusterIdentifier: clusterIdentifier,
			oceanClient:       mockOceanClient,
			awsClient:         mockAWSClient,
			log:               logger.New(),
		}

		res, err := manager.GetAllowedInstanceTypes()
		require.NoError(tt, err)
		assert.Equal(tt, tc.expected, res)

		// Should not fetch ocean cluster and instance types in region again
		res2, err := manager.GetAllowedInstanceTypes()
		require.NoError(tt, err)
		assert.Equal(tt, res, res2)

	}

	t.Run("whenNoWhiteOrBlacklist", func(tt *testing.T) {
		tc := testCase{
			instanceTypesInRegion: []*client.InstanceType{
				{
					InstanceType: "m5.xlarge",
				},
				{
					InstanceType: "r5.99xlarge",
				},
				{
					InstanceType: "r5.small",
				},
			},
			whitelist: nil,
			blacklist: nil,
			expected: InstanceTypes{
				"m5": {
					"m5.xlarge": true,
				},
				"r5": {
					"r5.99xlarge": true,
					"r5.small":    true,
				},
			},
		}
		testFunc(tt, tc)
	})

	t.Run("whenWhitelist", func(tt *testing.T) {
		tc := testCase{
			instanceTypesInRegion: []*client.InstanceType{
				{
					InstanceType: "m5.xlarge",
				},
				{
					InstanceType: "r5.99xlarge",
				},
				{
					InstanceType: "r5.small",
				},
			},
			whitelist: []string{
				"m5.xlarge",
				"r5.small",
			},
			blacklist: nil,
			expected: InstanceTypes{
				"m5": {
					"m5.xlarge": true,
				},
				"r5": {
					"r5.small": true,
				},
			},
		}
		testFunc(tt, tc)
	})

	t.Run("whenBlacklist", func(tt *testing.T) {
		tc := testCase{
			instanceTypesInRegion: []*client.InstanceType{
				{
					InstanceType: "m5.xlarge",
				},
				{
					InstanceType: "r5.99xlarge",
				},
				{
					InstanceType: "r5.small",
				},
			},
			whitelist: nil,
			blacklist: []string{
				"r5.small",
			},
			expected: InstanceTypes{
				"m5": {
					"m5.xlarge": true,
				},
				"r5": {
					"r5.99xlarge": true,
				},
			},
		}
		testFunc(tt, tc)
	})

}
