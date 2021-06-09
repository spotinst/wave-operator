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

func TestGetAllowedInstanceTypes(t *testing.T) {

	type testCase struct {
		instanceTypesInRegion          []*client.InstanceType
		whitelist                      []string
		blacklist                      []string
		getOceanClustersError          error
		getOceanClustersCallCount      int
		getInstanceTypesError          error
		getInstanceTypesCallCount      int
		oceanClusterIdentifierOverride string
		expected                       InstanceTypes
		expectedError                  string
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testFunc := func(tt *testing.T, tc testCase) {

		clusterIdentifier := "my-test-cluster"
		region := "us-west-99"

		responseOceanClusterID := clusterIdentifier
		if tc.oceanClusterIdentifierOverride != "" {
			responseOceanClusterID = tc.oceanClusterIdentifierOverride
		}

		oceanClustersResponse := []*client.OceanCluster{
			{
				ID:                  "o-something",
				Name:                "whatever",
				ControllerClusterId: responseOceanClusterID,
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
		mockOceanClient.EXPECT().GetAllOceanClusters().Return(oceanClustersResponse, tc.getOceanClustersError).Times(tc.getOceanClustersCallCount)

		mockAWSClient := mock_client.NewMockAWSClient(ctrl)
		mockAWSClient.EXPECT().GetAvailableInstanceTypesInRegion(region).Return(tc.instanceTypesInRegion, tc.getInstanceTypesError).Times(tc.getInstanceTypesCallCount)

		manager := &manager{
			clusterIdentifier: clusterIdentifier,
			oceanClient:       mockOceanClient,
			awsClient:         mockAWSClient,
			log:               logger.New(),
		}

		res, err := manager.GetAllowedInstanceTypes()
		if tc.expectedError != "" {
			require.Error(tt, err)
			assert.Contains(tt, err.Error(), tc.expectedError)
		} else {
			require.NoError(tt, err)
			assert.Equal(tt, tc.expected, res)
		}

		// Should not fetch ocean cluster and instance types in region again
		res2, err := manager.GetAllowedInstanceTypes()
		if tc.expectedError != "" {
			require.Error(tt, err)
			assert.Contains(tt, err.Error(), tc.expectedError)
		} else {
			require.NoError(tt, err)
			assert.Equal(tt, tc.expected, res2)
		}

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
			whitelist:                 nil,
			blacklist:                 nil,
			getOceanClustersCallCount: 1,
			getInstanceTypesCallCount: 1,
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
			blacklist:                 nil,
			getOceanClustersCallCount: 1,
			getInstanceTypesCallCount: 1,
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
			getOceanClustersCallCount: 1,
			getInstanceTypesCallCount: 1,
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

	t.Run("whenMalformedInstanceTypes", func(tt *testing.T) {
		tc := testCase{
			instanceTypesInRegion: []*client.InstanceType{
				{
					InstanceType: "m5.xlarge",
				},
				{
					InstanceType: "nonsense! should be ignored",
				},
				{
					InstanceType: "r5.",
				},
			},
			whitelist:                 nil,
			blacklist:                 nil,
			getOceanClustersCallCount: 1,
			getInstanceTypesCallCount: 1,
			expected: InstanceTypes{
				"m5": {
					"m5.xlarge": true,
				},
			},
		}
		testFunc(tt, tc)
	})

	t.Run("whenGetOceanClusterError", func(tt *testing.T) {
		tc := testCase{
			instanceTypesInRegion:     []*client.InstanceType{},
			whitelist:                 nil,
			blacklist:                 nil,
			getOceanClustersError:     fmt.Errorf("test error"),
			getOceanClustersCallCount: 2,
			getInstanceTypesCallCount: 0,
			expected:                  InstanceTypes{},
			expectedError:             "could not get ocean clusters",
		}
		testFunc(tt, tc)
	})

	t.Run("whenOceanClusterNotFound", func(tt *testing.T) {
		tc := testCase{
			instanceTypesInRegion:          []*client.InstanceType{},
			whitelist:                      nil,
			blacklist:                      nil,
			getOceanClustersCallCount:      2,
			getInstanceTypesCallCount:      0,
			oceanClusterIdentifierOverride: "name-override",
			expected:                       InstanceTypes{},
			expectedError:                  "could not get ocean cluster",
		}
		testFunc(tt, tc)
	})

	t.Run("whenGetInstanceTypesError", func(tt *testing.T) {
		tc := testCase{
			instanceTypesInRegion:     []*client.InstanceType{},
			whitelist:                 nil,
			blacklist:                 nil,
			getInstanceTypesError:     fmt.Errorf("test error"),
			getOceanClustersCallCount: 2,
			getInstanceTypesCallCount: 2,
			expected:                  InstanceTypes{},
			expectedError:             "could not get instance types",
		}
		testFunc(tt, tc)
	})

}
