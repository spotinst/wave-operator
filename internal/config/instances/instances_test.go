package instances

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/spotinst/wave-operator/internal/logger"
	"github.com/spotinst/wave-operator/internal/spot/client"
	"github.com/spotinst/wave-operator/internal/spot/client/mock_client"
)

func TestRefreshAllowedInstanceTypes(t *testing.T) {

	type testCase struct {
		instanceTypesInRegion          []*client.InstanceType
		whitelist                      []string
		blacklist                      []string
		getOceanClustersError          error
		getOceanClustersCallCount      int
		getInstanceTypesError          error
		getInstanceTypesCallCount      int
		oceanClusterIdentifierOverride string
		expected                       map[instanceTypeFamily]map[instanceType]bool
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

		err := manager.refreshAllowedInstanceTypes()
		if tc.expectedError != "" {
			require.Error(tt, err)
			assert.Contains(tt, err.Error(), tc.expectedError)
		} else {
			require.NoError(tt, err)
			assert.Equal(tt, tc.expected, manager.allowedInstanceTypes.m)
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
			expected: map[instanceTypeFamily]map[instanceType]bool{
				"m5": {
					instanceType{
						Family: "m5",
						Type:   "xlarge",
					}: true,
				},
				"r5": {
					instanceType{
						Family: "r5",
						Type:   "99xlarge",
					}: true,
					instanceType{
						Family: "r5",
						Type:   "small",
					}: true,
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
			expected: map[instanceTypeFamily]map[instanceType]bool{
				"m5": {
					instanceType{
						Family: "m5",
						Type:   "xlarge",
					}: true,
				},
				"r5": {
					instanceType{
						Family: "r5",
						Type:   "small",
					}: true,
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
			expected: map[instanceTypeFamily]map[instanceType]bool{
				"m5": {
					instanceType{
						Family: "m5",
						Type:   "xlarge",
					}: true,
				},
				"r5": {
					instanceType{
						Family: "r5",
						Type:   "99xlarge",
					}: true,
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
			expected: map[instanceTypeFamily]map[instanceType]bool{
				"m5": {
					instanceType{
						Family: "m5",
						Type:   "xlarge",
					}: true,
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
			getOceanClustersCallCount: 1,
			getInstanceTypesCallCount: 0,
			expected:                  map[instanceTypeFamily]map[instanceType]bool{},
			expectedError:             "could not get ocean clusters",
		}
		testFunc(tt, tc)
	})

	t.Run("whenOceanClusterNotFound", func(tt *testing.T) {
		tc := testCase{
			instanceTypesInRegion:          []*client.InstanceType{},
			whitelist:                      nil,
			blacklist:                      nil,
			getOceanClustersCallCount:      1,
			getInstanceTypesCallCount:      0,
			oceanClusterIdentifierOverride: "name-override",
			expected:                       map[instanceTypeFamily]map[instanceType]bool{},
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
			getOceanClustersCallCount: 1,
			getInstanceTypesCallCount: 1,
			expected:                  map[instanceTypeFamily]map[instanceType]bool{},
			expectedError:             "could not get instance types",
		}
		testFunc(tt, tc)
	})

}

var testAllowedInstanceTypes = map[instanceTypeFamily]map[instanceType]bool{
	"m5": {
		instanceType{
			Family: "m5",
			Type:   "large",
		}: true,
		instanceType{
			Family: "m5",
			Type:   "xlarge",
		}: true,
	},
	"r3": {
		instanceType{
			Family: "r3",
			Type:   "small",
		}: true,
		instanceType{
			Family: "r3",
			Type:   "large",
		}: true,
	},
	"h1": {
		instanceType{
			Family: "h1",
			Type:   "small",
		}: true,
		instanceType{
			Family: "h1",
			Type:   "medium",
		}: true,
		instanceType{
			Family: "h1",
			Type:   "large",
		}: true,
	},
	"g5": {},
}

func TestValidateInstanceType(t *testing.T) {

	t.Run("whenAllowedInstanceTypesUnknown", func(tt *testing.T) {

		m := manager{
			allowedInstanceTypes: instanceTypes{},
			log:                  logger.New(),
		}

		err := m.ValidateInstanceType("family.type")
		require.NoError(tt, err)

		err = m.ValidateInstanceType("family,type")
		require.Error(tt, err)

		err = m.ValidateInstanceType("family.type.test")
		require.Error(tt, err)

		err = m.ValidateInstanceType("family")
		require.Error(tt, err)

		err = m.ValidateInstanceType("family.")
		require.Error(tt, err)

		err = m.ValidateInstanceType(".type")
		require.Error(tt, err)

		err = m.ValidateInstanceType(".")
		require.Error(tt, err)

		err = m.ValidateInstanceType("")
		require.Error(tt, err)

	})

	t.Run("whenAllowedInstanceTypesKnown", func(tt *testing.T) {

		m := manager{
			allowedInstanceTypes: instanceTypes{m: testAllowedInstanceTypes},
			log:                  logger.New(),
		}

		// Allowed instance type
		err := m.ValidateInstanceType("r3.large")
		require.NoError(tt, err)

		// Forbidden instance type
		err = m.ValidateInstanceType("family.type")
		require.Error(tt, err)
		err = m.ValidateInstanceType("r3.1024large")
		require.Error(tt, err)
		err = m.ValidateInstanceType("c5.large")
		require.Error(tt, err)

		// Malformed instance types
		err = m.ValidateInstanceType("c5.large.test")
		require.Error(tt, err)
		err = m.ValidateInstanceType("g5.")
		require.Error(tt, err)
		err = m.ValidateInstanceType(".large")
		require.Error(tt, err)
		err = m.ValidateInstanceType(".")
		require.Error(tt, err)
		err = m.ValidateInstanceType("nonsense")
		require.Error(tt, err)
		err = m.ValidateInstanceType("")
		require.Error(tt, err)

	})

}

func TestGetValidInstanceTypesInFamily(t *testing.T) {

	t.Run("whenAllowedInstanceTypesUnknown", func(tt *testing.T) {

		m := manager{
			allowedInstanceTypes: instanceTypes{},
			log:                  logger.New(),
		}

		res, err := m.GetValidInstanceTypesInFamily("h1")
		require.Error(tt, err)
		assert.Equal(tt, 0, len(res))

	})

	t.Run("whenAllowedInstanceTypesKnown", func(tt *testing.T) {

		m := manager{
			allowedInstanceTypes: instanceTypes{m: testAllowedInstanceTypes},
			log:                  logger.New(),
		}

		// Allowed instance type family
		res, err := m.GetValidInstanceTypesInFamily("h1")
		require.NoError(tt, err)
		assert.Equal(tt, 3, len(res))
		assert.Contains(tt, res, "h1.small")
		assert.Contains(tt, res, "h1.medium")
		assert.Contains(tt, res, "h1.large")

		// Forbidden instance type families
		res, err = m.GetValidInstanceTypesInFamily("c5")
		require.Error(tt, err)
		res, err = m.GetValidInstanceTypesInFamily("g5")
		require.Error(tt, err)

		// Malformed instance type family
		res, err = m.GetValidInstanceTypesInFamily("h1.")
		require.Error(tt, err)
		res, err = m.GetValidInstanceTypesInFamily(",")
		require.Error(tt, err)
		res, err = m.GetValidInstanceTypesInFamily(".")
		require.Error(tt, err)

	})

}
