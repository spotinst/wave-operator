package client

import (
	"fmt"
	"github.com/spotinst/wave-operator/internal/logger"
	"github.com/stretchr/testify/require"
	"testing"
)

func AwsTestManual(t *testing.T) {

	c, err := NewClient(logger.New())
	require.NoError(t, err)

	t.Run("GetInstanceTypes", func(t *testing.T) {
		res, err := c.GetAvailableInstanceTypesInRegion("us-west-2")
		require.NoError(t, err)
		fmt.Println(res)
	})

}
