package client

import (
	"fmt"
	"github.com/spotinst/wave-operator/internal/logger"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestManualTestClient(t *testing.T) {

	c, err := NewClient(logger.New())
	require.NoError(t, err)

	t.Run("GetsOceanClusters", func(t *testing.T) {
		res, err := c.GetAllOceanClusters()
		require.NoError(t, err)
		fmt.Println(res)
	})

}
