package config

import (
	"fmt"
	"github.com/spotinst/wave-operator/internal/logger"
	"github.com/spotinst/wave-operator/internal/spot/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLoadInstanceTypes(t *testing.T) {

	c, err := client.NewClient(logger.New())
	require.NoError(t, err)

	m := NewInstanceTypeManager(c, logger.New())
	res, err := m.GetAllowedInstanceTypes()
	assert.NoError(t, err)
	fmt.Println(res)

}
