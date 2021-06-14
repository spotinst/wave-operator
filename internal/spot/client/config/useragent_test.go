package config

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetUserAgent(t *testing.T) {
	res := GetUserAgent()
	assert.Contains(t, res, fmt.Sprintf("%s/", productName))
}
