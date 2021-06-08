package spot

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBaseURL(t *testing.T) {

	originalEnvVar := os.Getenv(envVarBaseURL)

	defer func() {
		// Restore env var
		_ = os.Setenv(envVarBaseURL, originalEnvVar)
		assert.Equal(t, originalEnvVar, os.Getenv(envVarBaseURL))
	}()

	err := os.Setenv(envVarBaseURL, "")
	require.NoError(t, err)
	res, err := getBaseURL()
	require.NoError(t, err)
	assert.Equal(t, "api.spotinst.io", res.Host)
	assert.Equal(t, "https", res.Scheme)

	err = os.Setenv(envVarBaseURL, "http://dev.api.url")
	require.NoError(t, err)
	res, err = getBaseURL()
	require.NoError(t, err)
	assert.Equal(t, "dev.api.url", res.Host)
	assert.Equal(t, "http", res.Scheme)

	err = os.Setenv(envVarBaseURL, "https://nonsense!#$%$&")
	require.NoError(t, err)
	res, err = getBaseURL()
	require.Error(t, err)

	err = os.Setenv(envVarBaseURL, "nonsense")
	require.NoError(t, err)
	res, err = getBaseURL()
	require.Error(t, err)

}
