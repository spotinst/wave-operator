package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/spotinst/wave-operator/internal/logger"
	"github.com/spotinst/wave-operator/internal/ocean"
)

func TestGetBaseURL(t *testing.T) {

	log := logger.New()
	originalEnvVar := os.Getenv(envVarBaseURL)

	defer func() {
		// Restore env var
		_ = os.Setenv(envVarBaseURL, originalEnvVar)
		assert.Equal(t, originalEnvVar, os.Getenv(envVarBaseURL))
	}()

	t.Run("whenAllMissing_shouldDefault", func(tt *testing.T) {
		require.NoError(tt, os.Setenv(envVarBaseURL, ""))
		res, err := getBaseURL(getTestCM(nil), log)
		require.NoError(tt, err)
		assert.Equal(t, "api.spotinst.io", res.Host)
		assert.Equal(t, "https", res.Scheme)
	})

	t.Run("whenEnvVar", func(tt *testing.T) {
		require.NoError(tt, os.Setenv(envVarBaseURL, "https://my-test-url.io"))
		res, err := getBaseURL(getTestCM(nil), log)
		require.NoError(tt, err)
		assert.Equal(t, "my-test-url.io", res.Host)
		assert.Equal(t, "https", res.Scheme)
	})

	t.Run("whenFallback", func(tt *testing.T) {
		require.NoError(tt, os.Setenv(envVarBaseURL, ""))
		cmData := map[string]string{
			ocean.SpotinstBaseURL: "http://base-url-from-cm.io",
		}
		res, err := getBaseURL(getTestCM(cmData), log)
		require.NoError(tt, err)
		assert.Equal(t, "base-url-from-cm.io", res.Host)
		assert.Equal(t, "http", res.Scheme)
	})

	t.Run("whenMalformedURL", func(tt *testing.T) {
		require.NoError(tt, os.Setenv(envVarBaseURL, "https://nonsense!#$%$&"))
		_, err := getBaseURL(getTestCM(nil), log)
		require.Error(tt, err)

		require.NoError(tt, os.Setenv(envVarBaseURL, ""))
		cmData := map[string]string{
			ocean.SpotinstBaseURL: "nonsense",
		}
		_, err = getBaseURL(getTestCM(cmData), log)
		require.Error(tt, err)
	})

}
