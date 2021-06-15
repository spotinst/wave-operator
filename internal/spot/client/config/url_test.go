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
	originalEnv := os.Environ()
	originalSpotEnvVars := getSpotEnvVars()
	defer restoreEnvVars(t, originalSpotEnvVars, originalEnv)

	t.Run("whenAllMissing_shouldDefault", func(tt *testing.T) {
		clearSpotEnvVars(tt)
		res, err := getBaseURL(getTestCM(nil), log)
		require.NoError(tt, err)
		assert.Equal(t, "api.spotinst.io", res.Host)
		assert.Equal(t, "https", res.Scheme)
	})

	t.Run("whenEnvVar", func(tt *testing.T) {
		clearSpotEnvVars(tt)
		require.NoError(tt, os.Setenv(envVarBaseURL, "https://my-test-url.io"))
		res, err := getBaseURL(getTestCM(nil), log)
		require.NoError(tt, err)
		assert.Equal(t, "my-test-url.io", res.Host)
		assert.Equal(t, "https", res.Scheme)
	})

	t.Run("whenFallback", func(tt *testing.T) {
		clearSpotEnvVars(tt)
		cmData := map[string]string{
			ocean.SpotinstBaseURL: "http://base-url-from-cm.io",
		}
		res, err := getBaseURL(getTestCM(cmData), log)
		require.NoError(tt, err)
		assert.Equal(t, "base-url-from-cm.io", res.Host)
		assert.Equal(t, "http", res.Scheme)
	})

	t.Run("whenMalformedURL", func(tt *testing.T) {
		clearSpotEnvVars(tt)
		require.NoError(tt, os.Setenv(envVarBaseURL, "https://nonsense!#$%$&"))
		_, err := getBaseURL(getTestCM(nil), log)
		require.Error(tt, err)

		clearSpotEnvVars(tt)
		cmData := map[string]string{
			ocean.SpotinstBaseURL: "nonsense",
		}
		_, err = getBaseURL(getTestCM(cmData), log)
		require.Error(tt, err)
	})

}

func TestGetProxyConfiguration(t *testing.T) {
	log := logger.New()
	originalEnv := os.Environ()
	originalSpotEnvVars := getSpotEnvVars()
	defer restoreEnvVars(t, originalSpotEnvVars, originalEnv)

	t.Run("whenAllMissing", func(tt *testing.T) {
		clearSpotEnvVars(tt)
		res, err := getProxyConfiguration(getTestCM(nil), log)
		require.NoError(tt, err)
		assert.Nil(tt, res)
	})

	t.Run("whenEnvVar", func(tt *testing.T) {
		clearSpotEnvVars(tt)
		require.NoError(tt, os.Setenv(envVarProxyURL, "https://my-test-url.io"))
		res, err := getProxyConfiguration(getTestCM(nil), log)
		require.NoError(tt, err)
		assert.Equal(tt, "my-test-url.io", res.HTTPSProxy.Host)
	})

	t.Run("whenFallback", func(tt *testing.T) {
		clearSpotEnvVars(tt)
		cmData := map[string]string{
			ocean.SpotinstProxyURL: "https://proxy-url-from-cm.io",
		}
		res, err := getProxyConfiguration(getTestCM(cmData), log)
		require.NoError(tt, err)
		assert.Equal(tt, "proxy-url-from-cm.io", res.HTTPSProxy.Host)
	})

	t.Run("whenHTTP", func(tt *testing.T) {
		clearSpotEnvVars(tt)
		require.NoError(tt, os.Setenv(envVarProxyURL, "http://my-http-test-url.io"))
		res, err := getProxyConfiguration(getTestCM(nil), log)
		require.NoError(tt, err)
		assert.Equal(tt, "my-http-test-url.io", res.HTTPProxy.Host)
		assert.Nil(tt, res.HTTPSProxy)
	})

	t.Run("whenHTTPS", func(tt *testing.T) {
		clearSpotEnvVars(tt)
		require.NoError(tt, os.Setenv(envVarProxyURL, "https://my-https-test-url.io"))
		res, err := getProxyConfiguration(getTestCM(nil), log)
		require.NoError(tt, err)
		assert.Equal(tt, "my-https-test-url.io", res.HTTPSProxy.Host)
		assert.Nil(tt, res.HTTPProxy)
	})

	t.Run("whenMalformedURL", func(tt *testing.T) {
		clearSpotEnvVars(tt)
		require.NoError(tt, os.Setenv(envVarProxyURL, "https://nonsense!#$%$&"))
		res, err := getProxyConfiguration(getTestCM(nil), log)
		require.Error(tt, err)
		assert.Nil(tt, res)

		clearSpotEnvVars(tt)
		require.NoError(tt, os.Setenv(envVarProxyURL, "nonsense"))
		res, err = getProxyConfiguration(getTestCM(nil), log)
		require.Error(tt, err)
		assert.Nil(tt, res)

		clearSpotEnvVars(tt)
		require.NoError(tt, os.Setenv(envVarProxyURL, "smtp://nonsense.io"))
		res, err = getProxyConfiguration(getTestCM(nil), log)
		require.Error(tt, err)
		assert.Nil(tt, res)
	})

}
