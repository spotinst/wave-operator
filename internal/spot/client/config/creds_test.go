package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/spotinst/wave-operator/internal/logger"
	"github.com/spotinst/wave-operator/internal/ocean"
)

func TestGetCredentials(t *testing.T) {
	log := logger.New()

	originalEnv := os.Environ()
	originalChangedVars := map[string]envVar{
		envVarToken:         getEnvVar(envVarToken),
		envVarTokenLegacy:   getEnvVar(envVarTokenLegacy),
		envVarAccount:       getEnvVar(envVarAccount),
		envVarAccountLegacy: getEnvVar(envVarAccountLegacy),
	}

	clearEnv := func(tt *testing.T) {
		require.NoError(tt, os.Unsetenv(envVarToken))
		require.NoError(tt, os.Unsetenv(envVarTokenLegacy))
		require.NoError(tt, os.Unsetenv(envVarAccount))
		require.NoError(tt, os.Unsetenv(envVarAccountLegacy))
	}

	defer func() {
		// Restore env
		for k, v := range originalChangedVars {
			if v.set {
				assert.NoError(t, os.Setenv(k, v.val))
			} else {
				assert.NoError(t, os.Unsetenv(k))
			}
		}
		restoredEnv := os.Environ()
		assert.True(t, envsEqual(originalEnv, restoredEnv))
	}()

	t.Run("whenAllMissing", func(tt *testing.T) {
		clearEnv(tt)
		_, err := getCredentials(getTestCM(nil), getTestSecret(nil), log)
		require.Error(tt, err)
		assert.Contains(tt, err.Error(), "could not get required")
	})

	t.Run("whenTokenMissing", func(tt *testing.T) {
		clearEnv(tt)
		require.NoError(tt, os.Setenv(envVarAccount, "my-account"))
		_, err := getCredentials(getTestCM(nil), getTestSecret(nil), log)
		require.Error(t, err)
		assert.Contains(tt, err.Error(), "could not get token")
	})

	t.Run("whenAccountMissing", func(tt *testing.T) {
		clearEnv(tt)
		require.NoError(tt, os.Setenv(envVarToken, "my-token"))
		_, err := getCredentials(getTestCM(nil), getTestSecret(nil), log)
		require.Error(t, err)
		assert.Contains(tt, err.Error(), "could not get account")
	})

	t.Run("whenEnvVars", func(tt *testing.T) {
		clearEnv(tt)
		require.NoError(tt, os.Setenv(envVarToken, "my-token"))
		require.NoError(tt, os.Setenv(envVarAccount, "my-account"))
		res, err := getCredentials(getTestCM(nil), getTestSecret(nil), log)
		require.NoError(t, err)
		assert.Equal(tt, Credentials{
			Account: "my-account",
			Token:   "my-token",
		}, res)
	})

	t.Run("whenFallbackEnvVars", func(tt *testing.T) {
		clearEnv(tt)
		require.NoError(tt, os.Setenv(envVarTokenLegacy, "my-token-legacy"))
		require.NoError(tt, os.Setenv(envVarAccountLegacy, "my-account-legacy"))
		res, err := getCredentials(getTestCM(nil), getTestSecret(nil), log)
		require.NoError(t, err)
		assert.Equal(tt, Credentials{
			Account: "my-account-legacy",
			Token:   "my-token-legacy",
		}, res)
	})

	t.Run("whenFallbackSecret", func(tt *testing.T) {
		clearEnv(tt)
		secretData := map[string]string{
			ocean.SpotinstToken:   "token-from-secret",
			ocean.SpotinstAccount: "account-from-secret",
		}
		res, err := getCredentials(getTestCM(nil), getTestSecret(secretData), log)
		require.NoError(t, err)
		assert.Equal(tt, Credentials{
			Account: "account-from-secret",
			Token:   "token-from-secret",
		}, res)
	})

	t.Run("whenFallbackConfigMap", func(tt *testing.T) {
		clearEnv(tt)
		cmData := map[string]string{
			ocean.SpotinstTokenLegacy:   "token-from-cm",
			ocean.SpotinstAccountLegacy: "account-from-cm",
		}
		res, err := getCredentials(getTestCM(cmData), getTestSecret(nil), log)
		require.NoError(t, err)
		assert.Equal(tt, Credentials{
			Account: "account-from-cm",
			Token:   "token-from-cm",
		}, res)
	})

	t.Run("prefersEnvVars", func(tt *testing.T) {
		clearEnv(tt)
		require.NoError(tt, os.Setenv(envVarToken, "token-from-env-var"))
		require.NoError(tt, os.Setenv(envVarAccount, "account-from-env-var"))
		secretData := map[string]string{
			ocean.SpotinstToken:   "token-from-secret",
			ocean.SpotinstAccount: "account-from-secret",
		}
		cmData := map[string]string{
			ocean.SpotinstTokenLegacy:   "token-from-cm",
			ocean.SpotinstAccountLegacy: "account-from-cm",
		}
		res, err := getCredentials(getTestCM(cmData), getTestSecret(secretData), log)
		require.NoError(t, err)
		assert.Equal(tt, Credentials{
			Account: "account-from-env-var",
			Token:   "token-from-env-var",
		}, res)
	})

}
