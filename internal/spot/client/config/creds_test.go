package config

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetCredentials(t *testing.T) {

	originalEnv := os.Environ()

	defer func() {
		// Restore env
		os.Clearenv()
		for _, kv := range originalEnv {
			p := strings.SplitN(kv, "=", 2)
			k, v := p[0], ""
			if len(p) > 1 {
				v = p[1]
			}
			_ = os.Setenv(k, v)
		}
		restoredEnv := os.Environ()
		assert.Equal(t, originalEnv, restoredEnv)
	}()

	os.Clearenv()
	_, err := GetCredentials()
	require.Error(t, err)

	os.Clearenv()
	err = os.Setenv(envVarToken, "myToken")
	require.NoError(t, err)
	_, err = GetCredentials()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could not get account")

	os.Clearenv()
	err = os.Setenv(envVarAccount, "myAccount")
	require.NoError(t, err)
	_, err = GetCredentials()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could not get token")

	os.Clearenv()
	err = os.Setenv(envVarToken, "myToken")
	require.NoError(t, err)
	err = os.Setenv(envVarAccount, "myAccount")
	require.NoError(t, err)
	res, err := GetCredentials()
	require.NoError(t, err)
	assert.Equal(t, "myToken", res.Token)
	assert.Equal(t, "myAccount", res.Account)

}
