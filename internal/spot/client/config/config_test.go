package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/spotinst/wave-operator/internal/logger"
	"github.com/spotinst/wave-operator/internal/ocean"
)

func getTestCM(data map[string]string) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{}
	cm.Name = ocean.SpotinstOceanConfigmap
	cm.Namespace = v1.NamespaceSystem
	cm.Data = data
	return cm
}

func getTestSecret(data map[string]string) *corev1.Secret {
	secret := &corev1.Secret{}
	secret.Name = ocean.SpotinstOceanSecret
	secret.Namespace = v1.NamespaceSystem
	secret.Data = make(map[string][]byte)
	for k, v := range data {
		secret.Data[k] = []byte(v)
	}
	return secret
}

type envVar struct {
	val string
	set bool
}

func getEnvVar(key string) envVar {
	val, set := os.LookupEnv(key)
	return envVar{val: val, set: set}
}

func getSpotEnvVars() map[string]envVar {
	return map[string]envVar{
		envVarToken:             getEnvVar(envVarToken),
		envVarTokenLegacy:       getEnvVar(envVarTokenLegacy),
		envVarAccount:           getEnvVar(envVarAccount),
		envVarAccountLegacy:     getEnvVar(envVarAccountLegacy),
		envVarClusterIdentifier: getEnvVar(envVarClusterIdentifier),
		envVarProxyURL:          getEnvVar(envVarProxyURL),
		envVarBaseURL:           getEnvVar(envVarBaseURL),
	}
}

func clearSpotEnvVars(t *testing.T) {
	require.NoError(t, os.Unsetenv(envVarToken))
	require.NoError(t, os.Unsetenv(envVarTokenLegacy))
	require.NoError(t, os.Unsetenv(envVarAccount))
	require.NoError(t, os.Unsetenv(envVarAccountLegacy))
	require.NoError(t, os.Unsetenv(envVarClusterIdentifier))
	require.NoError(t, os.Unsetenv(envVarProxyURL))
	require.NoError(t, os.Unsetenv(envVarBaseURL))
}

func restoreEnvVars(t *testing.T, original map[string]envVar, originalEnv []string) {
	for k, v := range original {
		if v.set {
			assert.NoError(t, os.Setenv(k, v.val))
		} else {
			assert.NoError(t, os.Unsetenv(k))
		}
	}
	restoredEnv := os.Environ()
	assert.True(t, envsEqual(originalEnv, restoredEnv))
}

func envsEqual(env1 []string, env2 []string) bool {
	if len(env1) != len(env2) {
		return false
	}
	for _, x := range env1 {
		found := false
		for _, y := range env2 {
			if x == y {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func TestGetConfig(t *testing.T) {
	log := logger.New()
	originalEnv := os.Environ()
	originalSpotEnvVars := getSpotEnvVars()
	defer restoreEnvVars(t, originalSpotEnvVars, originalEnv)

	t.Run("whenError", func(tt *testing.T) {
		clearSpotEnvVars(tt)

		clientSet := k8sfake.NewSimpleClientset()
		_, err := GetConfig(clientSet, log)
		require.Error(tt, err)

		cm := getTestCM(nil)
		secret := getTestSecret(nil)
		clientSet = k8sfake.NewSimpleClientset(cm, secret)
		_, err = GetConfig(clientSet, log)
		require.Error(tt, err)

	})

	t.Run("whenSuccessful", func(tt *testing.T) {
		clearSpotEnvVars(tt)

		ns := &corev1.Namespace{
			ObjectMeta: v1.ObjectMeta{
				Name: "kube-system",
				UID:  "1234-5678",
			},
		}
		cm := getTestCM(map[string]string{
			ocean.SpotinstBaseURL:           "https://my-url.io",
			ocean.SpotinstProxyURL:          "https://my-proxy-url.io",
			ocean.SpotinstClusterIdentifier: "my-cluster-identifier",
		})
		secret := getTestSecret(map[string]string{
			ocean.SpotinstToken:   "my-token",
			ocean.SpotinstAccount: "my-account",
		})
		clientSet := k8sfake.NewSimpleClientset(ns, cm, secret)

		res, err := GetConfig(clientSet, log)
		require.NoError(tt, err)
		assert.Equal(tt, "my-token", res.Creds.Token)
		assert.Equal(tt, "my-account", res.Creds.Account)
		assert.Equal(tt, "my-url.io", res.BaseURL.Host)
		assert.Equal(tt, "my-proxy-url.io", res.ProxyConfig.HTTPSProxy.Host)
		assert.Equal(tt, "my-cluster-identifier", res.ClusterIdentifier)
		assert.Equal(tt, "1234-5678", res.ClusterUniqueIdentifier)

	})
}

func TestGetClusterIdentifier(t *testing.T) {
	log := logger.New()
	originalEnv := os.Environ()
	originalSpotEnvVars := getSpotEnvVars()
	defer restoreEnvVars(t, originalSpotEnvVars, originalEnv)

	t.Run("whenAllMissing", func(tt *testing.T) {
		clearSpotEnvVars(tt)
		_, err := GetClusterIdentifier(getTestCM(nil), log)
		require.Error(tt, err)
	})

	t.Run("whenEnvVar", func(tt *testing.T) {
		clearSpotEnvVars(tt)
		require.NoError(tt, os.Setenv(envVarClusterIdentifier, "my-cluster-id-from-env"))
		res, err := GetClusterIdentifier(getTestCM(nil), log)
		require.NoError(tt, err)
		assert.Equal(tt, "my-cluster-id-from-env", res)
	})

	t.Run("whenFallback", func(tt *testing.T) {
		clearSpotEnvVars(tt)
		cmData := map[string]string{
			ocean.SpotinstClusterIdentifier: "my-cluster-id-from-cm",
		}
		res, err := GetClusterIdentifier(getTestCM(cmData), log)
		require.NoError(tt, err)
		assert.Equal(tt, "my-cluster-id-from-cm", res)
	})
}
