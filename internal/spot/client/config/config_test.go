package config

import (
	"os"

	corev1 "k8s.io/api/core/v1"
)

func getTestCM(data map[string]string) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{}
	cm.Data = data
	return cm
}

func getTestSecret(data map[string]string) *corev1.Secret {
	secret := &corev1.Secret{}
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
