package config

import (
	"encoding/base64"
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
		encoded := base64.StdEncoding.EncodeToString([]byte(v))
		secret.Data[k] = []byte(encoded)
	}
	return secret
}
