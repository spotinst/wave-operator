package ocean

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	SpotinstOceanConfigmap = "spotinst-kubernetes-cluster-controller-config"
	SpotinstOceanSecret    = "spotinst-kubernetes-cluster-controller"

	SpotinstClusterIdentifier = "spotinst.cluster-identifier"
	SpotinstToken             = "token"
	SpotinstTokenLegacy       = "spotinst.token"
	SpotinstAccount           = "account"
	SpotinstAccountLegacy     = "spotinst.account"
	SpotinstProxyURL          = "proxy-url"
	SpotinstBaseURL           = "base-url"
)

func GetClusterIdentifier(cm *corev1.ConfigMap) string {
	return getKeyFromConfigMap(cm, SpotinstClusterIdentifier)
}

func GetClusterUniqueIdentifier(kc kubernetes.Interface) (string, error) {
	ctx := context.TODO()
	ns, err := kc.CoreV1().Namespaces().Get(ctx, v1.NamespaceSystem, v1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("could not get system namespace, %w", err)
	}
	id := string(ns.GetUID())
	return id, nil
}

func GetToken(secret *corev1.Secret, cm *corev1.ConfigMap) string {
	token := getKeyFromSecret(secret, SpotinstToken)
	if token == "" {
		token = getKeyFromConfigMap(cm, SpotinstTokenLegacy)
	}
	return token
}

func GetAccount(secret *corev1.Secret, cm *corev1.ConfigMap) string {
	account := getKeyFromSecret(secret, SpotinstAccount)
	if account == "" {
		account = getKeyFromConfigMap(cm, SpotinstAccountLegacy)
	}
	return account
}

func GetBaseURL(cm *corev1.ConfigMap) string {
	return getKeyFromConfigMap(cm, SpotinstBaseURL)
}

func GetProxyURL(cm *corev1.ConfigMap) string {
	return getKeyFromConfigMap(cm, SpotinstProxyURL)
}

func GetOceanConfigMap(ctx context.Context, kc kubernetes.Interface) (*corev1.ConfigMap, error) {
	cm, err := kc.CoreV1().ConfigMaps(v1.NamespaceSystem).Get(ctx, SpotinstOceanConfigmap, v1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return cm, nil
}

func GetOceanSecret(ctx context.Context, kc kubernetes.Interface) (*corev1.Secret, error) {
	secret, err := kc.CoreV1().Secrets(v1.NamespaceSystem).Get(ctx, SpotinstOceanSecret, v1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return secret, nil
}

func getKeyFromSecret(secret *corev1.Secret, key string) string {
	val := secret.Data[key]
	if len(val) == 0 {
		return ""
	}
	return string(val)
}

func getKeyFromConfigMap(cm *corev1.ConfigMap, key string) string {
	return cm.Data[key]
}
