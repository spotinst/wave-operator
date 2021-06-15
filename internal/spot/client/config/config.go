package config

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"net/url"
	"os"

	"github.com/go-logr/logr"
	"k8s.io/client-go/kubernetes"

	"github.com/spotinst/wave-operator/internal/ocean"
)

const (
	envVarToken             = "SPOTINST_TOKEN"
	envVarTokenLegacy       = "SPOTINST_TOKEN_LEGACY"
	envVarAccount           = "SPOTINST_ACCOUNT"
	envVarAccountLegacy     = "SPOTINST_ACCOUNT_LEGACY"
	envVarBaseURL           = "SPOTINST_BASE_URL"
	envVarProxyURL          = "PROXY_URL"
	envVarClusterIdentifier = "CLUSTER_IDENTIFIER"

	defaultBaseURL = "https://api.spotinst.io"
	productName    = "wave-operator"
)

type Config struct {
	Creds                   Credentials
	BaseURL                 *url.URL
	ProxyConfig             *ProxyConfig
	ClusterIdentifier       string
	ClusterUniqueIdentifier string
}

type configValGetter struct {
	log            logr.Logger
	envVar         string
	fallbackEnvVar string
	fallback       func() string
	required       bool
	defaultValue   string
}

func (g *configValGetter) get() (string, error) {
	log := g.log.WithValues("configVal", g.envVar)

	val := os.Getenv(g.envVar)
	if val != "" {
		return val, nil
	}

	if g.fallbackEnvVar != "" {
		log.Info("Trying fallback env var", "fallbackEnvVar", g.fallbackEnvVar)
		val = os.Getenv(g.fallbackEnvVar)
		if val != "" {
			return val, nil
		}
	}

	if g.fallback != nil {
		log.Info("Trying fallback")
		val = g.fallback()
		if val != "" {
			return val, nil
		}
	}

	if g.defaultValue != "" {
		log.Info("Using default value")
		return g.defaultValue, nil
	}

	if g.required {
		return "", fmt.Errorf("could not get required config value")
	}

	return "", nil
}

// GetConfig populates Config from environment variables,
// falling back on querying k8s api for configuration values as needed
func GetConfig(kc kubernetes.Interface, log logr.Logger) (Config, error) {
	ctx := context.TODO()

	cm, err := ocean.GetOceanConfigMap(ctx, kc)
	if err != nil {
		return Config{}, fmt.Errorf("could not get ocean config map, %w", err)
	}

	secret, err := ocean.GetOceanSecret(ctx, kc)
	if err != nil {
		return Config{}, fmt.Errorf("could not get ocean secret, %w", err)
	}

	creds, err := getCredentials(cm, secret, log)
	if err != nil {
		return Config{}, fmt.Errorf("could not get credentials, %w", err)
	}

	baseURL, err := getBaseURL(cm, log)
	if err != nil {
		return Config{}, fmt.Errorf("could not get base url, %w", err)
	}

	proxyConfig, err := getProxyConfiguration(cm, log)
	if err != nil {
		return Config{}, fmt.Errorf("could not get proxy configuration, %w", err)
	}

	clusterIdentifier, err := GetClusterIdentifier(cm, log)
	if err != nil {
		return Config{}, fmt.Errorf("could not get cluster identifier, %w", err)
	}

	clusterUniqueIdentifier, err := ocean.GetClusterUniqueIdentifier(kc)
	if err != nil {
		return Config{}, fmt.Errorf("could not get cluster unique identifier, %w", err)
	}

	return Config{
		Creds:                   creds,
		BaseURL:                 baseURL,
		ProxyConfig:             proxyConfig,
		ClusterIdentifier:       clusterIdentifier,
		ClusterUniqueIdentifier: clusterUniqueIdentifier,
	}, nil
}

func GetClusterIdentifier(cm *corev1.ConfigMap, log logr.Logger) (string, error) {
	clusterIdentifierGetter := configValGetter{
		log:            log,
		envVar:         envVarClusterIdentifier,
		fallbackEnvVar: "",
		fallback: func() string {
			return ocean.GetClusterIdentifier(cm)
		},
		required:     true,
		defaultValue: "",
	}

	clusterIdentifier, err := clusterIdentifierGetter.get()
	if err != nil {
		return "", fmt.Errorf("could not get cluster identifier, %w", err)
	}

	return clusterIdentifier, nil
}
