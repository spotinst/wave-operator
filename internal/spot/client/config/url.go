package config

import (
	"fmt"
	"net/url"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"

	"github.com/spotinst/wave-operator/internal/ocean"
)

type ProxyConfig struct {
	HTTPProxy  *url.URL
	HTTPSProxy *url.URL
}

func getBaseURL(cm *corev1.ConfigMap, log logr.Logger) (*url.URL, error) {
	urlGetter := configValGetter{
		log:            log,
		envVar:         envVarBaseURL,
		fallbackEnvVar: "",
		fallback: func() string {
			return ocean.GetBaseURL(cm)
		},
		required:     true,
		defaultValue: defaultBaseURL,
	}

	rawURL, err := urlGetter.get()
	if err != nil {
		return nil, fmt.Errorf("could not get raw url, %w", err)
	}

	return parseUrl(rawURL)
}

func getProxyConfiguration(cm *corev1.ConfigMap, log logr.Logger) (*ProxyConfig, error) {
	urlGetter := configValGetter{
		log:            log,
		envVar:         envVarProxyURL,
		fallbackEnvVar: "",
		fallback: func() string {
			return ocean.GetProxyURL(cm)
		},
		required:     false,
		defaultValue: "",
	}

	rawURL, err := urlGetter.get()
	if err != nil {
		return nil, fmt.Errorf("could not get raw url, %w", err)
	}

	if rawURL == "" {
		return nil, nil
	}

	parsed, err := parseUrl(rawURL)
	if err != nil {
		return nil, fmt.Errorf("could not parse url, %w", err)
	}

	// TODO(thorsteinn) validate
	switch parsed.Scheme {
	case "https":
		return &ProxyConfig{
			HTTPSProxy: parsed,
		}, nil
	case "http":
		return &ProxyConfig{
			HTTPProxy: parsed,
		}, nil
	default:
		return nil, fmt.Errorf("unknown scheme %q", parsed.Scheme)
	}
}

func parseUrl(rawURL string) (*url.URL, error) {
	baseURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("malformed URL %q", rawURL)
	}
	return baseURL, nil
}
