package config

import (
	"fmt"
	"net/url"
	"os"
)

const (
	defaultBaseURL = "https://api.spotinst.io"
	envVarBaseURL  = "SPOTINST_BASE_URL"
)

func GetBaseURL() (*url.URL, error) {
	rawURL := os.Getenv(envVarBaseURL)
	if rawURL == "" {
		rawURL = defaultBaseURL
	}
	baseURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("malformed URL %q", rawURL)
	}
	return baseURL, nil
}
