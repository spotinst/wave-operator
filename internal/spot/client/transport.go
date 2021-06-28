package client

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/spotinst/wave-operator/internal/spot/client/config"
)

type apiTransport struct {
	roundTripper http.RoundTripper
	baseURL      *url.URL
	credentials  config.Credentials
	userAgent    string
}

func (a *apiTransport) RoundTrip(req *http.Request) (*http.Response, error) {

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", a.credentials.Token))
	req.Header.Add("User-Agent", a.userAgent)

	query := req.URL.Query()
	query.Set(queryAccountId, a.credentials.Account)

	req.URL.RawQuery = query.Encode()

	// Set request base URL.
	req.URL.Host = a.baseURL.Host
	req.URL.Scheme = a.baseURL.Scheme

	// Set request headers.
	req.Host = a.baseURL.Host

	if !strings.HasPrefix(req.URL.Path, "/") {
		req.URL.Path = "/" + req.URL.Path
	}

	return a.roundTripper.RoundTrip(req)
}

func ApiTransport(roundTripper http.RoundTripper, baseURL *url.URL, creds config.Credentials) http.RoundTripper {
	if roundTripper == nil {
		roundTripper = http.DefaultTransport
	}
	ua := config.GetUserAgent()

	return &apiTransport{
		roundTripper: roundTripper,
		baseURL:      baseURL,
		credentials:  creds,
		userAgent:    ua,
	}
}
