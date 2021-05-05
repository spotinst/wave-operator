package spot

import (
	"fmt"
	"net/http"

	"github.com/spotinst/spotinst-sdk-go/spotinst"
	"github.com/spotinst/spotinst-sdk-go/spotinst/credentials"
)

type authTransport struct {
	base  http.RoundTripper
	creds *credentials.Credentials
}

func (a *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	creds, err := a.creds.Get()
	if err != nil {
		return nil, err
	}

	query := req.URL.Query()
	query.Set(queryAccountId, creds.Account)
	req.URL.RawQuery = query.Encode()

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", creds.Token))

	return a.base.RoundTrip(req)
}

func AuthTransport(base http.RoundTripper, config *spotinst.Config) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}

	return &authTransport{
		base:  base,
		creds: config.Credentials,
	}
}
