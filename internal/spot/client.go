package spot

import (
	"fmt"
	"net/http"

	"github.com/spotinst/wave-operator/internal/version"
)

const (
	queryAccountId         = "accountId"
	queryClusterIdentifier = "clusterIdentifier"
)

type Credentials struct {
	Token     string
	AccountId string
}

func NewClient(c Credentials, cluster string) *http.Client {
	return &http.Client{
		Transport: AuthTransport(nil, c, cluster),
	}
}

type spotTransport struct {
	base        http.RoundTripper
	credentials Credentials
	cluster     string
	userAgent   string
}

func (s *spotTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", s.credentials.Token))
	request.Header.Set("User-Agent", s.userAgent)
	query := request.URL.Query()
	query.Set(queryAccountId, s.credentials.AccountId)
	query.Set(queryClusterIdentifier, s.cluster)
	request.URL.RawQuery = query.Encode()
	return s.base.RoundTrip(request)
}

func AuthTransport(base http.RoundTripper, c Credentials, cluster string) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}

	return &spotTransport{
		base:        base,
		credentials: c,
		cluster:     cluster,
		userAgent: fmt.Sprintf("wave-operator/%s", version.BuildVersion),
	}
}
