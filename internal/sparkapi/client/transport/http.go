package transport

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

type HttpClientTransport struct {
	client *http.Client
	host   string
	port   string
}

type HttpClientTransportOpt func(t *HttpClientTransport)

func WithTimeout(duration time.Duration) HttpClientTransportOpt {
	return func(t *HttpClientTransport) {
		t.client.Timeout = duration
	}
}

func WithTransport(transport http.RoundTripper) HttpClientTransportOpt {
	return func(t *HttpClientTransport) {
		t.client.Transport = transport
	}
}

func NewHTTPClientTransport(host string, port string, opts ...HttpClientTransportOpt) *HttpClientTransport {
	const defaultTimeout = 15 * time.Second

	c := &HttpClientTransport{
		client: &http.Client{
			Timeout: defaultTimeout,
		},
		port: port,
		host: host,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (h HttpClientTransport) Get(path string) ([]byte, error) {
	pathURL, err := url.Parse(fmt.Sprintf("http://%s/%s", net.JoinHostPort(h.host, h.port), path))
	if err != nil {
		return nil, err
	}

	resp, err := h.client.Get(pathURL.String())
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, ServiceUnavailableError{err}
		}

		var opErr *net.OpError
		if errors.As(err, &opErr) {
			return nil, ServiceUnavailableError{err}
		}

		var urlErr *url.Error
		if errors.As(err, &urlErr) && urlErr.Timeout() {
			return nil, ServiceUnavailableError{err}
		}

		return nil, err
	}

	if resp.StatusCode == 404 {
		return nil, NotFoundError{fmt.Errorf("%s", pathURL)}
	}

	return io.ReadAll(resp.Body)
}
