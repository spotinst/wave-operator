package transport

import (
	"fmt"
	"io/ioutil"
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

func NewHTTPClientTransport(host string, port string, opts ...HttpClientTransportOpt) *HttpClientTransport {
	const defaultTimeout = 5 * time.Second

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
		return nil, err
	}

	return ioutil.ReadAll(resp.Body)
}
