package transport

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
)

type httpClientTransport struct {
	host string
	port string
}

func NewHTTPClientTransport(host string, port string) Client {
	return &httpClientTransport{
		port: port,
		host: host,
	}
}

func (h httpClientTransport) Get(path string) ([]byte, error) {
	pathURL, err := url.Parse(fmt.Sprintf("http://%s/%s", net.JoinHostPort(h.host, h.port), path))
	if err != nil {
		return nil, err
	}

	resp, err := http.Get(pathURL.String())
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(resp.Body)
}
