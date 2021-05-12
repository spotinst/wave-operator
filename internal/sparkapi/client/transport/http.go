package transport

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
)

type httpClientTransport struct {
	ip   string
	port string
}

func NewHTTPClientTransport(ip string, port string) Client {
	return &httpClientTransport{
		port: port,
		ip:   ip,
	}
}

func (h httpClientTransport) Get(path string) ([]byte, error) {
	pathURL, err := url.Parse(fmt.Sprintf("http://%s/%s", net.JoinHostPort(h.ip, h.port), path))
	if err != nil {
		return nil, err
	}

	resp, err := http.Get(pathURL.String())
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(resp.Body)
}
