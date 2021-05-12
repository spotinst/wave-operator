package transport

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
)

type httpClientTransport struct {
	name      string
	namespace string
	port      string
}

func NewHTTPClientTransport(name string, namespace string, port string) Client {
	return &httpClientTransport{
		name:      name,
		namespace: namespace,
		port:      port,
	}
}

func (h httpClientTransport) Get(path string) ([]byte, error) {
	pathURL, err := url.Parse(fmt.Sprintf("http://%s.%s:%s/%s", h.name, h.namespace, h.port, path))
	if err != nil {
		return nil, err
	}

	resp, err := http.Get(pathURL.String())
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(resp.Body)
}
