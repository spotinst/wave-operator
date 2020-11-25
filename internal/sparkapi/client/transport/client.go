package transport

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
)

type Client interface {
	Get(path string) ([]byte, error)
}

//region HTTP transport client

// TODO Remove?

type httpClient struct {
	client *http.Client
	host   string
	port   string
}

func NewHTTPClient(host string, port string) Client {
	c := &httpClient{
		host:   host,
		port:   port,
		client: &http.Client{},
	}
	return c
}

func (h httpClient) Get(path string) ([]byte, error) {

	url := fmt.Sprintf("http://%s:%s/%s", h.host, h.port, path)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("got http status %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

//endregion

//region Proxy client

type ProxyResource string

const (
	Pod     ProxyResource = "pods"
	Service ProxyResource = "services"
)

type proxyClient struct {
	resource  ProxyResource
	name      string
	namespace string
	port      string
	clientset kubernetes.Interface
}

func NewProxyClient(resource ProxyResource, name string, namespace string, port string, clientSet kubernetes.Interface) Client {
	c := &proxyClient{
		resource:  resource,
		name:      name,
		namespace: namespace,
		port:      port,
		clientset: clientSet,
	}
	return c
}

func (p proxyClient) Get(path string) ([]byte, error) {

	ctx := context.TODO()

	res := p.clientset.CoreV1().RESTClient().Get().
		Namespace(p.namespace).
		Resource(string(p.resource)).
		Name(fmt.Sprintf("%s:%s", p.name, p.port)).
		SubResource("proxy").
		Suffix(path).
		Do(ctx)

	body, err := res.Raw()
	if err != nil {
		statusError, ok := err.(*k8serrors.StatusError)
		if ok {
			return nil, decorateError(err, statusError)
		}
		return nil, err
	}

	return body, nil
}

func decorateError(err error, statusError *k8serrors.StatusError) error {
	status := statusError.Status()
	code := status.Code
	reason := status.Reason
	causeMessages := make([]string, 0)

	if status.Details != nil && status.Details.Causes != nil {
		for _, c := range status.Details.Causes {
			causeMessages = append(causeMessages, c.Message)
		}
	}

	return fmt.Errorf("code: %d, reason: %s, causes: %s, %w", code, reason, causeMessages, err)
}

//endregion
