package transport

import (
	"context"
	"fmt"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
)

type Client interface {
	Get(path string) ([]byte, error)
}

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
