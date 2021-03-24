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

	wrappedErr := fmt.Errorf("code: %d, reason: %s, causes: %s, %w", code, reason, causeMessages, err)

	if k8serrors.IsNotFound(statusError) {
		wrappedErr = newNotFoundError(wrappedErr)
	} else if k8serrors.IsServiceUnavailable(statusError) {
		wrappedErr = newServiceUnavailableError(wrappedErr)
	}

	return wrappedErr
}

// NotFoundError indicates that the requested resource was not found
type NotFoundError struct {
	err error
}

func newNotFoundError(err error) NotFoundError {
	return NotFoundError{err: err}
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("not found error: %s", e.err.Error())
}

func (e NotFoundError) Unwrap() error {
	return e.err
}

// ServiceUnavailableError indicates that the Spark API is unavailable
type ServiceUnavailableError struct {
	err error
}

func newServiceUnavailableError(err error) ServiceUnavailableError {
	return ServiceUnavailableError{err: err}
}

func (e ServiceUnavailableError) Error() string {
	return fmt.Sprintf("service unavailable error: %s", e.err.Error())
}

func (e ServiceUnavailableError) Unwrap() error {
	return e.err
}
