package client

import (
	"fmt"

	"github.com/spotinst/wave-operator/internal/sparkapi/client/transport"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

const historyServerPort = "18080"

type historyServer struct {
	*client
}

func NewHistoryServerClient(service *corev1.Service, clientSet kubernetes.Interface) Client {
	//tc := transport.NewProxyClient(transport.Service, service.Name, service.Namespace, historyServerPort, clientSet)
	tc := transport.NewHTTPClientTransport(fmt.Sprintf("%s.%s", service.Name, service.Namespace), historyServerPort)
	c := &historyServer{
		client: &client{
			transportClient: tc,
		},
	}
	return c
}
