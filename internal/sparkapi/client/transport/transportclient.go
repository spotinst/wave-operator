package transport

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type TransportClient interface {
	Get(path string) ([]byte, error)
}

//region HTTP transport client

type httpClient struct {
	client *http.Client
	host   string
	port   string
}

func NewHTTPClient(host string, port string) TransportClient {
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

//region Pod proxy client

type podProxyClient struct {
	pod       *corev1.Pod
	port      string
	clientset *kubernetes.Clientset
}

func NewPodProxyClient(pod *corev1.Pod, restConfig *rest.Config, port string) (TransportClient, error) {
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("could not create clientset, %w", err)
	}

	c := &podProxyClient{
		pod:       pod,
		port:      port,
		clientset: clientset,
	}

	return c, nil
}

func (p podProxyClient) Get(path string) ([]byte, error) {

	ctx := context.TODO()

	res := p.clientset.CoreV1().RESTClient().Get().
		Namespace(p.pod.Namespace).
		Resource("pods").
		Name(fmt.Sprintf("%s:%s", p.pod.Name, p.port)).
		SubResource("proxy").
		Suffix(path).
		Do(ctx)

	body, err := res.Raw()
	if err != nil {
		return nil, err
	}

	return body, nil
}

//endregion
