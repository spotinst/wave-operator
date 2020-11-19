package transport

import (
	"fmt"
	"k8s.io/client-go/kubernetes"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

func Test_httpClient_Get(t *testing.T) {

	client := NewHTTPClient("localhost", "18080")
	res, err := client.Get("api/v1/applications")
	assert.NoError(t, err)
	fmt.Println(string(res))

}

func Test_podProxyClient_Get(t *testing.T) {

	config := ctrl.GetConfigOrDie()
	clientset, err := kubernetes.NewForConfig(config)
	assert.NoError(t, err)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "guest-83a4e90d-ba45-4d5d-a6d9-4e59d27d7118-1605786444879-driver",
			Namespace: "guest-83a4e90d-ba45-4d5d-a6d9-4e59d27d7118",
		},
	}

	client := NewPodProxyClient(pod, clientset, "4040")
	assert.NoError(t, err)
	res, err := client.Get("api/v1/applications")
	assert.NoError(t, err)
	fmt.Println(string(res))

}
