package transport

import (
	"fmt"
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

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "guest-9eef4e53-cfdc-45d5-8941-8e3d907e2eff-1605771311132-driver",
			Namespace: "guest-9eef4e53-cfdc-45d5-8941-8e3d907e2eff",
		},
	}

	client, err := NewPodProxyClient(pod, config, "4040")
	assert.NoError(t, err)
	res, err := client.Get("api/v1/applications")
	assert.NoError(t, err)
	fmt.Println(string(res))

}
