package client

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"testing"
)

const sparkApplicationId = "spark-838d85fbbdde435b9456f2d8af6e6ec4"

func TestGetApplication_historyServerClient(t *testing.T) {
	client := getHistoryServerClient()
	res, err := client.GetApplication(sparkApplicationId)
	assert.NoError(t, err)

	fmt.Println(res)
}

func TestGetApplication_driverPodClient(t *testing.T) {
	client, err := getDriverPodClient()
	assert.NoError(t, err)
	res, err := client.GetApplication(sparkApplicationId)
	assert.NoError(t, err)

	fmt.Println(res)
}

func TestGetStages_historyServerClient(t *testing.T) {
	client := getHistoryServerClient()
	res, err := client.GetStages(sparkApplicationId)
	assert.NoError(t, err)

	fmt.Println(res)
}

func TestGetStages_driverPodClient(t *testing.T) {
	client, err := getDriverPodClient()
	assert.NoError(t, err)
	res, err := client.GetStages(sparkApplicationId)
	assert.NoError(t, err)

	fmt.Println(res)
}

func TestGetEnvironment_historyServerClient(t *testing.T) {
	client := getHistoryServerClient()
	res, err := client.GetEnvironment(sparkApplicationId)
	assert.NoError(t, err)

	fmt.Println(res)
}

func TestGetEnvironment_driverPodClient(t *testing.T) {
	client, err := getDriverPodClient()
	assert.NoError(t, err)
	res, err := client.GetEnvironment(sparkApplicationId)
	assert.NoError(t, err)

	fmt.Println(res)
}

func getHistoryServerClient() Client {
	return NewHistoryServerClient("localhost")
}

func getDriverPodClient() (Client, error) {
	config := ctrl.GetConfigOrDie()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "guest-9eef4e53-cfdc-45d5-8941-8e3d907e2eff-1605771311132-driver",
			Namespace: "guest-9eef4e53-cfdc-45d5-8941-8e3d907e2eff",
		},
	}

	return NewDriverPodClient(pod, config)
}
