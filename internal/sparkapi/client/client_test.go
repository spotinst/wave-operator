package client

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
)

const sparkApplicationId = "spark-1c795f6bd15a4c019a87cb8aac96a8b9"

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
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "guest-83a4e90d-ba45-4d5d-a6d9-4e59d27d7118-1605786444879-driver",
			Namespace: "guest-83a4e90d-ba45-4d5d-a6d9-4e59d27d7118",
		},
	}

	return NewDriverPodClient(pod, clientSet), nil
}
