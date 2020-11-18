package sparkapi

import (
	"fmt"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"testing"
)

func TestGetApplication(t *testing.T) {

	client := NewClient("http://localhost:9999", getTestLogger())
	res, err := client.GetApplication("spark-07be00fc282f45bf8f46bc7a32c82360")
	assert.NoError(t, err)

	fmt.Println(res)
}

func TestGetEnvironment(t *testing.T) {

	client := NewClient("http://localhost:9999", getTestLogger())
	res, err := client.GetEnvironment("spark-07be00fc282f45bf8f46bc7a32c82360")
	assert.NoError(t, err)

	fmt.Println(res)
}

func getTestLogger() logr.Logger {
	return zap.New(zap.UseDevMode(true))
}
