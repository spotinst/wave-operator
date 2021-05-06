package spot_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/spotinst/spotinst-sdk-go/spotinst"
	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/internal/logger"
	"github.com/spotinst/wave-operator/internal/spot"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestClient(t *testing.T) {
	conf := spotinst.DefaultConfig()

	c := spot.NewClient(conf, "arnar-test-ekctl", logger.New())

	t.Run("GetsApplication", func(t *testing.T) {
		app, err := c.GetSparkApplication(context.TODO(), "wsa-0439934b2ca5460e")
		require.NoError(t, err)
		fmt.Println(app)
	})

	t.Run("SavesApplication", func(t *testing.T) {
		app := &v1alpha1.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name: "Test-arnar-app",
			},
			Spec:       v1alpha1.SparkApplicationSpec{
				ApplicationID: "some-random-thing",
				ApplicationName: "Test-arnar-app",
			},
			Status:     v1alpha1.SparkApplicationStatus{
				Data: v1alpha1.SparkApplicationData{
					RunStatistics:   v1alpha1.Statistics{},
					Driver:          v1alpha1.Pod{},
					Executors:       nil,
				},
			},
		}

		err := c.SaveApplication(app)
		require.NoError(t, err)
	})

}
