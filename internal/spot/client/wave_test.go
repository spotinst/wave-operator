package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/internal/logger"
)

func ManualTestClient(t *testing.T) {

	c, err := NewClient(logger.New())
	require.NoError(t, err)

	t.Run("GetsApplication", func(t *testing.T) {
		app, err := c.GetSparkApplication(context.TODO(), "wsa-0439934b2ca5460e")
		require.NoError(t, err)
		fmt.Println(app)
	})

	t.Run("SavesApplication", func(t *testing.T) {
		app := &v1alpha1.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				ResourceVersion: "3",
				Name:            "Test-arnar-app",
			},
			Spec: v1alpha1.SparkApplicationSpec{
				ApplicationID:   "some-random-thing",
				ApplicationName: "Test-arnar-app",
				Heritage:        "spark-submit",
			},
			Status: v1alpha1.SparkApplicationStatus{
				Data: v1alpha1.SparkApplicationData{
					RunStatistics: v1alpha1.Statistics{
						Attempts: []v1alpha1.Attempt{
							{
								StartTimeEpoch:   time.Now().Unix(),
								AppSparkVersion:  "2.9.9",
								LastUpdatedEpoch: time.Now().Add(30 * time.Second).Unix(),
							},
						},
						TotalInputBytes: 10,
					},
					Driver: v1alpha1.Pod{
						Name:              "arnar-test-driver",
						Namespace:         "spark-test-jobs",
						CreationTimestamp: metav1.Now(),
					},
					Executors: []v1alpha1.Pod{
						{
							Name:              "arnar-test-executor-1",
							Namespace:         "spark-test-jobs",
							CreationTimestamp: metav1.Now(),
						},
					},
				},
			},
		}

		err := c.SaveApplication(app)
		require.NoError(t, err)
	})

}
