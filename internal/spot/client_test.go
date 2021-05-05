package spot_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/spotinst/spotinst-sdk-go/spotinst"
	"github.com/spotinst/wave-operator/internal/logger"
	"github.com/spotinst/wave-operator/internal/spot"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	conf := spotinst.DefaultConfig()

	c := spot.NewClient(conf, "arnar-test-ekctl", logger.New())

	app, err := c.GetSparkApplication(context.TODO(), "wsa-0439934b2ca5460e")
	require.NoError(t, err)
	fmt.Println(app)
}
