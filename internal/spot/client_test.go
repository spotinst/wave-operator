package spot_test

import (
	"fmt"
	"testing"

	"github.com/spotinst/spotinst-sdk-go/spotinst"
	spotlog "github.com/spotinst/spotinst-sdk-go/spotinst/log"
	"github.com/spotinst/wave-operator/internal/logger"
	"github.com/spotinst/wave-operator/internal/spot"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	log := logger.New()
	conf := spotinst.DefaultConfig().WithLogger(spotlog.LoggerFunc(func(format string, args ...interface{}) {
		log.Info(fmt.Sprintf(format, args...))
	}))

	c := spot.NewClient(conf, "arnar-test-ekctl")

	app, err := c.GetSparkApplication("wsa-0439934b2ca5460e")
	require.NoError(t, err)
	fmt.Println(app)
}
