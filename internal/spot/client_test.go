package spot_test

import (
	"fmt"
	"net/http/httputil"
	"testing"

	"github.com/spotinst/wave-operator/internal/spot"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	c := spot.NewClient(spot.Credentials{
		AccountId: "ble", Token: "shuff",
	}, "clusterBle")

	resp, err := c.Get("https://httpbin.org/get")
	require.NoError(t, err)
	body, err := httputil.DumpResponse(resp, true)
	require.NoError(t, err)
	fmt.Println(string(body))
}
