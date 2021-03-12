package components

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func getTestLogger() logr.Logger {
	return zap.New(zap.UseDevMode(true))
}
func TestParseAppversion(t *testing.T) {
	v1, v2, v3 := parseAppVersion("v1beta2-1.1.1-3.0.0")
	assert.Equal(t, "v1beta2", v1)
	assert.Equal(t, "1.1.1", v2)
	assert.Equal(t, "3.0.0", v3)
}

func TestGetSparkOperatorProperties(t *testing.T) {
	c := &v1alpha1.WaveComponent{
		Status: v1alpha1.WaveComponentStatus{
			Properties: map[string]string{
				"AppVersion": "v1beta2-1.1.1-3.0.0",
			},
		},
	}

	p, err := GetSparkOperatorProperties(c, nil, getTestLogger())
	assert.NoError(t, err)
	assert.Equal(t, "v1beta2", p["APIVersion"])
	assert.Equal(t, "1.1.1", p["OperatorVersion"])
	assert.Equal(t, "3.0.0", p["SparkVersion"])
	assert.Equal(t, "v1beta2-1.1.1-3.0.0", p["AppVersion"])
}

func TestGetSparkOperatorNoProperties(t *testing.T) {
	c := &v1alpha1.WaveComponent{}

	p, err := GetSparkOperatorProperties(c, nil, getTestLogger())
	assert.NoError(t, err)
	assert.Equal(t, "", p["APIVersion"])
	assert.Equal(t, "", p["OperatorVersion"])
	assert.Equal(t, "", p["SparkVersion"])
}
