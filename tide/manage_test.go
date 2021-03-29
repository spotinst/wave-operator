package tide

import (
	"regexp"
	"testing"

	"github.com/go-logr/logr"
	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func getTestLogger() logr.Logger {
	return zap.New(zap.UseDevMode(true))
}

func TestSetImageInValues(t *testing.T) {

	// empty values

	image := "public.ecr.aws/l8m2k1n1/netapp/wave-operator:0.2.0-33a41c7f"
	valuesString := ""

	tagRE := regexp.MustCompile("(?m)^[[:space:]]*tag: 0.2.0-33a41c7f$")
	policyRE := regexp.MustCompile("(?m)^[[:space:]]*pullPolicy: IfNotPresent$")
	repoRE := regexp.MustCompile("(?m)^[[:space:]]*repository: public.ecr.aws/l8m2k1n1/netapp/wave-operator$")

	newVals, err := setImageInValues(valuesString, image)
	assert.NoError(t, err)
	assert.True(t, tagRE.Match([]byte(newVals)), newVals)
	assert.True(t, policyRE.Match([]byte(newVals)), newVals)
	assert.True(t, repoRE.Match([]byte(newVals)), newVals)

	// some values

	valuesString = `
imagePullSecrets: []
nameOverride: ""
fullnameOverride: ""

serviceAccount:
  create: true
  annotations: {}
  name: wave-operator`

	newVals, err = setImageInValues(valuesString, image)
	assert.NoError(t, err)
	assert.True(t, tagRE.Match([]byte(newVals)), newVals)
	assert.True(t, policyRE.Match([]byte(newVals)), newVals)
	assert.True(t, repoRE.Match([]byte(newVals)), newVals)

	// override image

	valuesString = `
replicaCount: 1

image:
  repository: some.other.repo/wave-op
  pullPolicy: Always
  tag: "0.0.1-alpha"`

	newVals, err = setImageInValues(valuesString, image)
	assert.NoError(t, err)
	assert.True(t, tagRE.Match([]byte(newVals)), newVals)
	assert.True(t, policyRE.Match([]byte(newVals)), newVals)
	assert.True(t, repoRE.Match([]byte(newVals)), newVals)

	// latest image
	image = "public.ecr.aws/l8m2k1n1/netapp/wave-operator"

	tagRE = regexp.MustCompile("(?m)^[[:space:]]*tag: latest$")
	newVals, err = setImageInValues(valuesString, image)
	assert.NoError(t, err)
	assert.True(t, tagRE.Match([]byte(newVals)), newVals)
	assert.True(t, policyRE.Match([]byte(newVals)), newVals)
	assert.True(t, repoRE.Match([]byte(newVals)), newVals)

	// bad image
	image = "public.ecr.aws/l8m2k1n1/netapp/wave-operator:this:is:not:ok"
	_, err = setImageInValues(valuesString, image)
	assert.Error(t, err, image)

	image = ":"
	_, err = setImageInValues(valuesString, image)
	assert.Error(t, err, image)
}

func TestLoadCrd(t *testing.T) {
	logger := getTestLogger()
	iface, err := NewManager(logger)
	require.NoError(t, err)
	m, ok := iface.(*manager)
	require.True(t, ok)
	w, err := m.loadCRD("wave.spot.io_wavecomponents.yaml")
	assert.NoError(t, err)
	assert.Equal(t, "wavecomponents.wave.spot.io", w.Name)
}

func TestLoadComponents(t *testing.T) {
	logger := getTestLogger()
	iface, err := NewManager(logger)
	require.NoError(t, err)
	m, ok := iface.(*manager)
	require.True(t, ok)
	ww, err := m.loadWaveComponents()
	assert.NoError(t, err)
	assert.Equal(t, 4, len(ww))
	for _, wc := range ww {
		assert.Equal(t, v1alpha1.PresentComponentState, wc.Spec.State)
	}
}

func TestDisableComponents(t *testing.T) {
	logger := getTestLogger()
	iface, err := NewManager(logger)
	require.NoError(t, err)
	m, ok := iface.(*manager)
	require.True(t, ok)

	m.spec.Enabled[v1alpha1.SparkHistoryChartName] = false

	ww, err := m.loadWaveComponents()
	assert.NoError(t, err)
	assert.Equal(t, 4, len(ww))
	for _, wc := range ww {
		if wc.Spec.Name == v1alpha1.SparkHistoryChartName {
			assert.Equal(t, v1alpha1.AbsentComponentState, wc.Spec.State)
		} else {
			assert.Equal(t, v1alpha1.PresentComponentState, wc.Spec.State)
		}
	}
}
