package install

import (
	"testing"

	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/release"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func TestYamlConversion(t *testing.T) {

	values := `
serviceAccount:
  create: true
`
	var vals map[string]interface{}
	err := yaml.Unmarshal([]byte(values), &vals)
	assert.NoError(t, err)

	for k, v := range vals {
		t.Log("values", k, v)
	}

	e := vals["serviceAccount"]
	s, ok := e.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, true, s["create"])
}

func getVersionedObjects(componentVersion, releasedVersion string) (*v1alpha1.WaveComponent, *release.Release) {
	return &v1alpha1.WaveComponent{
			ObjectMeta: metav1.ObjectMeta{
				Name: "wave-foo",
			},
			Spec: v1alpha1.WaveComponentSpec{
				Name:                "foo",
				Version:             componentVersion,
				ValuesConfiguration: "",
			},
		},
		&release.Release{
			Name: "foo",
			Chart: &chart.Chart{
				Metadata: &chart.Metadata{
					Name:       "foo",
					Version:    releasedVersion,
					AppVersion: "v99.12345.0",
				},
			},
			Config: map[string]interface{}{},
		}
}

func getValuesObjects(componentValues string, releasedValues map[string]interface{}) (*v1alpha1.WaveComponent, *release.Release) {
	return &v1alpha1.WaveComponent{
			ObjectMeta: metav1.ObjectMeta{
				Name: "wave-foo",
			},
			Spec: v1alpha1.WaveComponentSpec{
				Name:                "foo",
				Version:             "v1.2",
				ValuesConfiguration: componentValues,
			},
		},
		&release.Release{
			Name: "foo",
			Chart: &chart.Chart{
				Metadata: &chart.Metadata{
					Name:       "foo",
					Version:    "v1.2",
					AppVersion: "v99.12345.0",
				},
			},
			Config: releasedValues,
		}
}

func TestIsUpgrade(t *testing.T) {

	logger := zap.New(zap.UseDevMode(true)).WithValues("test", t.Name())
	i := &HelmInstaller{nil, logger} // fix ClientGetter for more complex tests
	var u bool

	u = i.IsUpgrade(getVersionedObjects("v1.1.0", "v0.9.8"))
	assert.True(t, u)

	u = i.IsUpgrade(getVersionedObjects("v1.1.0", "v1.1.0"))
	assert.False(t, u)

	u = i.IsUpgrade(getValuesObjects("metricsEnabled: true", map[string]interface{}{}))
	assert.True(t, u)

	u = i.IsUpgrade(getValuesObjects("", map[string]interface{}{}))
	assert.False(t, u)

	u = i.IsUpgrade(getValuesObjects(":unparseable yaml is an upgrade lol:", map[string]interface{}{}))
	assert.True(t, u)

	v1 := `
serviceAccount:
  create: true
`
	v2 := map[string]interface{}{
		"serviceAccount": map[string]interface{}{
			"create": true,
		},
	}
	u = i.IsUpgrade(getValuesObjects(v1, v2))
	assert.False(t, u)

}
