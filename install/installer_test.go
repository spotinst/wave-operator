package install

import (
	"encoding/json"
	"reflect"
	"strconv"
	"testing"

	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
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

func getVersionedObjects(componentVersion, releasedVersion string) (*v1alpha1.WaveComponent, *Installation) {
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
		NewInstallation("foo", releasedVersion, "", "", nil)
}

func getValuesObjects(componentValues string, releasedValues map[string]interface{}) (*v1alpha1.WaveComponent, *Installation) {
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
		NewInstallation("foo", "v1.2", "", "", releasedValues)
}

func TestIsUpgrade(t *testing.T) {

	logger := zap.New(zap.UseDevMode(true)).WithValues("test", t.Name())
	i := &HelmInstaller{
		prefix:       "wave",
		ClientGetter: nil,
		Log:          logger,
	} // fix getClient for more complex tests
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

func TestMarshal(t *testing.T) {
	is := InstallSpec{
		Name:       "wave-operator",
		Repository: "https://charts.spot.io",
		Version:    "0.2.1",
		Values: `
image:
  repository: public.ecr.aws/l8m2k1n1/netapp/wave-operator
  tag: "0.2.1-1d11e752"
`,
	}

	spec, err := json.Marshal(is)
	assert.NoError(t, err)
	assert.NotEmpty(t, spec)
}

func TestUnmarshal(t *testing.T) {

	text := "{\"name\":\"wave-operator\",\"repository\":\"https://charts.spot.io\",\"version\":\"0.2.0\"}"
	is := &InstallSpec{}

	err := json.Unmarshal([]byte(text), is)
	assert.NoError(t, err)
	assert.NotNil(t, is)
	assert.Equal(t, "wave-operator", is.Name)
	assert.Equal(t, "https://charts.spot.io", is.Repository)
	assert.Equal(t, "0.2.0", is.Version)
	assert.Empty(t, is.Values)
}

func TestUnmarshalJsonValues(t *testing.T) {

	// "values" can be a json string & it will unmarshal as yaml, so long as its quoted properly
	values := "{\"image\":{\"repository\": \"public.ecr.aws/l8m2k1n1/netapp/wave-operator\",\"tag\": \"0.2.1-1d11e752\"}}"
	text := "{\"name\":\"wave-operator\",\"repository\":\"https://charts.spot.io\",\"version\":\"0.2.0\",\"values\": " + strconv.Quote(values) + "}"
	is := &InstallSpec{}

	err := json.Unmarshal([]byte(text), is)
	assert.NoError(t, err)
	assert.NotNil(t, is)
	assert.Equal(t, "wave-operator", is.Name)
	assert.Equal(t, "https://charts.spot.io", is.Repository)
	assert.Equal(t, "0.2.0", is.Version)
	assert.NotEmpty(t, is.Values)

	var vals map[string]interface{}
	err = yaml.Unmarshal([]byte(is.Values), &vals)
	assert.NoError(t, err)

	expected := map[string]string{
		"repository": "public.ecr.aws/l8m2k1n1/netapp/wave-operator",
		"tag":        "0.2.1-1d11e752",
	}
	// assert.True(t, reflect.DeepEqual(expected, vals["image"]),
	// 	fmt.Sprintf("      got %v\n expected %v", vals["image"], expected))
	vals2, ok := vals["image"].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "", reflect.TypeOf(vals["image"]).Name())
	assert.Equal(t, expected["repository"], vals2["repository"])
	assert.Equal(t, expected["tag"], vals2["tag"])

}
