package install

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
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
