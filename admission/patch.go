package admission

import (
	"encoding/json"

	"github.com/mattbaird/jsonpatch"

	//	jsonpatch "github.com/evanphx/json-patch/v5"
	"k8s.io/apimachinery/pkg/runtime"
)

type patchOperation struct {
	Op   string `json:"op"`
	Path string `json:"path"`
	//Value interface{} `json:"value,omitempty"`
	Value string `json:"value,omitempty"`
}

func GetJsonPatch(original, modified runtime.Object) ([]byte, error) {
	orig, err := json.Marshal(original)
	if err != nil {
		return nil, err
	}
	mod, err := json.Marshal(modified)
	if err != nil {
		return nil, err
	}
	op, err := jsonpatch.CreatePatch(orig, mod)
	if err != nil {
		return nil, err
	}
	return json.Marshal(op)
}
