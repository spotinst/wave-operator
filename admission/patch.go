package admission

import (
	"encoding/json"

	"github.com/mattbaird/jsonpatch"
	"github.com/spotinst/wave-operator/api/v1alpha1"
	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
)

var (
	runtimeScheme = runtime.NewScheme()
	codecs        = serializer.NewCodecFactory(runtimeScheme)
	deserializer  = codecs.UniversalDeserializer()
	jsonPatchType = admissionv1.PatchTypeJSONPatch
)

func init() {
	_ = clientgoscheme.AddToScheme(runtimeScheme)
	_ = v1alpha1.AddToScheme(runtimeScheme)
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
