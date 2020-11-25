package admission

import (
	"encoding/json"
	"testing"
	"time"

	kubejsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/spotinst/wave-operator/cloudstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	log         = zap.New(zap.UseDevMode(true))
	storageInfo = &cloudstorage.StorageInfo{
		Name:    "test",
		Region:  "utah",
		Path:    "s3://test/",
		Created: time.Now(),
	}
	T            = true
	jsonPathType = admissionv1.PatchTypeJSONPatch

	//mwp = admissionregistrationv1.MutatingWebhook{}
)

type Mutatable interface {
	runtime.Object
	metav1.Object
}

func getAdmissionRequest(t *testing.T, obj Mutatable) *admissionv1.AdmissionRequest {
	// ? check if the webhook labelselector would admit it?
	raw, err := json.Marshal(obj)
	require.NoError(t, err)
	return &admissionv1.AdmissionRequest{
		UID:    obj.GetUID(),
		Object: runtime.RawExtension{Raw: raw},
	}
}

func ApplyJsonPatch(patchBytes []byte, sourceObj runtime.Object) (runtime.Object, error) {
	orig, err := json.Marshal(sourceObj)
	if err != nil {
		return nil, err
	}
	patch, err := kubejsonpatch.DecodePatch(patchBytes)
	if err != nil {
		return nil, err
	}
	modified, err := patch.Apply(orig)
	if err != nil {
		return nil, err
	}
	gvk := sourceObj.GetObjectKind().GroupVersionKind()
	obj, _, err := deserializer.Decode(modified, &gvk, nil)
	return obj, err
}

// TestCreatePatch reassures that a patch created with "github.com/mattbaird/jsonpatch" can be
// deserialized with "github.com/evanphx/json-patch/v5"
func TestCreatePatch(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "test",
					Image: "alpine",
				},
			},
		},
	}
	newPod := pod.DeepCopy()
	newPod.ObjectMeta.Labels = map[string]string{
		"function": "TestCreatePatch",
	}

	patch, err := GetJsonPatch(pod, newPod)
	assert.NoError(t, err)

	t.Log(string(patch))

	// try decoding in the way the kube api server does
	_, err = kubejsonpatch.DecodePatch(patch)
	assert.NoError(t, err)

}
