package admission

import (
	"testing"

	"github.com/spotinst/wave-operator/internal/util"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	simplePod = &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"kubernetes.io/psp": "eks.privileged",
			},
			Name:      "curl",
			Namespace: "default",
			UID:       "c5d82a65-9422-4556-b9f3-ea14c5ad80eb",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Command: []string{
						"tail",
						"-f",
						"/dev/null",
					},
					Image: "appropriate/curl",
					Name:  "curl",
				},
			},
		},
	}
)

// PLEASE NOTE all of the filtering on pods is done in the MutatingWebhook ObjectSelector
// before the AdmissionRequest is created.
// once the AdmissionRequest is submitted, the changes will be attempted
func TestMutateSimplePod(t *testing.T) {
	req := getAdmissionRequest(t, simplePod)
	r, err := MutatePod(&util.FakeStorageProvider{}, log, req)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, simplePod.UID, r.UID)
	assert.Equal(t, &jsonPatchType, r.PatchType)
	assert.NotNil(t, r.Patch)
	assert.True(t, r.Allowed)

	obj, err := ApplyJsonPatch(r.Patch, simplePod)
	assert.NoError(t, err)
	newPod, ok := obj.(*(corev1.Pod))
	assert.True(t, ok)
	assert.Equal(t, 2, len(newPod.Spec.Containers))
	assert.Equal(t, ondemandAffinity, newPod.Spec.Affinity)
}

func TestMutatePodBadStorage(t *testing.T) {
	req := getAdmissionRequest(t, simplePod)
	r, err := MutatePod(&util.FailedStorageProvider{}, log, req)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, simplePod.UID, r.UID)
	assert.Nil(t, r.PatchType)
	assert.Nil(t, r.Patch)
	assert.True(t, r.Allowed)

	req = getAdmissionRequest(t, simplePod)
	r, err = MutatePod(&util.NilStorageProvider{}, log, req)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, simplePod.UID, r.UID)
	assert.Nil(t, r.PatchType)
	assert.Nil(t, r.Patch)
	assert.True(t, r.Allowed)
}