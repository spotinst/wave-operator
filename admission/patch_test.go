package admission

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kubejsonpatch "github.com/evanphx/json-patch/v5"
)

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
