package admission

import (
	"testing"

	"github.com/spotinst/wave-operator/internal/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// the MutatingWebhook ObjectSelector does the initial filtering on the pod and should select only those
// with the label SparkRoleLabel
func TestMutateDriverPod(t *testing.T) {
	driverPod := simplePod
	driverPod.Labels = map[string]string{
		SparkRoleLabel: SparkRoleDriverValue,
	}
	req := getAdmissionRequest(t, driverPod)
	r, err := NewPodMutator(log, &util.FakeStorageProvider{}).Mutate(req)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, driverPod.UID, r.UID)
	assert.Equal(t, &jsonPatchType, r.PatchType)
	assert.NotNil(t, r.Patch)
	assert.True(t, r.Allowed)

	obj, err := ApplyJsonPatch(r.Patch, driverPod)
	assert.NoError(t, err)
	newPod, ok := obj.(*(corev1.Pod))
	assert.True(t, ok)
	assert.Equal(t, 2, len(newPod.Spec.Containers))
	assert.Equal(t, "storage-sync", newPod.Spec.Containers[0].Name)
	assert.Equal(t, ondemandAffinity, newPod.Spec.Affinity)
	assert.Equal(t, 1, len(newPod.Spec.Volumes))
	assert.Equal(t, "spark-logs", newPod.Spec.Volumes[0].Name)
}

func TestMutateExecutorPod(t *testing.T) {
	execPod := simplePod
	execPod.Labels = map[string]string{
		SparkRoleLabel: SparkRoleExecutorValue,
	}
	req := getAdmissionRequest(t, execPod)
	r, err := NewPodMutator(log, &util.FakeStorageProvider{}).Mutate(req)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, execPod.UID, r.UID)
	assert.Equal(t, &jsonPatchType, r.PatchType)
	assert.NotNil(t, r.Patch)
	assert.True(t, r.Allowed)

	obj, err := ApplyJsonPatch(r.Patch, execPod)
	assert.NoError(t, err)
	newPod, ok := obj.(*(corev1.Pod))
	assert.True(t, ok)
	assert.Equal(t, 1, len(newPod.Spec.Containers))
	assert.Equal(t, ondemandAntiAffinity, newPod.Spec.Affinity)
	assert.Equal(t, 0, len(newPod.Spec.Volumes))
}

func TestIdempotency(t *testing.T) {
	driverPod := simplePod
	driverPod.Labels = map[string]string{
		SparkRoleLabel: SparkRoleDriverValue,
	}
	req := getAdmissionRequest(t, driverPod)
	m := NewPodMutator(log, &util.FakeStorageProvider{})
	r, err := m.Mutate(req)
	require.NoError(t, err)

	obj, err := ApplyJsonPatch(r.Patch, driverPod)
	require.NoError(t, err)
	newPod, ok := obj.(*corev1.Pod)
	require.True(t, ok)

	req = getAdmissionRequest(t, newPod)
	r, err = m.Mutate(req)
	assert.NoError(t, err)
	assert.Equal(t, "[]", string(r.Patch))

	obj2, err := ApplyJsonPatch(r.Patch, newPod)
	assert.NoError(t, err)
	pod2, ok := obj2.(*corev1.Pod)
	require.True(t, ok)

	assert.Equal(t, 2, len(pod2.Spec.Containers))
	assert.Equal(t, ondemandAffinity, pod2.Spec.Affinity)
	assert.Equal(t, 1, len(newPod.Spec.Volumes))
	assert.Equal(t, "spark-logs", newPod.Spec.Volumes[0].Name)
}

func TestSkipNonSparkPod(t *testing.T) {
	req := getAdmissionRequest(t, simplePod)
	r, err := NewPodMutator(log, &util.FailedStorageProvider{}).Mutate(req)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, simplePod.UID, r.UID)
	assert.Nil(t, r.PatchType)
	assert.Nil(t, r.Patch)
	assert.True(t, r.Allowed)
}

func TestMutatePodBadStorage(t *testing.T) {
	driverPod := simplePod
	driverPod.Labels = map[string]string{
		SparkRoleLabel: SparkRoleDriverValue,
	}
	req := getAdmissionRequest(t, driverPod)
	r, err := NewPodMutator(log, &util.FailedStorageProvider{}).Mutate(req)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, driverPod.UID, r.UID)
	assert.Nil(t, r.PatchType)
	assert.Nil(t, r.Patch)
	assert.True(t, r.Allowed)

	req = getAdmissionRequest(t, driverPod)
	r, err = NewPodMutator(log, &util.NilStorageProvider{}).Mutate(req)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, driverPod.UID, r.UID)
	assert.Nil(t, r.PatchType)
	assert.Nil(t, r.Patch)
	assert.True(t, r.Allowed)
}
