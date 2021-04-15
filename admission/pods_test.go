package admission

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/spotinst/wave-operator/cloudstorage"
	"github.com/spotinst/wave-operator/controllers"
	"github.com/spotinst/wave-operator/internal/util"
)

func getSimplePod() *corev1.Pod {
	return &corev1.Pod{
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
}

// the MutatingWebhook ObjectSelector does the initial filtering on the pod and should select only those
// with the label SparkRoleLabel
func TestMutateDriverPod(t *testing.T) {

	type testCase struct {
		eventLogSyncAnnotationPresent bool
		eventLogSyncAnnotationValue   string
		shouldAddEventLogSync         bool
	}

	testCases := []testCase{
		{
			eventLogSyncAnnotationPresent: false,
			eventLogSyncAnnotationValue:   "",
			shouldAddEventLogSync:         false,
		},
		{
			eventLogSyncAnnotationPresent: true,
			eventLogSyncAnnotationValue:   "",
			shouldAddEventLogSync:         false,
		},
		{
			eventLogSyncAnnotationPresent: true,
			eventLogSyncAnnotationValue:   "false",
			shouldAddEventLogSync:         false,
		},
		{
			eventLogSyncAnnotationPresent: true,
			eventLogSyncAnnotationValue:   "nonsense",
			shouldAddEventLogSync:         false,
		},
		{
			eventLogSyncAnnotationPresent: true,
			eventLogSyncAnnotationValue:   "true",
			shouldAddEventLogSync:         true,
		},
	}

	for _, tc := range testCases {

		driverPod := getSimplePod()
		driverPod.Labels = map[string]string{
			SparkRoleLabel: SparkRoleDriverValue,
		}
		if tc.eventLogSyncAnnotationPresent {
			driverPod.Annotations[controllers.WaveConfigAnnotationSyncEventLogs] = tc.eventLogSyncAnnotationValue
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
		assert.Equal(t, onDemandAffinity, newPod.Spec.Affinity)

		if tc.shouldAddEventLogSync {
			assert.Equal(t, len(driverPod.Spec.Containers)+1, len(newPod.Spec.Containers))
			assert.Equal(t, "storage-sync", newPod.Spec.Containers[0].Name)
			assert.Equal(t, len(driverPod.Spec.Volumes)+1, len(newPod.Spec.Volumes))
			assert.Equal(t, "spark-logs", newPod.Spec.Volumes[0].Name)
		} else {
			// No change to containers or volumes
			assert.Equal(t, driverPod.Spec.Containers, newPod.Spec.Containers)
			assert.Equal(t, driverPod.Spec.Volumes, newPod.Spec.Volumes)
		}
	}
}

func TestMutateExecutorPod(t *testing.T) {
	execPod := getSimplePod()
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
	assert.Equal(t, onDemandAntiAffinity, newPod.Spec.Affinity)
	assert.Equal(t, 0, len(newPod.Spec.Volumes))
}

func TestIdempotency(t *testing.T) {
	driverPod := getSimplePod()
	driverPod.Labels = map[string]string{
		SparkRoleLabel: SparkRoleDriverValue,
	}
	driverPod.Annotations[controllers.WaveConfigAnnotationSyncEventLogs] = "true"
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
	assert.Equal(t, onDemandAffinity, pod2.Spec.Affinity)
	assert.Equal(t, 1, len(newPod.Spec.Volumes))
	assert.Equal(t, "spark-logs", newPod.Spec.Volumes[0].Name)
}

func TestSkipNonSparkPod(t *testing.T) {
	nonSparkPod := getSimplePod()
	req := getAdmissionRequest(t, nonSparkPod)
	r, err := NewPodMutator(log, &util.FailedStorageProvider{}).Mutate(req)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, nonSparkPod.UID, r.UID)
	assert.Nil(t, r.PatchType)
	assert.Nil(t, r.Patch)
	assert.True(t, r.Allowed)
}

func TestMutatePodBadStorage(t *testing.T) {

	testFunc := func(provider cloudstorage.CloudStorageProvider) {
		driverPod := getSimplePod()
		driverPod.Labels = map[string]string{
			SparkRoleLabel: SparkRoleDriverValue,
		}
		driverPod.Annotations[controllers.WaveConfigAnnotationSyncEventLogs] = "true"

		req := getAdmissionRequest(t, driverPod)
		r, err := NewPodMutator(log, provider).Mutate(req)
		require.NoError(t, err)
		assert.NotNil(t, r)
		assert.Equal(t, driverPod.UID, r.UID)
		assert.NotNil(t, r.PatchType)
		assert.NotNil(t, r.Patch)
		assert.True(t, r.Allowed)

		obj, err := ApplyJsonPatch(r.Patch, driverPod)
		require.NoError(t, err)
		newPod, ok := obj.(*corev1.Pod)
		require.True(t, ok)

		// No change to containers or volumes
		assert.Equal(t, driverPod.Spec.Containers, newPod.Spec.Containers)
		assert.Equal(t, driverPod.Spec.Volumes, newPod.Spec.Volumes)

		// We still want to add node affinity even though we have a failed storage provider
		assert.Nil(t, driverPod.Spec.Affinity)
		assert.Equal(t, onDemandAffinity, newPod.Spec.Affinity)
	}

	t.Run("whenFailedStorageProvider", func(tt *testing.T) {
		testFunc(&util.FailedStorageProvider{})
	})

	t.Run("testNilStorageProvider", func(tt *testing.T) {
		testFunc(&util.NilStorageProvider{})
	})
}
