package admission

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/spotinst/wave-operator/cloudstorage"
	"github.com/spotinst/wave-operator/internal/config"
	"github.com/spotinst/wave-operator/internal/util"
)

func getOnDemandAffinity() *corev1.Affinity {
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      "spotinst.io/node-lifecycle",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{"od"},
							},
						},
					},
				},
			},
		},
	}
}

func getOnDemandAntiAffinity() *corev1.Affinity {
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{
				{
					Weight: 1,
					Preference: corev1.NodeSelectorTerm{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      "spotinst.io/node-lifecycle",
								Operator: corev1.NodeSelectorOpNotIn,
								Values:   []string{"od"},
							},
						},
					},
				},
			},
		},
	}
}

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
			driverPod.Annotations[config.WaveConfigAnnotationSyncEventLogs] = tc.eventLogSyncAnnotationValue
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
		newPod, ok := obj.(*corev1.Pod)
		assert.True(t, ok)
		assert.Equal(t, getOnDemandAffinity(), newPod.Spec.Affinity)

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

func TestMutateSparkPod_instanceConfiguration(t *testing.T) {

	type testCase struct {
		pod      *corev1.Pod
		expected *corev1.Affinity
	}

	getDriverPod := func() *corev1.Pod {
		pod := getSimplePod()
		pod.Labels = map[string]string{
			SparkRoleLabel: SparkRoleDriverValue,
		}
		return pod
	}

	getExecutorPod := func() *corev1.Pod {
		pod := getSimplePod()
		pod.Labels = map[string]string{
			SparkRoleLabel: SparkRoleExecutorValue,
		}
		return pod
	}

	testFunc := func(tt *testing.T, tc testCase) {

		req := getAdmissionRequest(tt, tc.pod)
		res, err := NewPodMutator(log, &util.FakeStorageProvider{}).Mutate(req)
		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, tc.pod.UID, res.UID)
		assert.Equal(tt, &jsonPatchType, res.PatchType)
		assert.NotNil(tt, res.Patch)
		assert.True(tt, res.Allowed)

		obj, err := ApplyJsonPatch(res.Patch, tc.pod)
		assert.NoError(tt, err)
		newPod, ok := obj.(*corev1.Pod)
		assert.True(tt, ok)
		assert.Equal(tt, tc.expected, newPod.Spec.Affinity)

	}

	t.Run("whenDefaultInstanceLifecycle_driver", func(tt *testing.T) {
		tc := testCase{
			pod:      getDriverPod(),
			expected: getOnDemandAffinity(),
		}
		testFunc(tt, tc)
	})

	t.Run("whenDefaultInstanceLifecycle_executor", func(tt *testing.T) {
		tc := testCase{
			pod:      getExecutorPod(),
			expected: getOnDemandAntiAffinity(),
		}
		testFunc(tt, tc)
	})

	t.Run("whenOD_driver", func(tt *testing.T) {
		pod := getDriverPod()
		pod.Annotations[config.WaveConfigAnnotationInstanceLifecycle] = "od"
		tc := testCase{
			pod:      pod,
			expected: getOnDemandAffinity(),
		}
		testFunc(tt, tc)
	})

	t.Run("whenOD_executor", func(tt *testing.T) {
		pod := getExecutorPod()
		pod.Annotations[config.WaveConfigAnnotationInstanceLifecycle] = "od"
		tc := testCase{
			pod:      pod,
			expected: getOnDemandAffinity(),
		}
		testFunc(tt, tc)
	})

	t.Run("whenSpot_driver", func(tt *testing.T) {
		pod := getDriverPod()
		pod.Annotations[config.WaveConfigAnnotationInstanceLifecycle] = "spot"
		tc := testCase{
			pod:      pod,
			expected: getOnDemandAntiAffinity(),
		}
		testFunc(tt, tc)
	})

	t.Run("whenSpot_executor", func(tt *testing.T) {
		pod := getExecutorPod()
		pod.Annotations[config.WaveConfigAnnotationInstanceLifecycle] = "spot"
		tc := testCase{
			pod:      pod,
			expected: getOnDemandAntiAffinity(),
		}
		testFunc(tt, tc)
	})

	t.Run("whenInstanceLifecycleAlreadyConfigured_shouldNotOverride", func(tt *testing.T) {

		// Required affinity

		requiredAffinity := getOnDemandAffinity()
		requiredAffinity.NodeAffinity.
			RequiredDuringSchedulingIgnoredDuringExecution.
			NodeSelectorTerms[0].MatchExpressions[0].Values = []string{"whatever"}

		pod := getDriverPod()
		pod.Spec.Affinity = requiredAffinity
		tc := testCase{
			pod:      pod,
			expected: requiredAffinity,
		}
		testFunc(tt, tc)

		pod = getDriverPod()
		pod.Spec.Affinity = requiredAffinity
		pod.Annotations[config.WaveConfigAnnotationInstanceLifecycle] = "spot"
		tc = testCase{
			pod:      pod,
			expected: requiredAffinity,
		}
		testFunc(tt, tc)

		pod = getExecutorPod()
		pod.Spec.Affinity = requiredAffinity
		tc = testCase{
			pod:      pod,
			expected: requiredAffinity,
		}
		testFunc(tt, tc)

		pod = getExecutorPod()
		pod.Spec.Affinity = requiredAffinity
		pod.Annotations[config.WaveConfigAnnotationInstanceLifecycle] = "od"
		tc = testCase{
			pod:      pod,
			expected: requiredAffinity,
		}
		testFunc(tt, tc)

		// Preferred affinity

		preferredAffinity := getOnDemandAntiAffinity()
		preferredAffinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0].
			Preference.MatchExpressions[0].Values = []string{"huh?"}

		pod = getDriverPod()
		pod.Spec.Affinity = preferredAffinity
		tc = testCase{
			pod:      pod,
			expected: preferredAffinity,
		}
		testFunc(tt, tc)

		pod = getDriverPod()
		pod.Spec.Affinity = preferredAffinity
		pod.Annotations[config.WaveConfigAnnotationInstanceLifecycle] = "spot"
		tc = testCase{
			pod:      pod,
			expected: preferredAffinity,
		}
		testFunc(tt, tc)

		pod = getExecutorPod()
		pod.Spec.Affinity = preferredAffinity
		tc = testCase{
			pod:      pod,
			expected: preferredAffinity,
		}
		testFunc(tt, tc)

		pod = getExecutorPod()
		pod.Spec.Affinity = preferredAffinity
		pod.Annotations[config.WaveConfigAnnotationInstanceLifecycle] = "od"
		tc = testCase{
			pod:      pod,
			expected: preferredAffinity,
		}
		testFunc(tt, tc)

	})

	t.Run("whenInstanceLifecycleMisConfigured", func(tt *testing.T) {

		pod := getDriverPod()
		pod.Annotations[config.WaveConfigAnnotationInstanceLifecycle] = ""
		tc := testCase{
			pod:      pod,
			expected: getOnDemandAffinity(),
		}
		testFunc(tt, tc)

		pod = getDriverPod()
		pod.Annotations[config.WaveConfigAnnotationInstanceLifecycle] = "nonsense (driver)"
		tc = testCase{
			pod:      pod,
			expected: getOnDemandAffinity(),
		}
		testFunc(tt, tc)

		pod = getDriverPod()
		pod.Annotations[config.WaveConfigAnnotationInstanceLifecycle] = " SPOT " // Test input sanitation
		tc = testCase{
			pod:      pod,
			expected: getOnDemandAntiAffinity(),
		}
		testFunc(tt, tc)

		pod = getExecutorPod()
		pod.Annotations[config.WaveConfigAnnotationInstanceLifecycle] = ""
		tc = testCase{
			pod:      pod,
			expected: getOnDemandAntiAffinity(),
		}
		testFunc(tt, tc)

		pod = getExecutorPod()
		pod.Annotations[config.WaveConfigAnnotationInstanceLifecycle] = "nonsense (executor)"
		tc = testCase{
			pod:      pod,
			expected: getOnDemandAntiAffinity(),
		}
		testFunc(tt, tc)

		pod = getExecutorPod()
		pod.Annotations[config.WaveConfigAnnotationInstanceLifecycle] = " OD " // Test input sanitation
		tc = testCase{
			pod:      pod,
			expected: getOnDemandAffinity(),
		}
		testFunc(tt, tc)

	})

	t.Run("whenInstanceTypesConfigured_driver", func(tt *testing.T) {

		expectedAffinity := getOnDemandAffinity()
		expectedAffinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions =
			append(expectedAffinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions,
				corev1.NodeSelectorRequirement{
					Key:      "node.kubernetes.io/instance-type",
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"t2.micro", "m5.xlarge"},
				},
				corev1.NodeSelectorRequirement{
					Key:      "beta.kubernetes.io/instance-type",
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"t2.micro", "m5.xlarge"},
				})

		pod := getDriverPod()
		pod.Annotations[config.WaveConfigAnnotationInstanceType] = "t2.micro, m5, m5.xlarge"
		tc := testCase{
			pod:      pod,
			expected: expectedAffinity,
		}
		testFunc(tt, tc)

	})

	t.Run("whenInstanceTypesConfigured_executor", func(tt *testing.T) {

		expectedAffinity := getOnDemandAntiAffinity()
		expectedAffinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      "node.kubernetes.io/instance-type",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"t2.micro", "m5.xlarge"},
						},
						{
							Key:      "beta.kubernetes.io/instance-type",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"t2.micro", "m5.xlarge"},
						},
					},
				},
			},
		}

		pod := getExecutorPod()
		pod.Annotations[config.WaveConfigAnnotationInstanceType] = "t2.micro, m5, m5.xlarge"
		tc := testCase{
			pod:      pod,
			expected: expectedAffinity,
		}
		testFunc(tt, tc)

	})

	t.Run("whenInstanceTypesNotConfigured", func(tt *testing.T) {

		expectedAffinity := getOnDemandAffinity()

		pod := getDriverPod()
		tc := testCase{
			pod:      pod,
			expected: expectedAffinity,
		}
		testFunc(tt, tc)

		pod = getDriverPod()
		pod.Annotations[config.WaveConfigAnnotationInstanceType] = ""
		tc = testCase{
			pod:      pod,
			expected: expectedAffinity,
		}
		testFunc(tt, tc)

		expectedAffinity = getOnDemandAntiAffinity()

		pod = getExecutorPod()
		tc = testCase{
			pod:      pod,
			expected: expectedAffinity,
		}
		testFunc(tt, tc)

		pod = getExecutorPod()
		pod.Annotations[config.WaveConfigAnnotationInstanceType] = ""
		tc = testCase{
			pod:      pod,
			expected: expectedAffinity,
		}
		testFunc(tt, tc)

	})

	t.Run("whenInstanceTypesAlreadyConfigured", func(tt *testing.T) {

	})

	t.Run("whenInstanceTypesMisConfigured", func(tt *testing.T) {

	})

	t.Run("whenOtherNodeSelectorRequirements_shouldNotOverride", func(tt *testing.T) {

	})

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
	assert.Equal(t, getOnDemandAntiAffinity(), newPod.Spec.Affinity)
	assert.Equal(t, 0, len(newPod.Spec.Volumes))
}

func TestIdempotency(t *testing.T) {
	driverPod := getSimplePod()
	driverPod.Labels = map[string]string{
		SparkRoleLabel: SparkRoleDriverValue,
	}
	driverPod.Annotations[config.WaveConfigAnnotationSyncEventLogs] = "true"
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
	assert.Equal(t, getOnDemandAffinity(), pod2.Spec.Affinity)
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
		driverPod.Annotations[config.WaveConfigAnnotationSyncEventLogs] = "true"

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
		assert.Equal(t, getOnDemandAffinity(), newPod.Spec.Affinity)
	}

	t.Run("whenFailedStorageProvider", func(tt *testing.T) {
		testFunc(&util.FailedStorageProvider{})
	})

	t.Run("whenNilStorageProvider", func(tt *testing.T) {
		testFunc(&util.NilStorageProvider{})
	})
}
