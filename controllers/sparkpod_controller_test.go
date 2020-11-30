package controllers

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	"github.com/golang/mock/gomock"
	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/internal/sparkapi"
	"github.com/spotinst/wave-operator/internal/sparkapi/mock_sparkapi"
	"github.com/spotinst/wave-operator/internal/version"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrlrt "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlrt_fake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"testing"
)

//var testScheme = runtime.NewScheme()

func init() {
	_ = clientgoscheme.AddToScheme(testScheme)
	_ = v1alpha1.AddToScheme(testScheme)
	_ = apiextensions.AddToScheme(testScheme)

	version.BuildVersion = "v0.0.0-test"
	version.BuildDate = "1970-01-01T00:00:00Z"
}

func TestReconcile_sparkAppIdMissing(t *testing.T) {
	ctx := context.TODO()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "test-ns",
			Labels: map[string]string{
				SparkAppLabel: "",
			},
		},
	}

	ctrlClient := ctrlrt_fake.NewFakeClientWithScheme(testScheme, pod)
	clientSet := k8sfake.NewSimpleClientset()

	controller := NewSparkPodReconciler(ctrlClient, clientSet, nil, getTestLogger(), testScheme)

	req := ctrlrt.Request{
		NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
	}

	res, err := controller.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, ctrlrt.Result{}, res)

	// No spark application CR should have been created
	crList := &v1alpha1.SparkApplicationList{}
	err = ctrlClient.List(ctx, crList, &client.ListOptions{
		Namespace: pod.Namespace,
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(crList.Items))
}

func TestReconcile_unknownRole(t *testing.T) {
	ctx := context.TODO()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "test-ns",
			Labels: map[string]string{
				SparkAppLabel:  "spark-123",
				SparkRoleLabel: "nonsense",
			},
		},
	}

	ctrlClient := ctrlrt_fake.NewFakeClientWithScheme(testScheme, pod)
	clientSet := k8sfake.NewSimpleClientset()

	controller := NewSparkPodReconciler(ctrlClient, clientSet, nil, getTestLogger(), testScheme)

	req := ctrlrt.Request{
		NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
	}

	res, err := controller.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, ctrlrt.Result{}, res)

	// No spark application CR should have been created
	crList := &v1alpha1.SparkApplicationList{}
	err = ctrlClient.List(ctx, crList, &client.ListOptions{
		Namespace: pod.Namespace,
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(crList.Items))
}

func TestReconcile_driver_garbageCollectionEvent(t *testing.T) {
	ctx := context.TODO()

	sparkAppId := "spark-123456"

	deletionTimestamp := metav1.Unix(int64(1234), int64(1000))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-driver",
			Namespace: "test-ns",
			UID:       "123-456-789",
			Labels: map[string]string{
				SparkAppLabel:  sparkAppId,
				SparkRoleLabel: DriverRole,
			},
			DeletionTimestamp: &deletionTimestamp,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: apiVersion,
					Kind:       sparkApplicationKind,
					Name:       sparkAppId,
					UID:        "123-456-789-5555",
				},
			},
		},
	}

	ctrlClient := ctrlrt_fake.NewFakeClientWithScheme(testScheme, pod)
	clientSet := k8sfake.NewSimpleClientset()

	// Mock Spark API manager
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock_sparkapi.NewMockManager(ctrl)
	var getMockSparkApiManager SparkApiManagerGetter = func(clientSet kubernetes.Interface, driverPod *corev1.Pod, logger logr.Logger) (sparkapi.Manager, error) {
		return m, nil
	}

	controller := NewSparkPodReconciler(ctrlClient, clientSet, getMockSparkApiManager, getTestLogger(), testScheme)

	req := ctrlrt.Request{
		NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
	}

	res, err := controller.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, ctrlrt.Result{}, res)

	// No spark application CR should have been created
	crList := &v1alpha1.SparkApplicationList{}
	err = ctrlClient.List(ctx, crList, &client.ListOptions{
		Namespace: pod.Namespace,
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(crList.Items))
}

func TestReconcile_driver_whenSparkApiCommunicationFails(t *testing.T) {
	ctx := context.TODO()

	sparkAppId := "spark-123456"

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-driver",
			Namespace: "test-ns",
			UID:       "123-456-789",
			Labels: map[string]string{
				SparkAppLabel:  sparkAppId,
				SparkRoleLabel: DriverRole,
			},
		},
	}

	ctrlClient := ctrlrt_fake.NewFakeClientWithScheme(testScheme, pod)
	clientSet := k8sfake.NewSimpleClientset()

	// Mock Spark API manager
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := mock_sparkapi.NewMockManager(ctrl)
	m.EXPECT().GetApplicationInfo(sparkAppId).Return(nil, fmt.Errorf("test error")).Times(1)

	var getMockSparkApiManager SparkApiManagerGetter = func(clientSet kubernetes.Interface, driverPod *corev1.Pod, logger logr.Logger) (sparkapi.Manager, error) {
		return m, nil
	}

	controller := NewSparkPodReconciler(ctrlClient, clientSet, getMockSparkApiManager, getTestLogger(), testScheme)

	req := ctrlrt.Request{
		NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
	}

	res, err := controller.Reconcile(ctx, req)
	// TODO throw error instead of just requeue
	assert.NoError(t, err) // Should be no error
	assert.Equal(t, ctrlrt.Result{Requeue: true, RequeueAfter: requeueAfterTimeout}, res)

	createdCr := &v1alpha1.SparkApplication{}
	err = ctrlClient.Get(ctx, client.ObjectKey{Name: sparkAppId, Namespace: pod.Namespace}, createdCr)
	require.NoError(t, err)

	verifyCrPod(t, pod, createdCr.Status.Data.Driver, false)
	assert.Equal(t, pod.Name, createdCr.Spec.ApplicationName) // Fall back on driver name
	assert.Equal(t, sparkAppId, createdCr.Spec.ApplicationId)

}

func TestReconcile_driver_whenSuccessful(t *testing.T) {
}

func TestReconcile_executor_whenSuccessful(t *testing.T) {
	ctx := context.TODO()

	applicationId := "spark-123"
	ns := "test-ns"

	exec1 := getTestPod(ns, "exec1", "123890", ExecutorRole, applicationId, false)
	exec2 := getTestPod(ns, "exec2", "456789", ExecutorRole, applicationId, false)

	cr := getMinimalTestCr(ns, applicationId)

	ctrlClient := ctrlrt_fake.NewFakeClientWithScheme(testScheme, exec1, exec2, cr)
	clientSet := k8sfake.NewSimpleClientset()

	controller := NewSparkPodReconciler(ctrlClient, clientSet, nil, getTestLogger(), testScheme)

	// Executor 1

	req := ctrlrt.Request{
		NamespacedName: types.NamespacedName{Namespace: exec1.Namespace, Name: exec1.Name},
	}
	_, err := controller.Reconcile(ctx, req)
	require.NoError(t, err)

	patchedCr := &v1alpha1.SparkApplication{}
	err = ctrlClient.Get(ctx, client.ObjectKey{Name: applicationId, Namespace: ns}, patchedCr)
	require.NoError(t, err)
	assert.Equal(t, 1, len(patchedCr.Status.Data.Executors))
	executor := patchedCr.Status.Data.Executors[0]
	verifyCrPod(t, exec1, executor, false)

	// Executor 2

	req = ctrlrt.Request{
		NamespacedName: types.NamespacedName{Namespace: exec2.Namespace, Name: exec2.Name},
	}
	_, err = controller.Reconcile(ctx, req)
	require.NoError(t, err)

	patchedCr = &v1alpha1.SparkApplication{}
	err = ctrlClient.Get(ctx, client.ObjectKey{Name: applicationId, Namespace: ns}, patchedCr)
	require.NoError(t, err)
	assert.Equal(t, 2, len(patchedCr.Status.Data.Executors))
	executor = patchedCr.Status.Data.Executors[1]
	verifyCrPod(t, exec2, executor, false)

	// Update executor 1

	exec1.Status.Phase = corev1.PodSucceeded
	deletionTimestamp := metav1.Unix(int64(1234), int64(1000))
	exec1.DeletionTimestamp = &deletionTimestamp
	err = ctrlClient.Update(ctx, exec1, &client.UpdateOptions{})
	require.NoError(t, err)

	req = ctrlrt.Request{
		NamespacedName: types.NamespacedName{Namespace: exec1.Namespace, Name: exec1.Name},
	}
	_, err = controller.Reconcile(ctx, req)
	require.NoError(t, err)

	patchedCr = &v1alpha1.SparkApplication{}
	err = ctrlClient.Get(ctx, client.ObjectKey{Name: applicationId, Namespace: ns}, patchedCr)
	require.NoError(t, err)
	assert.Equal(t, 2, len(patchedCr.Status.Data.Executors))
	executor = patchedCr.Status.Data.Executors[0]
	verifyCrPod(t, exec1, executor, true)
}

func TestReconcile_executor_whenCrDoesntExist(t *testing.T) {
	ctx := context.TODO()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-executor",
			Namespace: "test-ns",
			Labels: map[string]string{
				SparkAppLabel:  "spark-123456",
				SparkRoleLabel: ExecutorRole,
			},
		},
	}

	ctrlClient := ctrlrt_fake.NewFakeClientWithScheme(testScheme, pod)
	clientSet := k8sfake.NewSimpleClientset()

	controller := NewSparkPodReconciler(ctrlClient, clientSet, nil, getTestLogger(), testScheme)

	req := ctrlrt.Request{
		NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
	}

	_, err := controller.Reconcile(ctx, req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not get spark application cr")
}

func TestReconcile_finalizer_add(t *testing.T) {
	ctx := context.TODO()

	applicationId := "spark-123"
	ns := "test-ns"

	pod := getTestPod(ns, "exec1", "123890", ExecutorRole, applicationId, false)

	cr := getMinimalTestCr(ns, applicationId)

	ctrlClient := ctrlrt_fake.NewFakeClientWithScheme(testScheme, pod, cr)
	clientSet := k8sfake.NewSimpleClientset()

	controller := NewSparkPodReconciler(ctrlClient, clientSet, nil, getTestLogger(), testScheme)

	req := ctrlrt.Request{
		NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
	}
	_, err := controller.Reconcile(ctx, req)
	require.NoError(t, err)

	updatedPod := &corev1.Pod{}
	err = ctrlClient.Get(ctx, client.ObjectKey{Name: pod.Name, Namespace: pod.Namespace}, updatedPod)
	require.NoError(t, err)

	assert.Equal(t, 1, len(updatedPod.Finalizers))
	assert.Equal(t, sparkApplicationFinalizerName, updatedPod.Finalizers[0])
}

func TestReconcile_finalizer_remove(t *testing.T) {
	ctx := context.TODO()

	applicationId := "spark-123"
	ns := "test-ns"

	pod := getTestPod(ns, "exec1", "123890", ExecutorRole, applicationId, true)
	pod.Finalizers = []string{sparkApplicationFinalizerName}

	cr := getMinimalTestCr(ns, applicationId)

	ctrlClient := ctrlrt_fake.NewFakeClientWithScheme(testScheme, pod, cr)
	clientSet := k8sfake.NewSimpleClientset()

	controller := NewSparkPodReconciler(ctrlClient, clientSet, nil, getTestLogger(), testScheme)

	req := ctrlrt.Request{
		NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
	}
	_, err := controller.Reconcile(ctx, req)
	require.NoError(t, err)

	updatedPod := &corev1.Pod{}
	err = ctrlClient.Get(ctx, client.ObjectKey{Name: pod.Name, Namespace: pod.Namespace}, updatedPod)
	require.NoError(t, err)

	assert.Equal(t, 0, len(updatedPod.Finalizers))
}

func TestReconcile_ownerReference_add(t *testing.T) {
	// TODO
}

func getTestLogger() logr.Logger {
	return zap.New(zap.UseDevMode(true))
}

func verifyCrPod(t *testing.T, pod *corev1.Pod, crPod v1alpha1.Pod, deleted bool) {
	assert.Equal(t, pod.Name, crPod.Name)
	assert.Equal(t, pod.Namespace, crPod.Namespace)
	assert.Equal(t, string(pod.UID), crPod.UID)
	assert.Equal(t, pod.Status.Phase, crPod.Phase)
	assert.Equal(t, deleted, crPod.Deleted)
	assert.Equal(t, len(pod.Status.ContainerStatuses), len(crPod.Statuses))

	for _, cs := range pod.Status.ContainerStatuses {
		foundCs := false
		for _, crcs := range crPod.Statuses {
			if cs.ContainerID == crcs.ContainerID {
				foundCs = true
				assert.Equal(t, cs.ContainerID, crcs.ContainerID)
				assert.Equal(t, cs.Name, crcs.Name)
				assert.Equal(t, cs.State, crcs.State)
				assert.Equal(t, cs.Ready, crcs.Ready)
				assert.Equal(t, cs.Image, crcs.Image)
				assert.Equal(t, cs.ImageID, crcs.ImageID)
				assert.Equal(t, cs.LastTerminationState, crcs.LastTerminationState)
				assert.Equal(t, cs.RestartCount, crcs.RestartCount)
				assert.Equal(t, cs.Started, crcs.Started)
			}
		}
		assert.True(t, foundCs)
	}
}

func getMinimalTestCr(namespace string, applicationId string) *v1alpha1.SparkApplication {
	return &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      applicationId,
			Namespace: namespace,
		},
		Spec: v1alpha1.SparkApplicationSpec{
			ApplicationId:   applicationId,
			ApplicationName: "my test application",
			Heritage:        "spark-submit",
		},
		Status: v1alpha1.SparkApplicationStatus{
			Data: v1alpha1.SparkApplicationData{
				SparkProperties: nil,
				RunStatistics:   v1alpha1.Statistics{},
				Driver:          v1alpha1.Pod{},
				Executors:       nil,
			},
		},
	}
}

func getTestPod(namespace string, name string, uid string, role string, applicationId string, deleted bool) *corev1.Pod {
	var deletionTimestamp metav1.Time
	if deleted {
		deletionTimestamp = metav1.Unix(int64(1234), int64(1000))
	}
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         namespace,
			UID:               types.UID(uid),
			DeletionTimestamp: &deletionTimestamp,
			Labels: map[string]string{
				SparkAppLabel:  applicationId,
				SparkRoleLabel: role,
			},
		},
		Spec: corev1.PodSpec{},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: fmt.Sprintf("spark-kubernetes-%s", role),
					State: corev1.ContainerState{
						Waiting: nil,
						Running: &corev1.ContainerStateRunning{
							StartedAt: metav1.Unix(int64(100000), int64(0)),
						},
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode:  1,
							StartedAt: metav1.Unix(int64(100000), int64(0)),
						},
					},
					LastTerminationState: corev1.ContainerState{},
					Ready:                true,
					RestartCount:         9,
					Image:                "spark-image",
					ImageID:              "spark-image-id",
					ContainerID:          "container-id",
					Started:              nil,
				},
			},
		},
	}
}
