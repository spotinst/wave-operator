package controllers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/golang/mock/gomock"
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

	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/internal/config"
	"github.com/spotinst/wave-operator/internal/sparkapi"
	sparkapiclient "github.com/spotinst/wave-operator/internal/sparkapi/client"
	"github.com/spotinst/wave-operator/internal/sparkapi/client/transport"
	"github.com/spotinst/wave-operator/internal/sparkapi/mock_sparkapi"
	"github.com/spotinst/wave-operator/internal/version"
)

func init() {
	_ = clientgoscheme.AddToScheme(testScheme)
	_ = v1alpha1.AddToScheme(testScheme)
	_ = apiextensions.AddToScheme(testScheme)

	version.BuildVersion = "v0.0.0-test"
	version.BuildDate = "1970-01-01T00:00:00Z"
}

func TestReconcile_sparkAppIDMissing(t *testing.T) {
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

	sparkAppID := "spark-123456"

	pod := getTestPod("test-ns", "test-driver", "123-456", DriverRole, sparkAppID, true)
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: apiVersion,
		Kind:       sparkApplicationKind,
		Name:       sparkAppID,
		UID:        "123-456-789-5555",
	}}
	pod.Finalizers = []string{sparkApplicationFinalizerName}

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

	// Finalizer should have been removed
	updatedPod := &corev1.Pod{}
	err = ctrlClient.Get(ctx, client.ObjectKey{Name: pod.Name, Namespace: pod.Namespace}, updatedPod)
	require.NoError(t, err)
	assert.Equal(t, 0, len(updatedPod.Finalizers))

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

	sparkAppID := "spark-123456"

	cr := getMinimalTestCR("test-ns", sparkAppID)

	pod := getTestPod("test-ns", "test-driver", "123-456", DriverRole, sparkAppID, false)
	pod.Finalizers = []string{sparkApplicationFinalizerName}
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: apiVersion,
		Kind:       sparkApplicationKind,
		Name:       cr.Name,
		UID:        cr.UID,
	}}

	ctrlClient := ctrlrt_fake.NewFakeClientWithScheme(testScheme, pod, cr)
	clientSet := k8sfake.NewSimpleClientset()

	// Mock Spark API manager
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := mock_sparkapi.NewMockManager(ctrl)
	m.EXPECT().GetApplicationInfo(sparkAppID).Return(nil, fmt.Errorf("test error")).Times(1)

	var getMockSparkApiManager SparkApiManagerGetter = func(clientSet kubernetes.Interface, driverPod *corev1.Pod, logger logr.Logger) (sparkapi.Manager, error) {
		return m, nil
	}

	controller := NewSparkPodReconciler(ctrlClient, clientSet, getMockSparkApiManager, getTestLogger(), testScheme)

	req := ctrlrt.Request{
		NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
	}

	_, err := controller.Reconcile(ctx, req)
	assert.Error(t, err)

	// Still want to have updated the driver pod info in the CR
	createdCR := &v1alpha1.SparkApplication{}
	err = ctrlClient.Get(ctx, client.ObjectKey{Name: sparkAppID, Namespace: pod.Namespace}, createdCR)
	require.NoError(t, err)
	verifyCRPod(t, pod, createdCR.Status.Data.Driver)
}

func TestReconcile_driver_whenSparkApiError(t *testing.T) {

	ctx := context.TODO()
	sparkAppID := "spark-123456"
	cr := getMinimalTestCR("test-ns", sparkAppID)
	pod := getTestPod("test-ns", "test-driver", "123-456", DriverRole, sparkAppID, false)
	pod.Finalizers = []string{sparkApplicationFinalizerName}
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: apiVersion,
		Kind:       sparkApplicationKind,
		Name:       cr.Name,
		UID:        cr.UID,
	}}

	ctrlClient := ctrlrt_fake.NewFakeClientWithScheme(testScheme, pod, cr)
	clientSet := k8sfake.NewSimpleClientset()

	// Mock Spark API manager
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock_sparkapi.NewMockManager(ctrl)

	var getMockSparkApiManager SparkApiManagerGetter = func(clientSet kubernetes.Interface, driverPod *corev1.Pod, logger logr.Logger) (sparkapi.Manager, error) {
		return m, nil
	}

	controller := NewSparkPodReconciler(ctrlClient, clientSet, getMockSparkApiManager, getTestLogger(), testScheme)

	testReconcile := func(podPhase corev1.PodPhase, sparkApiError error) (ctrlrt.Result, error) {

		pod.Status.Phase = podPhase
		err := ctrlClient.Update(ctx, pod)
		require.NoError(t, err)

		m.EXPECT().GetApplicationInfo(sparkAppID).Return(getTestApplicationInfo(), sparkApiError).Times(1)

		req := ctrlrt.Request{
			NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
		}

		reconcileRes, reconcileErr := controller.Reconcile(ctx, req)

		// Still want to have updated the driver pod info in the CR
		createdCR := &v1alpha1.SparkApplication{}
		err = ctrlClient.Get(ctx, client.ObjectKey{Name: sparkAppID, Namespace: pod.Namespace}, createdCR)
		require.NoError(t, err)
		verifyCRPod(t, pod, createdCR.Status.Data.Driver)

		return reconcileRes, reconcileErr
	}

	t.Run("testUpdateAttemptCount", func(tt *testing.T) {

		notFoundError := transport.NewNotFoundError(fmt.Errorf("test error"))

		// No update for running pods - regular error
		res, err := testReconcile(corev1.PodRunning, fmt.Errorf("test error"))
		assert.Error(tt, err)
		assert.Equal(tt, ctrlrt.Result{}, res)
		assert.Equal(tt, 0, controller.sparkApiAttemptCounter[sparkAppID])

		// No update for running pods - expected Spark API error
		res, err = testReconcile(corev1.PodRunning, notFoundError)
		assert.NoError(tt, err)
		assert.Equal(tt, ctrlrt.Result{
			Requeue:      true,
			RequeueAfter: requeueAfterTimeout,
		}, res)
		assert.Equal(tt, 0, controller.sparkApiAttemptCounter[sparkAppID])

		// No update for non-running pods - regular error
		res, err = testReconcile(corev1.PodSucceeded, fmt.Errorf("test error"))
		assert.Error(tt, err)
		assert.Equal(tt, ctrlrt.Result{}, res)
		assert.Equal(tt, 0, controller.sparkApiAttemptCounter[sparkAppID])

		// Update for non-running pods - expected Spark API error

		res, err = testReconcile(corev1.PodSucceeded, notFoundError)
		assert.NoError(tt, err)
		assert.Equal(tt, ctrlrt.Result{
			Requeue:      true,
			RequeueAfter: requeueAfterTimeout,
		}, res)
		assert.Equal(tt, 1, controller.sparkApiAttemptCounter[sparkAppID])

		res, err = testReconcile(corev1.PodSucceeded, notFoundError)
		assert.NoError(tt, err)
		assert.Equal(tt, ctrlrt.Result{
			Requeue:      true,
			RequeueAfter: requeueAfterTimeout,
		}, res)
		assert.Equal(tt, 2, controller.sparkApiAttemptCounter[sparkAppID])

		// Successful Spark API communication should reset counter
		res, err = testReconcile(corev1.PodSucceeded, nil)
		assert.NoError(tt, err)
		assert.Equal(tt, ctrlrt.Result{
			Requeue:      false,
			RequeueAfter: 0,
		}, res)
		_, entryExists := controller.sparkApiAttemptCounter[sparkAppID]
		assert.False(tt, entryExists)
		assert.Equal(tt, 0, controller.sparkApiAttemptCounter[sparkAppID])

	})

	t.Run("testShouldRequeue", func(tt *testing.T) {

		notFoundError := transport.NewNotFoundError(fmt.Errorf("test error"))

		// Should requeue
		res, err := testReconcile(corev1.PodSucceeded, notFoundError)
		assert.NoError(tt, err)
		assert.Equal(tt, ctrlrt.Result{
			Requeue:      true,
			RequeueAfter: requeueAfterTimeout,
		}, res)
		assert.Equal(tt, 1, controller.sparkApiAttemptCounter[sparkAppID])

		// Should requeue
		res, err = testReconcile(corev1.PodSucceeded, notFoundError)
		assert.NoError(tt, err)
		assert.Equal(tt, ctrlrt.Result{
			Requeue:      true,
			RequeueAfter: requeueAfterTimeout,
		}, res)
		assert.Equal(tt, 2, controller.sparkApiAttemptCounter[sparkAppID])

		// Set Spark API communication attempts to max - 1
		controller.sparkApiAttemptCounter[sparkAppID] = maxSparkApiCommunicationAttemptCount - 1

		// Should always requeue running pods
		res, err = testReconcile(corev1.PodRunning, notFoundError)
		assert.NoError(tt, err)
		assert.Equal(tt, ctrlrt.Result{
			Requeue:      true,
			RequeueAfter: requeueAfterTimeout,
		}, res)
		assert.Equal(tt, maxSparkApiCommunicationAttemptCount-1, controller.sparkApiAttemptCounter[sparkAppID])

		// Should not requeue non-running pod - max attempts reached
		res, err = testReconcile(corev1.PodSucceeded, notFoundError)
		assert.NoError(tt, err)
		assert.Equal(tt, ctrlrt.Result{
			Requeue:      false,
			RequeueAfter: 0,
		}, res)
		assert.Equal(tt, maxSparkApiCommunicationAttemptCount, controller.sparkApiAttemptCounter[sparkAppID])

	})

}

func TestReconcile_driver_whenSuccessful(t *testing.T) {
	ctx := context.TODO()

	sparkAppID := "spark-123456"

	pod := getTestPod("test-ns", "test-driver", "123-456", DriverRole, sparkAppID, false)

	ctrlClient := ctrlrt_fake.NewFakeClientWithScheme(testScheme, pod)
	clientSet := k8sfake.NewSimpleClientset()

	// Mock Spark API manager
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := mock_sparkapi.NewMockManager(ctrl)
	m.EXPECT().GetApplicationInfo(sparkAppID).Return(getTestApplicationInfo(), nil).Times(1)

	var getMockSparkApiManager SparkApiManagerGetter = func(clientSet kubernetes.Interface, driverPod *corev1.Pod, logger logr.Logger) (sparkapi.Manager, error) {
		return m, nil
	}

	controller := NewSparkPodReconciler(ctrlClient, clientSet, getMockSparkApiManager, getTestLogger(), testScheme)

	req := ctrlrt.Request{
		NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
	}

	// First reconcile - finalizer added
	res, err := controller.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, ctrlrt.Result{Requeue: true}, res)

	updatedPod := &corev1.Pod{}
	err = ctrlClient.Get(ctx, client.ObjectKey{Name: pod.Name, Namespace: pod.Namespace}, updatedPod)
	require.NoError(t, err)
	assert.Equal(t, 1, len(updatedPod.Finalizers))

	// Second reconcile - cr created
	res, err = controller.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, ctrlrt.Result{Requeue: true}, res)

	createdCR := &v1alpha1.SparkApplication{}
	err = ctrlClient.Get(ctx, client.ObjectKey{Name: sparkAppID, Namespace: pod.Namespace}, createdCR)
	require.NoError(t, err)
	assert.Equal(t, 2, len(createdCR.Labels))
	assert.Equal(t, sparkApplicationKind, createdCR.Labels[waveKindLabel])
	assert.Equal(t, sparkAppID, createdCR.Labels[waveSparkApplicationIDLabel])
	assert.Equal(t, sparkAppID, createdCR.Name)
	assert.Equal(t, pod.Namespace, createdCR.Namespace)
	// Application name == driver name until we learn otherwise from Spark API
	assert.Equal(t, pod.Name, createdCR.Spec.ApplicationName)
	assert.Equal(t, v1alpha1.SparkHeritageSubmit, createdCR.Spec.Heritage)
	assert.Equal(t, sparkAppID, createdCR.Spec.ApplicationID)
	verifyCRPod(t, pod, createdCR.Status.Data.Driver)

	// Third reconcile - owner reference added
	res, err = controller.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, ctrlrt.Result{Requeue: true}, res)

	err = ctrlClient.Get(ctx, client.ObjectKey{Name: pod.Name, Namespace: pod.Namespace}, updatedPod)
	require.NoError(t, err)
	assert.Equal(t, 1, len(updatedPod.OwnerReferences))
	assert.Equal(t, createdCR.Name, updatedPod.OwnerReferences[0].Name)

	// Fourth reconcile - cr updated
	res, err = controller.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, ctrlrt.Result{Requeue: true, RequeueAfter: requeueAfterTimeout}, res)

	createdCR = &v1alpha1.SparkApplication{}
	err = ctrlClient.Get(ctx, client.ObjectKey{Name: sparkAppID, Namespace: pod.Namespace}, createdCR)
	require.NoError(t, err)

	verifyCRPod(t, pod, createdCR.Status.Data.Driver)
	assert.Equal(t, getTestApplicationInfo().ApplicationName, createdCR.Spec.ApplicationName)
	assert.Equal(t, sparkAppID, createdCR.Spec.ApplicationID)
	assert.Equal(t, getTestApplicationInfo().SparkProperties, createdCR.Status.Data.SparkProperties)
	assert.Equal(t, string(getTestApplicationInfo().WorkloadType), createdCR.Annotations[workloadTypeAnnotation])
	verifyCRAttempts(t, getTestApplicationInfo().Attempts, createdCR.Status.Data.RunStatistics.Attempts)
	verifyCRExecutors(t, getTestApplicationInfo().Executors, createdCR.Status.Data.RunStatistics.Executors)
}

func TestReconcile_driver_whenPodDeletionTimeoutPassed(t *testing.T) {
	ctx := context.TODO()

	sparkAppID := "spark-123456"
	pod := getTestPod("test-ns", "test-driver", "123-456", DriverRole, sparkAppID, false)
	pod.Finalizers = []string{sparkApplicationFinalizerName}
	deletionTimestamp := metav1.NewTime(time.Now().Add(-30 * time.Minute)) // Deleted 30 minutes ago
	pod.DeletionTimestamp = &deletionTimestamp

	ctrlClient := ctrlrt_fake.NewFakeClientWithScheme(testScheme, pod)
	clientSet := k8sfake.NewSimpleClientset()

	// Mock Spark API manager
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := mock_sparkapi.NewMockManager(ctrl)
	m.EXPECT().GetApplicationInfo(sparkAppID).Return(getTestApplicationInfo(), nil).Times(0)

	var getMockSparkApiManager SparkApiManagerGetter = func(clientSet kubernetes.Interface, driverPod *corev1.Pod, logger logr.Logger) (sparkapi.Manager, error) {
		return m, nil
	}

	controller := NewSparkPodReconciler(ctrlClient, clientSet, getMockSparkApiManager, getTestLogger(), testScheme)

	req := ctrlrt.Request{
		NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
	}

	// Reconcile should just return without requeue
	res, err := controller.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, ctrlrt.Result{}, res)

	// Finalizer should have been removed
	updatedPod := &corev1.Pod{}
	err = ctrlClient.Get(ctx, client.ObjectKey{Name: pod.Name, Namespace: pod.Namespace}, updatedPod)
	require.NoError(t, err)
	assert.Equal(t, 0, len(updatedPod.Finalizers))
}

func TestReconcile_driver_setApplicationName(t *testing.T) {
	ctx := context.TODO()
	sparkAppID := "spark-123456"

	// Mock Spark API manager
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock_sparkapi.NewMockManager(ctrl)
	m.EXPECT().GetApplicationInfo(sparkAppID).Return(getTestApplicationInfo(), nil).AnyTimes()
	var getMockSparkApiManager SparkApiManagerGetter = func(clientSet kubernetes.Interface, driverPod *corev1.Pod, logger logr.Logger) (sparkapi.Manager, error) {
		return m, nil
	}

	testFunc := func(tt *testing.T, pod *corev1.Pod, expectedName string, callSparkAPI bool) {

		ctrlClient := ctrlrt_fake.NewFakeClientWithScheme(testScheme, pod)
		clientSet := k8sfake.NewSimpleClientset()
		controller := NewSparkPodReconciler(ctrlClient, clientSet, getMockSparkApiManager, getTestLogger(), testScheme)

		req := ctrlrt.Request{
			NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
		}

		// First reconcile - finalizer added
		_, err := controller.Reconcile(ctx, req)
		assert.NoError(t, err)

		// Second reconcile - cr created
		_, err = controller.Reconcile(ctx, req)
		assert.NoError(t, err)

		// Third reconcile - owner reference added
		_, err = controller.Reconcile(ctx, req)
		assert.NoError(t, err)

		if callSparkAPI {
			// Fourth reconcile - cr updated with Spark API info
			_, err = controller.Reconcile(ctx, req)
			assert.NoError(t, err)
		}

		createdCR := &v1alpha1.SparkApplication{}
		err = ctrlClient.Get(ctx, client.ObjectKey{Name: sparkAppID, Namespace: pod.Namespace}, createdCR)
		require.NoError(t, err)

		// Application name set as annotation on CR
		assert.Equal(tt, expectedName, createdCR.Annotations[config.WaveConfigAnnotationApplicationName])
		// Application name set in CR spec
		assert.Equal(tt, expectedName, createdCR.Spec.ApplicationName)
	}

	t.Run("whenNameFromAnnotation", func(tt *testing.T) {
		name := "app-name-from-annotation"
		pod := getTestPod("test-ns", "test-driver", "123-456", DriverRole, sparkAppID, false)
		pod.Annotations = map[string]string{
			config.WaveConfigAnnotationApplicationName: name,
		}
		pod.Labels[sparkOperatorAppNameLabel] = "whatever - shouldn't be used"
		testFunc(tt, pod, name, true)
	})

	t.Run("whenNameFromSparkOperatorCR", func(tt *testing.T) {
		name := "spark-operator-app-name"
		pod := getTestPod("test-ns", "test-driver", "123-456", DriverRole, sparkAppID, false)
		pod.Labels[sparkOperatorAppNameLabel] = name
		testFunc(tt, pod, name, true)
	})

	t.Run("whenNameFromSparkAPI", func(tt *testing.T) {
		name := getTestApplicationInfo().ApplicationName
		pod := getTestPod("test-ns", "test-driver", "123-456", DriverRole, sparkAppID, false)
		testFunc(tt, pod, name, true)
	})

	t.Run("whenNameFromDriverPod", func(tt *testing.T) {
		name := "driver-pod-name"
		pod := getTestPod("test-ns", name, "123-456", DriverRole, sparkAppID, false)
		testFunc(tt, pod, name, false)
	})

}

func TestReconcile_executor_whenSuccessful(t *testing.T) {
	ctx := context.TODO()

	applicationID := "spark-123"
	ns := "test-ns"

	exec1 := getTestPod(ns, "exec1", "123890", ExecutorRole, applicationID, false)
	exec1.Finalizers = []string{sparkApplicationFinalizerName}
	exec2 := getTestPod(ns, "exec2", "456789", ExecutorRole, applicationID, false)
	exec2.Finalizers = []string{sparkApplicationFinalizerName}

	cr := getMinimalTestCR(ns, applicationID)

	ctrlClient := ctrlrt_fake.NewFakeClientWithScheme(testScheme, exec1, exec2, cr)
	clientSet := k8sfake.NewSimpleClientset()

	controller := NewSparkPodReconciler(ctrlClient, clientSet, nil, getTestLogger(), testScheme)

	// Executor 1

	req := ctrlrt.Request{
		NamespacedName: types.NamespacedName{Namespace: exec1.Namespace, Name: exec1.Name},
	}
	_, err := controller.Reconcile(ctx, req)
	require.NoError(t, err)

	patchedCR := &v1alpha1.SparkApplication{}
	err = ctrlClient.Get(ctx, client.ObjectKey{Name: applicationID, Namespace: ns}, patchedCR)
	require.NoError(t, err)
	assert.Equal(t, 1, len(patchedCR.Status.Data.Executors))
	executor := patchedCR.Status.Data.Executors[0]
	verifyCRPod(t, exec1, executor)

	// Executor 2

	req = ctrlrt.Request{
		NamespacedName: types.NamespacedName{Namespace: exec2.Namespace, Name: exec2.Name},
	}
	_, err = controller.Reconcile(ctx, req)
	require.NoError(t, err)

	patchedCR = &v1alpha1.SparkApplication{}
	err = ctrlClient.Get(ctx, client.ObjectKey{Name: applicationID, Namespace: ns}, patchedCR)
	require.NoError(t, err)
	assert.Equal(t, 2, len(patchedCR.Status.Data.Executors))
	executor = patchedCR.Status.Data.Executors[1]
	verifyCRPod(t, exec2, executor)

	// Update executor 1

	exec1.Status.Phase = corev1.PodSucceeded
	deletionTimestamp := metav1.Now()
	exec1.DeletionTimestamp = &deletionTimestamp
	err = ctrlClient.Update(ctx, exec1, &client.UpdateOptions{})
	require.NoError(t, err)

	req = ctrlrt.Request{
		NamespacedName: types.NamespacedName{Namespace: exec1.Namespace, Name: exec1.Name},
	}
	_, err = controller.Reconcile(ctx, req)
	require.NoError(t, err)

	// Fetch the updated pod so timestamps match
	updatedExec1 := &corev1.Pod{}
	err = ctrlClient.Get(ctx, client.ObjectKey{Namespace: exec1.Namespace, Name: exec1.Name}, updatedExec1)
	require.NoError(t, err)
	assert.False(t, updatedExec1.DeletionTimestamp.IsZero())

	patchedCR = &v1alpha1.SparkApplication{}
	err = ctrlClient.Get(ctx, client.ObjectKey{Name: applicationID, Namespace: ns}, patchedCR)
	require.NoError(t, err)
	assert.Equal(t, 2, len(patchedCR.Status.Data.Executors))
	executor = patchedCR.Status.Data.Executors[0]
	verifyCRPod(t, updatedExec1, executor)
}

func TestReconcile_executor_whenCRDoesntExist(t *testing.T) {
	ctx := context.TODO()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-executor",
			Namespace: "test-ns",
			Labels: map[string]string{
				SparkAppLabel:  "spark-123456",
				SparkRoleLabel: ExecutorRole,
			},
			Finalizers: []string{
				sparkApplicationFinalizerName,
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
	assert.Contains(t, err.Error(), "spark application cr not found")
}

func TestReconcile_finalizer_add(t *testing.T) {
	ctx := context.TODO()

	applicationID := "spark-123"
	ns := "test-ns"

	pod := getTestPod(ns, "exec1", "123890", ExecutorRole, applicationID, false)

	cr := getMinimalTestCR(ns, applicationID)

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

	applicationID := "spark-123"
	ns := "test-ns"

	pod := getTestPod(ns, "exec1", "123890", ExecutorRole, applicationID, true)
	pod.Finalizers = []string{sparkApplicationFinalizerName}

	cr := getMinimalTestCR(ns, applicationID)

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

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns := "test-ns"
	sparkAppID := "spark-123456"
	cr := getMinimalTestCR(ns, sparkAppID)
	controller := false
	blockOwnerDeletion := true
	crOwnerRef := metav1.OwnerReference{
		APIVersion:         apiVersion,
		Kind:               sparkApplicationKind,
		Name:               cr.Name,
		UID:                cr.UID,
		Controller:         &controller,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}
	otherOwnerRef1 := metav1.OwnerReference{
		APIVersion:         "sparkoperator",
		Kind:               "sparkoperator.sparkapplication",
		Name:               "my-spark-operator-application",
		UID:                "4132-2345-63546-1324",
		Controller:         &controller,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}
	otherOwnerRef2 := metav1.OwnerReference{
		APIVersion:         "some-api",
		Kind:               "some-kind",
		Name:               "some-name",
		UID:                "xxxx-yyyy-xxxx-yyyy",
		Controller:         &controller,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}

	type testCase struct {
		existingOwnerRefs      []metav1.OwnerReference
		ownerRefAlreadyPresent bool
		message                string
	}

	testCases := []testCase{
		{
			existingOwnerRefs:      nil,
			ownerRefAlreadyPresent: false,
			message:                "when owner refs nil",
		},
		{
			existingOwnerRefs:      make([]metav1.OwnerReference, 0),
			ownerRefAlreadyPresent: false,
			message:                "when owner refs empty",
		},
		{
			existingOwnerRefs: []metav1.OwnerReference{
				crOwnerRef,
			},
			ownerRefAlreadyPresent: true,
			message:                "when owner ref already present",
		},
		{
			existingOwnerRefs: []metav1.OwnerReference{
				otherOwnerRef1,
			},
			ownerRefAlreadyPresent: false,
			message:                "when existing different owner ref",
		},
		{
			existingOwnerRefs: []metav1.OwnerReference{
				otherOwnerRef1,
				otherOwnerRef2,
			},
			ownerRefAlreadyPresent: false,
			message:                "when existing multiple different owner ref",
		},
		{
			existingOwnerRefs: []metav1.OwnerReference{
				crOwnerRef,
				otherOwnerRef1,
			},
			ownerRefAlreadyPresent: true,
			message:                "when already exists front of list",
		},
		{
			existingOwnerRefs: []metav1.OwnerReference{
				crOwnerRef,
				otherOwnerRef1,
				otherOwnerRef2,
			},
			ownerRefAlreadyPresent: true,
			message:                "when already exists front of list - multiple",
		},
		{
			existingOwnerRefs: []metav1.OwnerReference{
				otherOwnerRef1,
				crOwnerRef,
			},
			ownerRefAlreadyPresent: true,
			message:                "when already exists not front of list",
		},
		{
			existingOwnerRefs: []metav1.OwnerReference{
				otherOwnerRef1,
				otherOwnerRef2,
				crOwnerRef,
			},
			ownerRefAlreadyPresent: true,
			message:                "when already exists not front of list - multiple 1",
		},
		{
			existingOwnerRefs: []metav1.OwnerReference{
				otherOwnerRef1,
				crOwnerRef,
				otherOwnerRef2,
			},
			ownerRefAlreadyPresent: true,
			message:                "when already exists not front of list - multiple 2",
		},
	}

	t.Run("testOwnerReferenceAdd", func(tt *testing.T) {

		for _, tc := range testCases {

			ctx := context.TODO()

			pod := getTestPod(ns, "test-driver", "123-456", DriverRole, sparkAppID, false)
			pod.Finalizers = []string{sparkApplicationFinalizerName}

			if tc.existingOwnerRefs != nil {
				pod.OwnerReferences = tc.existingOwnerRefs
			}

			ctrlClient := ctrlrt_fake.NewFakeClientWithScheme(testScheme, pod, cr)
			clientSet := k8sfake.NewSimpleClientset()

			// Mock Spark API manager
			m := mock_sparkapi.NewMockManager(ctrl)
			m.EXPECT().GetApplicationInfo(sparkAppID).Return(getTestApplicationInfo(), nil).AnyTimes()

			var getMockSparkApiManager SparkApiManagerGetter = func(clientSet kubernetes.Interface, driverPod *corev1.Pod, logger logr.Logger) (sparkapi.Manager, error) {
				return m, nil
			}

			controller := NewSparkPodReconciler(ctrlClient, clientSet, getMockSparkApiManager, getTestLogger(), testScheme)

			req := ctrlrt.Request{
				NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
			}

			_, err := controller.Reconcile(ctx, req)
			assert.NoError(t, err, tc.message)

			fetchedCR := &v1alpha1.SparkApplication{}
			err = ctrlClient.Get(ctx, client.ObjectKey{Name: sparkAppID, Namespace: pod.Namespace}, fetchedCR)
			require.NoError(t, err, tc.message)

			updatedPod := &corev1.Pod{}
			err = ctrlClient.Get(ctx, client.ObjectKey{Name: pod.Name, Namespace: pod.Namespace}, updatedPod)
			require.NoError(t, err, tc.message)

			if tc.ownerRefAlreadyPresent {
				assert.Equal(t, len(tc.existingOwnerRefs), len(updatedPod.OwnerReferences), tc.message)
			} else {
				assert.Equal(t, len(tc.existingOwnerRefs)+1, len(updatedPod.OwnerReferences), tc.message)
			}

			// The CR owner ref should be at the front of the list
			assert.Equal(t, fetchedCR.UID, updatedPod.OwnerReferences[0].UID, tc.message)
			assert.Equal(t, fetchedCR.Name, updatedPod.OwnerReferences[0].Name, tc.message)
			assert.Equal(t, apiVersion, updatedPod.OwnerReferences[0].APIVersion, tc.message)
			assert.Equal(t, sparkApplicationKind, updatedPod.OwnerReferences[0].Kind, tc.message)
			assert.Equal(t, true, *updatedPod.OwnerReferences[0].BlockOwnerDeletion, tc.message)
			assert.Equal(t, false, *updatedPod.OwnerReferences[0].Controller, tc.message)

			// All existing owner refs should still be there, with no duplicates
			for _, existingOwnerRef := range tc.existingOwnerRefs {
				found := 0
				for _, podOwnerRef := range updatedPod.OwnerReferences {
					if existingOwnerRef.UID == podOwnerRef.UID &&
						existingOwnerRef.Name == podOwnerRef.Name &&
						existingOwnerRef.Kind == podOwnerRef.Kind &&
						existingOwnerRef.APIVersion == podOwnerRef.APIVersion &&
						*existingOwnerRef.Controller == *podOwnerRef.Controller &&
						*existingOwnerRef.BlockOwnerDeletion == *podOwnerRef.BlockOwnerDeletion {
						found++
					}
				}
				assert.Equal(t, 1, found, tc.message)
			}
		}
	})

}

func TestPodStateHistory(t *testing.T) {

	name := "my-pod"
	ns := "test-ns"
	logger := getTestLogger()
	exitCode0 := int32(0)
	exitCode1 := int32(1)

	type testCase struct {
		currentStateHistory  []v1alpha1.PodStateHistoryEntry
		expectedStateHistory []v1alpha1.PodStateHistoryEntry
		phase                corev1.PodPhase
		containerStatuses    []corev1.ContainerStatus
		podCRExists          bool
		message              string
	}

	testCases := []testCase{
		{
			currentStateHistory: nil,
			expectedStateHistory: []v1alpha1.PodStateHistoryEntry{
				{
					Timestamp: metav1.Now(),
					Phase:     corev1.PodPending,
					ContainerStatuses: map[string]v1alpha1.PodStateHistoryContainerStatus{
						"driver-container": {
							State:    v1alpha1.ContainerStateWaiting,
							ExitCode: nil,
						},
					},
				},
			},
			phase: corev1.PodPending,
			containerStatuses: []corev1.ContainerStatus{
				{
					Name: "driver-container",
					State: corev1.ContainerState{
						Waiting:    &corev1.ContainerStateWaiting{},
						Running:    nil,
						Terminated: nil,
					},
				},
			},
			podCRExists: false,
			message:     "whenPodCRDoesntExist",
		},
		{
			currentStateHistory: nil,
			expectedStateHistory: []v1alpha1.PodStateHistoryEntry{
				{
					Timestamp: metav1.Now(),
					Phase:     corev1.PodPending,
					ContainerStatuses: map[string]v1alpha1.PodStateHistoryContainerStatus{
						"driver-container": {
							State:    v1alpha1.ContainerStateWaiting,
							ExitCode: nil,
						},
					},
				},
			},
			phase: corev1.PodPending,
			containerStatuses: []corev1.ContainerStatus{
				{
					Name: "driver-container",
					State: corev1.ContainerState{
						Waiting:    &corev1.ContainerStateWaiting{},
						Running:    nil,
						Terminated: nil,
					},
				},
			},
			podCRExists: true,
			message:     "whenNilStateHistory_pending",
		},
		{
			currentStateHistory: []v1alpha1.PodStateHistoryEntry{},
			expectedStateHistory: []v1alpha1.PodStateHistoryEntry{
				{
					Timestamp: metav1.Now(),
					Phase:     corev1.PodFailed,
					ContainerStatuses: map[string]v1alpha1.PodStateHistoryContainerStatus{
						"executor-one": {
							State:    v1alpha1.ContainerStateTerminated,
							ExitCode: &exitCode1,
						},
					},
				},
			},
			phase: corev1.PodFailed,
			containerStatuses: []corev1.ContainerStatus{
				{
					Name: "executor-one",
					State: corev1.ContainerState{
						Waiting: nil,
						Running: nil,
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 1,
						},
					},
				},
			},
			podCRExists: true,
			message:     "whenEmptyStateHistory_terminated",
		},
		{
			currentStateHistory: []v1alpha1.PodStateHistoryEntry{
				{
					Timestamp: metav1.Now(),
					Phase:     corev1.PodFailed,
					ContainerStatuses: map[string]v1alpha1.PodStateHistoryContainerStatus{
						"executor-one": {
							State:    v1alpha1.ContainerStateTerminated,
							ExitCode: &exitCode1,
						},
					},
				},
			},
			expectedStateHistory: []v1alpha1.PodStateHistoryEntry{
				{
					Timestamp: metav1.Now(),
					Phase:     corev1.PodFailed,
					ContainerStatuses: map[string]v1alpha1.PodStateHistoryContainerStatus{
						"executor-one": {
							State:    v1alpha1.ContainerStateTerminated,
							ExitCode: &exitCode1,
						},
					},
				},
			},
			phase: corev1.PodFailed,
			containerStatuses: []corev1.ContainerStatus{
				{
					Name: "executor-one",
					State: corev1.ContainerState{
						Waiting: nil,
						Running: nil,
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 1,
						},
					},
				},
			},
			podCRExists: true,
			message:     "whenExistingStateHistory_noUpdate_1",
		},
		{
			currentStateHistory: []v1alpha1.PodStateHistoryEntry{
				{
					Timestamp: metav1.Now(),
					Phase:     corev1.PodPending,
					ContainerStatuses: map[string]v1alpha1.PodStateHistoryContainerStatus{
						"my-driver": {
							State:    v1alpha1.ContainerStateWaiting,
							ExitCode: nil,
						},
					},
				},
				{
					Timestamp: metav1.Now(),
					Phase:     corev1.PodRunning,
					ContainerStatuses: map[string]v1alpha1.PodStateHistoryContainerStatus{
						"my-driver": {
							State:    v1alpha1.ContainerStateRunning,
							ExitCode: nil,
						},
						"my-storage-sync": {
							State:    v1alpha1.ContainerStateWaiting,
							ExitCode: nil,
						},
					},
				},
			},
			expectedStateHistory: []v1alpha1.PodStateHistoryEntry{
				{
					Timestamp: metav1.Now(),
					Phase:     corev1.PodPending,
					ContainerStatuses: map[string]v1alpha1.PodStateHistoryContainerStatus{
						"my-driver": {
							State:    v1alpha1.ContainerStateWaiting,
							ExitCode: nil,
						},
					},
				},
				{
					Timestamp: metav1.Now(),
					Phase:     corev1.PodRunning,
					ContainerStatuses: map[string]v1alpha1.PodStateHistoryContainerStatus{
						"my-driver": {
							State:    v1alpha1.ContainerStateRunning,
							ExitCode: nil,
						},
						"my-storage-sync": {
							State:    v1alpha1.ContainerStateWaiting,
							ExitCode: nil,
						},
					},
				},
			},
			phase: corev1.PodRunning,
			containerStatuses: []corev1.ContainerStatus{
				{
					Name: "my-driver",
					State: corev1.ContainerState{
						Waiting:    nil,
						Running:    &corev1.ContainerStateRunning{},
						Terminated: nil,
					},
				},
				{
					Name: "my-storage-sync",
					State: corev1.ContainerState{
						Waiting:    &corev1.ContainerStateWaiting{},
						Running:    nil,
						Terminated: nil,
					},
				},
			},
			podCRExists: true,
			message:     "whenExistingStateHistory_noUpdate_2",
		},
		{
			currentStateHistory: []v1alpha1.PodStateHistoryEntry{
				{
					Timestamp: metav1.Now(),
					Phase:     corev1.PodPending,
					ContainerStatuses: map[string]v1alpha1.PodStateHistoryContainerStatus{
						"my-driver": {
							State:    v1alpha1.ContainerStateWaiting,
							ExitCode: nil,
						},
					},
				},
				{
					Timestamp: metav1.Now(),
					Phase:     corev1.PodRunning,
					ContainerStatuses: map[string]v1alpha1.PodStateHistoryContainerStatus{
						"my-driver": {
							State:    v1alpha1.ContainerStateRunning,
							ExitCode: nil,
						},
					},
				},
			},
			expectedStateHistory: []v1alpha1.PodStateHistoryEntry{
				{
					Timestamp: metav1.Now(),
					Phase:     corev1.PodPending,
					ContainerStatuses: map[string]v1alpha1.PodStateHistoryContainerStatus{
						"my-driver": {
							State:    v1alpha1.ContainerStateWaiting,
							ExitCode: nil,
						},
					},
				},
				{
					Timestamp: metav1.Now(),
					Phase:     corev1.PodRunning,
					ContainerStatuses: map[string]v1alpha1.PodStateHistoryContainerStatus{
						"my-driver": {
							State:    v1alpha1.ContainerStateRunning,
							ExitCode: nil,
						},
					},
				},
				{
					Timestamp: metav1.Now(),
					Phase:     corev1.PodFailed,
					ContainerStatuses: map[string]v1alpha1.PodStateHistoryContainerStatus{
						"my-driver": {
							State:    v1alpha1.ContainerStateTerminated,
							ExitCode: &exitCode1,
						},
					},
				},
			},
			phase: corev1.PodFailed,
			containerStatuses: []corev1.ContainerStatus{
				{
					Name: "my-driver",
					State: corev1.ContainerState{
						Waiting: nil,
						Running: nil,
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 1,
						},
					},
				},
			},
			podCRExists: true,
			message:     "whenExistingStateHistory_shouldUpdate",
		},
		{
			currentStateHistory: []v1alpha1.PodStateHistoryEntry{
				{
					Timestamp: metav1.Time{},
					Phase:     corev1.PodRunning,
					ContainerStatuses: map[string]v1alpha1.PodStateHistoryContainerStatus{
						"driver-container": {
							State:    v1alpha1.ContainerStateRunning,
							ExitCode: nil,
						},
						"storage-sync-container": {
							State:    v1alpha1.ContainerStateWaiting,
							ExitCode: nil,
						},
					},
				},
			},
			expectedStateHistory: []v1alpha1.PodStateHistoryEntry{
				{
					Timestamp: metav1.Time{},
					Phase:     corev1.PodRunning,
					ContainerStatuses: map[string]v1alpha1.PodStateHistoryContainerStatus{
						"driver-container": {
							State:    v1alpha1.ContainerStateRunning,
							ExitCode: nil,
						},
						"storage-sync-container": {
							State:    v1alpha1.ContainerStateWaiting,
							ExitCode: nil,
						},
					},
				},
				{
					Timestamp: metav1.Time{},
					Phase:     corev1.PodFailed,
					ContainerStatuses: map[string]v1alpha1.PodStateHistoryContainerStatus{
						"driver-container": {
							State:    v1alpha1.ContainerStateTerminated,
							ExitCode: &exitCode1,
						},
						"storage-sync-container": {
							State:    v1alpha1.ContainerStateTerminated,
							ExitCode: &exitCode0,
						},
					},
				},
			},
			phase: corev1.PodFailed,
			containerStatuses: []corev1.ContainerStatus{
				{
					Name: "driver-container",
					State: corev1.ContainerState{
						Waiting: nil,
						Running: nil,
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 1,
						},
					},
				},
				{
					Name: "storage-sync-container",
					State: corev1.ContainerState{
						Waiting: nil,
						Running: nil,
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 0,
						},
					},
				},
			},
			podCRExists: true,
			message:     "whenMultipleContainers_stateUpdate",
		},
	}

	t.Run("whenSuccessful", func(tt *testing.T) {
		for _, tc := range testCases {

			pod := getTestPod(ns, name, "123890", DriverRole, "id", false)
			pod.Status.Phase = tc.phase
			pod.Status.ContainerStatuses = tc.containerStatuses

			var podCR *v1alpha1.Pod
			if tc.podCRExists {
				podCR = &v1alpha1.Pod{StateHistory: tc.currentStateHistory}
			}

			res := newPodCR(pod, podCR, logger)

			assert.Equal(tt, tc.phase, res.Phase, tc.message)
			assert.Equal(tt, tc.containerStatuses, res.Statuses, tc.message)

			assert.Equal(tt, len(tc.expectedStateHistory), len(res.StateHistory), tc.message)
			for i := 0; i < len(tc.expectedStateHistory); i++ {
				assert.Equal(tt, tc.expectedStateHistory[i].Phase, res.StateHistory[i].Phase, tc.message)
				assert.Equal(tt, tc.expectedStateHistory[i].ContainerStatuses, res.StateHistory[i].ContainerStatuses, tc.message)
			}
		}
	})
}

func getTestLogger() logr.Logger {
	return zap.New(zap.UseDevMode(true))
}

func verifyCRPod(t *testing.T, pod *corev1.Pod, crPod v1alpha1.Pod) {
	assert.Equal(t, pod.Name, crPod.Name)
	assert.Equal(t, pod.Namespace, crPod.Namespace)
	assert.Equal(t, string(pod.UID), crPod.UID)
	assert.Equal(t, pod.Status.Phase, crPod.Phase)
	assert.Equal(t, pod.CreationTimestamp, crPod.CreationTimestamp)
	assert.Equal(t, pod.DeletionTimestamp, crPod.DeletionTimestamp)

	assert.Equal(t, len(pod.Labels), len(crPod.Labels))
	for k, v := range pod.Labels {
		assert.Equal(t, crPod.Labels[k], v)
	}

	assert.Equal(t, len(pod.Status.ContainerStatuses), len(crPod.Statuses))
	assert.True(t, len(crPod.StateHistory) > 0)

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

func verifyCRExecutors(t *testing.T, executors []sparkapiclient.Executor, crExecutors []v1alpha1.Executor) {
	assert.Equal(t, len(executors), len(crExecutors))

	for _, expectedExecutor := range executors {
		foundExecutor := false
		for _, actualExecutor := range crExecutors {
			foundExecutor = actualExecutor.ID == expectedExecutor.ID &&
				actualExecutor.AddTime == expectedExecutor.AddTime &&
				actualExecutor.IsActive == expectedExecutor.IsActive &&
				actualExecutor.RemoveTime == expectedExecutor.RemoveTime &&
				actualExecutor.FailedTasks == expectedExecutor.FailedTasks
			if foundExecutor {
				break
			}
		}
		assert.True(t, foundExecutor)
	}
}

func verifyCRAttempts(t *testing.T, attempts []sparkapiclient.Attempt, crAttempts []v1alpha1.Attempt) {
	assert.Equal(t, len(attempts), len(crAttempts))

	for _, expectedAttempt := range attempts {
		foundAttempt := false
		for _, actualAttempt := range crAttempts {
			foundAttempt = actualAttempt.Completed == expectedAttempt.Completed &&
				actualAttempt.StartTimeEpoch == expectedAttempt.StartTimeEpoch &&
				actualAttempt.EndTimeEpoch == expectedAttempt.EndTimeEpoch &&
				actualAttempt.LastUpdatedEpoch == expectedAttempt.LastUpdatedEpoch &&
				actualAttempt.AppSparkVersion == expectedAttempt.AppSparkVersion
			if foundAttempt {
				break
			}
		}
		assert.True(t, foundAttempt)
	}
}

func getMinimalTestCR(namespace string, applicationID string) *v1alpha1.SparkApplication {
	return &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      applicationID,
			Namespace: namespace,
			UID:       "123-456-999-1234",
		},
		Spec: v1alpha1.SparkApplicationSpec{
			ApplicationID:   applicationID,
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

func getTestPod(namespace string, name string, uid string, role string, applicationID string, deleted bool) *corev1.Pod {
	var deletionTimestamp *metav1.Time
	if deleted {
		ts := metav1.Unix(int64(1234), int64(1000))
		deletionTimestamp = &ts
	}
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         namespace,
			UID:               types.UID(uid),
			CreationTimestamp: metav1.Unix(int64(1000), 0),
			DeletionTimestamp: deletionTimestamp,
			Labels: map[string]string{
				SparkAppLabel:   applicationID,
				SparkRoleLabel:  role,
				"my-test-label": "some-value",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "spark-kubernetes-driver",
					Image: "doesnt-matter",
				},
			},
		},
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

func getTestApplicationInfo() *sparkapi.ApplicationInfo {
	return &sparkapi.ApplicationInfo{
		ApplicationName: "The application name",
		SparkProperties: map[string]string{
			"prop1": "val1",
			"prop2": "val2",
		},
		TotalNewInputBytes:      987,
		TotalNewOutputBytes:     765,
		TotalNewExecutorCpuTime: 543,
		Attempts: []sparkapiclient.Attempt{
			{
				StartTimeEpoch:   3528,
				EndTimeEpoch:     5146,
				LastUpdatedEpoch: 5684,
				Duration:         4563,
				SparkUser:        "the spark user",
				Completed:        false,
				AppSparkVersion:  "v3.0.0",
			},
			{
				StartTimeEpoch:   9213,
				EndTimeEpoch:     4672,
				LastUpdatedEpoch: 9435,
				Duration:         5678,
				SparkUser:        "the second spark user",
				Completed:        true,
				AppSparkVersion:  "v7.0.0",
			},
		},
		Executors: []sparkapiclient.Executor{
			{
				ID:       "driver",
				AddTime:  "2020-12-14T14:07:27.142GMT",
				IsActive: true,
			},
			{
				ID:          "1",
				AddTime:     "2020-12-14T15:17:37.142GMT",
				RemoveTime:  "2021-12-14T15:17:37.142GMT",
				FailedTasks: 9999,
				IsActive:    false,
			},
			{
				ID:          "2",
				AddTime:     "2020-12-14T16:27:47.142GMT",
				FailedTasks: 90,
				IsActive:    true,
			},
		},
		WorkloadType: "my-workload-type",
	}
}
