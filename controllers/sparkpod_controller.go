package controllers

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/internal/sparkapi"
)

const (
	SparkRoleLabel = "spark-role"
	SparkAppLabel  = "spark-app-selector"
	DriverRole     = "driver"
	ExecutorRole   = "executor"

	AppLabel                       = "app"
	AppLabelValueEnterpriseGateway = "enterprise-gateway"
	SparkOperatorLaunchedByLabel   = "sparkoperator.k8s.io/launched-by-spark-operator"

	sparkApplicationFinalizerName = OperatorFinalizerName + "/sparkapplication"

	apiVersion           = "wave.spot.io/v1alpha1"
	sparkApplicationKind = "SparkApplication"

	requeueAfterTimeout = 10 * time.Second
)

// SparkPodReconciler reconciles Pod objects to discover Spark applications
type SparkPodReconciler struct {
	client.Client
	ClientSet kubernetes.Interface
	Log       logr.Logger
	Scheme    *runtime.Scheme
}

func NewSparkPodReconciler(
	client client.Client,
	clientSet kubernetes.Interface,
	log logr.Logger,
	scheme *runtime.Scheme) *SparkPodReconciler {

	return &SparkPodReconciler{
		Client:    client,
		ClientSet: clientSet,
		Log:       log,
		Scheme:    scheme,
	}
}

func (r *SparkPodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("pod", req.NamespacedName)

	p := &corev1.Pod{}
	err := r.Get(ctx, req.NamespacedName, p)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err, "cannot get pod")
		}
		return ctrl.Result{}, nil
	}

	sparkApplicationId, ok := p.Labels[SparkAppLabel]
	if !ok {
		// This is not a Spark application pod, ignore
		return ctrl.Result{}, nil
	}

	if sparkApplicationId == "" {
		err := fmt.Errorf("spark application ID label value missing")
		log.Error(err, "error handling spark pod")
		return ctrl.Result{}, nil // Just log error
	}

	sparkRole := p.Labels[SparkRoleLabel]
	if !(sparkRole == DriverRole || sparkRole == ExecutorRole) {
		err := fmt.Errorf("unknown spark role: %q", sparkRole)
		log.Error(err, "error handling spark pod")
		return ctrl.Result{}, nil // Just log error
	}

	// Set finalizer
	if p.ObjectMeta.DeletionTimestamp.IsZero() {
		if !containsString(p.ObjectMeta.Finalizers, sparkApplicationFinalizerName) {
			deepCopy := p.DeepCopy()
			deepCopy.ObjectMeta.Finalizers = append(deepCopy.ObjectMeta.Finalizers, sparkApplicationFinalizerName)
			log.Info("Adding finalizer")
			err := r.Client.Patch(ctx, deepCopy, client.MergeFrom(p))
			if err != nil {
				log.Error(err, "could not set finalizer")
				return ctrl.Result{}, err
			}
		}
	}

	shouldRequeue := false
	switch sparkRole {
	case DriverRole:
		shouldRequeue, err = r.handleDriverPod(ctx, sparkApplicationId, p)
		if err != nil {
			log.Error(err, "error handling driver pod")
			return ctrl.Result{}, err
		}
	case ExecutorRole:
		err = r.handleExecutorPod(ctx, sparkApplicationId, p)
		if err != nil {
			log.Error(err, "error handling executor pod")
			return ctrl.Result{}, err
		}
	default:
		// Just log error
		err := fmt.Errorf("unknown spark role: %q", sparkRole)
		log.Error(err, "error handling spark pod")
	}

	// Remove finalizer
	if !p.ObjectMeta.DeletionTimestamp.IsZero() && !shouldRequeue {
		if containsString(p.ObjectMeta.Finalizers, sparkApplicationFinalizerName) {
			deepCopy := p.DeepCopy()
			deepCopy.ObjectMeta.Finalizers = removeString(deepCopy.ObjectMeta.Finalizers, sparkApplicationFinalizerName)
			log.Info("Removing finalizer")
			err = r.Client.Patch(ctx, deepCopy, client.MergeFrom(p))
			if err != nil {
				log.Error(err, "could not remove finalizer")
				return ctrl.Result{}, err
			}
		}
	}

	if shouldRequeue {
		return ctrl.Result{
			Requeue:      true,
			RequeueAfter: requeueAfterTimeout,
		}, nil
	}

	return ctrl.Result{}, nil
}

func (r *SparkPodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Complete(r)
}

func (r *SparkPodReconciler) handleDriverPod(ctx context.Context, applicationId string, driverPod *corev1.Pod) (bool, error) {
	log := r.Log.WithValues("name", driverPod.Name, "namespace", driverPod.Namespace, "sparkApplicationId", applicationId)
	log.Info("Handling driver pod", "phase", driverPod.Status.Phase, "deleted", !driverPod.ObjectMeta.DeletionTimestamp.IsZero())

	// Get application CR if it exists, otherwise build new one, unless the pod is being garbage collected
	crExists := true
	cr, err := r.getSparkApplicationCr(ctx, driverPod.Namespace, applicationId)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			if !driverPod.DeletionTimestamp.IsZero() && hasWaveSparkApplicationOwnerRef(driverPod, applicationId) {
				// The Wave Spark application CR does not exist, the pod has an owner reference to it,
				// and the pod is being deleted
				// -> assume this is a garbage collection event and don't re-create the CR
				log.Info("Ignoring garbage collection event")
				return false, nil
			}
			crExists = false
			cr = &v1alpha1.SparkApplication{}
		} else {
			return false, fmt.Errorf("could not get spark application cr, %w", err)
		}
	}

	deepCopy := cr.DeepCopy()

	deepCopy.Name = applicationId
	deepCopy.Namespace = driverPod.Namespace
	deepCopy.Spec.ApplicationId = applicationId

	heritage, err := getHeritage(driverPod)
	if err != nil {
		return false, fmt.Errorf("could not get heritage, %w", err)
	}
	deepCopy.Spec.Heritage = heritage

	driverPodCr := newPodCR(driverPod)
	deepCopy.Status.Data.Driver = driverPodCr

	sparkApiSuccess := false
	sparkApiApplicationInfo, err := getSparkApiApplicationInfo(r.ClientSet, driverPod, applicationId, log)
	if err != nil {
		// Best effort, just log error
		log.Error(err, "could not get spark api application information")
	} else {
		sparkApiSuccess = true
		mapSparkApplicationInfo(deepCopy, sparkApiApplicationInfo)
	}

	// We always need an application name, set it to driver pod name if it isn't set already
	if deepCopy.Spec.ApplicationName == "" {
		deepCopy.Spec.ApplicationName = driverPod.Name
	}

	if crExists {
		err := r.Client.Patch(ctx, deepCopy, client.MergeFrom(cr))
		if err != nil {
			return false, fmt.Errorf("patch error, %w", err)
		}
	} else {
		err := r.createSparkApplicationCr(ctx, deepCopy)
		if err != nil {
			return false, fmt.Errorf("could not create spark application cr, %w", err)
		}
	}

	// Set owner reference driver pod -> spark application CR if needed
	shouldSetOwnerReference := shouldSetOwnerReference(driverPod, heritage)
	if shouldSetOwnerReference {
		podOwnerReferenceChanged := false
		driverPodDeepCopy := driverPod.DeepCopy()

		if !crExists {
			// Get the newly created spark application CR
			createdCr, err := r.getSparkApplicationCr(ctx, driverPod.Namespace, applicationId)
			if err != nil {
				return false, fmt.Errorf("could not get newly created spark application cr, %w", err)
			}
			podOwnerReferenceChanged = setPodOwnerReference(driverPodDeepCopy, createdCr)
		} else {
			podOwnerReferenceChanged = setPodOwnerReference(driverPodDeepCopy, deepCopy)
		}

		if podOwnerReferenceChanged {
			log.Info("Patching driver pod with owner reference")
			err := r.Client.Patch(ctx, driverPodDeepCopy, client.MergeFrom(driverPod))
			if err != nil {
				return false, fmt.Errorf("patch pod error, %w", err)
			}
		}
	}

	shouldRequeue := false

	// Requeue if we were unsuccessful in communicating with Spark API,
	// or if the driver pod is still running
	if !sparkApiSuccess || driverPod.Status.Phase == corev1.PodRunning {
		shouldRequeue = true
	}

	log.Info("Finished handling driver pod", "requeue", shouldRequeue)
	return shouldRequeue, nil
}

func shouldSetOwnerReference(driverPod *corev1.Pod, heritage v1alpha1.SparkHeritage) bool {
	if !(heritage == v1alpha1.SparkHeritageSubmit || heritage == v1alpha1.SparkHeritageJupyter) {
		return false
	}
	if len(driverPod.OwnerReferences) != 0 {
		return false
	}
	return true
}

func setPodOwnerReference(pod *corev1.Pod, cr *v1alpha1.SparkApplication) bool {
	changed := false

	if cr.UID != "" && len(pod.OwnerReferences) == 0 {
		ownedByController := false // This controller is a pod controller, not a Spark Application CR controller
		blockOwnerDeletion := true // We still want the pod to be garbage collected immediately when the CR is deleted
		ownerRef := v1.OwnerReference{
			APIVersion:         cr.APIVersion,
			Kind:               cr.Kind,
			Name:               cr.Name,
			UID:                cr.UID,
			Controller:         &ownedByController,
			BlockOwnerDeletion: &blockOwnerDeletion,
		}

		if pod.OwnerReferences == nil {
			pod.OwnerReferences = make([]v1.OwnerReference, 0, 1)
		}

		pod.OwnerReferences = append(pod.OwnerReferences, ownerRef)
		changed = true
	}

	return changed
}

func hasWaveSparkApplicationOwnerRef(pod *corev1.Pod, applicationId string) bool {
	for _, ownerRef := range pod.OwnerReferences {
		if ownerRef.APIVersion == apiVersion &&
			ownerRef.Kind == sparkApplicationKind &&
			ownerRef.Name == applicationId {
			return true
		}
	}

	return false
}

func (r *SparkPodReconciler) handleExecutorPod(ctx context.Context, applicationId string, executorPod *corev1.Pod) error {
	log := r.Log.WithValues("name", executorPod.Name, "namespace", executorPod.Namespace, "sparkApplicationId", applicationId)
	log.Info("Handling executor pod", "phase", executorPod.Status.Phase, "deleted", !executorPod.ObjectMeta.DeletionTimestamp.IsZero())

	cr, err := r.getSparkApplicationCr(ctx, executorPod.Namespace, applicationId)
	if err != nil {
		// CR should be created during driver reconciliation
		return fmt.Errorf("could not get spark application cr, %w", err)
	}

	deepCopy := cr.DeepCopy()

	// Do we already have an entry for this executor in the CR?
	foundIdx := -1
	for idx, executor := range deepCopy.Status.Data.Executors {
		if executor.UID == string(executorPod.UID) {
			foundIdx = idx
			break
		}
	}

	if foundIdx == -1 {
		// Create new executor entry
		newExecutor := newPodCR(executorPod)
		newExecutors := append(deepCopy.Status.Data.Executors, newExecutor)
		deepCopy.Status.Data.Executors = newExecutors
	} else {
		// Update existing executor entry
		updatedExecutor := newPodCR(executorPod)
		deepCopy.Status.Data.Executors[foundIdx] = updatedExecutor
	}

	err = r.Client.Patch(ctx, deepCopy, client.MergeFrom(cr))
	if err != nil {
		return fmt.Errorf("patch error, %w", err)
	}

	log.Info("Finished handling executor pod")

	return nil
}

func newPodCR(pod *corev1.Pod) v1alpha1.Pod {
	podCr := v1alpha1.Pod{}

	podCr.UID = string(pod.UID)
	podCr.Namespace = pod.Namespace
	podCr.Name = pod.Name
	podCr.Phase = pod.Status.Phase
	podCr.Statuses = pod.Status.ContainerStatuses
	podCr.Deleted = !pod.DeletionTimestamp.IsZero()

	if podCr.Statuses == nil {
		podCr.Statuses = make([]corev1.ContainerStatus, 0)
	}

	return podCr
}

func getSparkApiApplicationInfo(clientSet kubernetes.Interface, driverPod *corev1.Pod, applicationId string, logger logr.Logger) (*sparkapi.ApplicationInfo, error) {

	manager, err := sparkapi.NewManager(clientSet, driverPod, logger)
	if err != nil {
		return nil, fmt.Errorf("could not get spark api manager, %w", err)
	}

	applicationInfo, err := manager.GetApplicationInfo(applicationId)
	if err != nil {
		return nil, fmt.Errorf("could not get spark api application info, %w", err)
	}

	return applicationInfo, nil
}

func mapSparkApplicationInfo(deepCopy *v1alpha1.SparkApplication, sparkApiInfo *sparkapi.ApplicationInfo) {
	deepCopy.Spec.ApplicationName = sparkApiInfo.ApplicationName
	deepCopy.Status.Data.SparkProperties = sparkApiInfo.SparkProperties
	deepCopy.Status.Data.RunStatistics.TotalExecutorCpuTime = sparkApiInfo.TotalExecutorCpuTime
	deepCopy.Status.Data.RunStatistics.TotalInputBytes = sparkApiInfo.TotalInputBytes
	deepCopy.Status.Data.RunStatistics.TotalOutputBytes = sparkApiInfo.TotalOutputBytes

	attempts := make([]v1alpha1.Attempt, 0, len(sparkApiInfo.Attempts))
	for _, apiAttempt := range sparkApiInfo.Attempts {
		attempt := v1alpha1.Attempt{
			StartTimeEpoch:   apiAttempt.StartTimeEpoch,
			EndTimeEpoch:     apiAttempt.EndTimeEpoch,
			LastUpdatedEpoch: apiAttempt.LastUpdatedEpoch,
			Completed:        apiAttempt.Completed,
			AppSparkVersion:  apiAttempt.AppSparkVersion,
		}
		attempts = append(attempts, attempt)
	}

	deepCopy.Status.Data.RunStatistics.Attempts = attempts
}

func getHeritage(pod *corev1.Pod) (v1alpha1.SparkHeritage, error) {
	if pod.Labels[AppLabel] == AppLabelValueEnterpriseGateway {
		return v1alpha1.SparkHeritageJupyter, nil
	}

	if pod.Labels[SparkOperatorLaunchedByLabel] == "true" {
		return v1alpha1.SparkHeritageOperator, nil
	}

	// Make sure that this is a Spark pod before deciding it is spark-submit
	_, ok := pod.Labels[SparkAppLabel]
	if ok {
		return v1alpha1.SparkHeritageSubmit, nil
	}

	return "", fmt.Errorf("could not determine heritage")
}

func (r *SparkPodReconciler) getSparkApplicationCr(ctx context.Context, namespace string, applicationId string) (*v1alpha1.SparkApplication, error) {
	app := v1alpha1.SparkApplication{}
	err := r.Get(ctx, ctrlclient.ObjectKey{Name: applicationId, Namespace: namespace}, &app)
	if err != nil {
		return nil, err
	}

	return &app, nil
}

func (r *SparkPodReconciler) createSparkApplicationCr(ctx context.Context, application *v1alpha1.SparkApplication) error {

	if application.Status.Data.Executors == nil {
		application.Status.Data.Executors = make([]v1alpha1.Pod, 0)
	}

	if application.Status.Data.RunStatistics.Attempts == nil {
		application.Status.Data.RunStatistics.Attempts = make([]v1alpha1.Attempt, 0)
	}

	if application.Status.Data.SparkProperties == nil {
		application.Status.Data.SparkProperties = make(map[string]string)
	}

	err := r.Create(ctx, application)

	return err
}
