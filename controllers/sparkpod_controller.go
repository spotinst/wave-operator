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
	AppEnterpriseGatewayLabelValue = "enterprise-gateway"
	SparkOperatorLaunchedByLabel   = "sparkoperator.k8s.io/launched-by-spark-operator"

	sparkApplicationFinalizerName = OperatorFinalizerName + "/sparkapplication"

	apiVersion           = "wave.spot.io/v1alpha1"
	sparkApplicationKind = "SparkApplication"

	requeueAfterTimeout = 10 * time.Second
)

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=wave.spot.io,resources=sparkapplications,verbs=get;list;watch;create;update;patch;delete

// SparkPodReconciler reconciles Pod objects to discover Spark applications
type SparkPodReconciler struct {
	client.Client
	ClientSet          kubernetes.Interface
	getSparkApiManager SparkApiManagerGetter
	Log                logr.Logger
	Scheme             *runtime.Scheme
}

func NewSparkPodReconciler(
	client client.Client,
	clientSet kubernetes.Interface,
	sparkApiManagerGetter SparkApiManagerGetter,
	log logr.Logger,
	scheme *runtime.Scheme) *SparkPodReconciler {

	return &SparkPodReconciler{
		Client:             client,
		ClientSet:          clientSet,
		getSparkApiManager: sparkApiManagerGetter,
		Log:                log,
		Scheme:             scheme,
	}
}

// SparkApiManagerGetter is a factory function that returns an implementation of sparkapi.Manager
type SparkApiManagerGetter func(clientSet kubernetes.Interface, driverPod *corev1.Pod, logger logr.Logger) (sparkapi.Manager, error)

func (r *SparkPodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("pod", req.NamespacedName)

	p := &corev1.Pod{}
	err := r.Get(ctx, req.NamespacedName, p)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err, "cannot get pod")
		}
		return ctrl.Result{}, err
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

	log = r.Log.WithValues("name", p.Name, "namespace", p.Namespace, "sparkApplicationId", sparkApplicationId,
		"role", sparkRole, "phase", p.Status.Phase, "deleted", !p.ObjectMeta.DeletionTimestamp.IsZero())

	// Add finalizer if needed
	changed := addFinalizer(p)
	if changed {
		err := r.Client.Update(ctx, p)
		if err != nil {
			if k8serrors.IsConflict(err) {
				log.Info(fmt.Sprintf("could not add finalizer, conflict error: %s", err.Error()))
			} else {
				log.Error(err, "could not add finalizer")
			}
			return ctrl.Result{}, err
		}
		log.Info("Finalizer added")
		return ctrl.Result{Requeue: true}, nil
	}

	// Get or create Spark application CR
	cr, err := r.getSparkApplicationCr(ctx, p.Namespace, sparkApplicationId)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			if !p.DeletionTimestamp.IsZero() && hasWaveSparkApplicationOwnerRef(p, sparkApplicationId) {
				// The Wave Spark application CR does not exist but the pod has an owner reference to it,
				// and the pod is being deleted -> this is a garbage collection event
				changed = removeFinalizer(p)
				if changed {
					err := r.Client.Update(ctx, p)
					if err != nil {
						if k8serrors.IsConflict(err) {
							log.Info(fmt.Sprintf("could not remove finalizer, conflict error: %s", err.Error()))
						} else {
							log.Error(err, "could not remove finalizer")
						}
						return ctrl.Result{}, err
					}
					log.Info("Finalizer removed")
				}
				log.Info("Ignoring garbage collection event")
				return ctrl.Result{}, nil
			} else if sparkRole == DriverRole {
				err := r.createNewSparkApplicationCr(ctx, p, sparkApplicationId)
				if err != nil {
					log.Error(err, "could not create spark application cr")
					return ctrl.Result{}, err
				}
				return ctrl.Result{Requeue: true}, nil
			} else {
				log.Info("Spark application CR not found")
				err := fmt.Errorf("spark application cr not found")
				return ctrl.Result{}, err
			}
		} else {
			log.Error(err, "could not get spark application cr")
			return ctrl.Result{}, err
		}
	}

	switch sparkRole {
	case DriverRole:
		changed = setPodOwnerReference(p, cr)
		if changed {
			err := r.Client.Update(ctx, p)
			if err != nil {
				if k8serrors.IsConflict(err) {
					log.Info(fmt.Sprintf("could not set owner reference, conflict error: %s", err.Error()))
				} else {
					log.Error(err, "could not set owner reference")
				}
				return ctrl.Result{}, err
			}
			log.Info("Added owner reference")
			return ctrl.Result{Requeue: true}, nil
		}

		err := r.handleDriver(ctx, p, cr, log)
		if err != nil {
			// Check for expected Spark API communication errors
			if sparkapi.IsApplicationNotFoundError(err) ||
				sparkapi.IsServiceUnavailableError(err) {
				log.Info(fmt.Sprintf("Spark API error: %s", err.Error()))
			} else {
				log.Error(err, "error handling driver pod")
			}
			return ctrl.Result{}, err
		}

		// Requeue running drivers
		if p.Status.Phase == corev1.PodRunning {
			log.Info("Requeue running driver")
			return ctrl.Result{
				Requeue:      true,
				RequeueAfter: requeueAfterTimeout,
			}, nil
		}
	case ExecutorRole:
		err = r.handleExecutor(ctx, p, cr, log)
		if err != nil {
			log.Error(err, "error handling executor pod")
			return ctrl.Result{}, err
		}
	default:
		// Just log error
		err := fmt.Errorf("unknown spark role: %q", sparkRole)
		log.Error(err, "error handling spark pod")
	}

	// Remove finalizer if needed
	changed = removeFinalizer(p)
	if changed {
		err := r.Client.Update(ctx, p)
		if err != nil {
			if k8serrors.IsConflict(err) {
				log.Info(fmt.Sprintf("could not remove finalizer, conflict error: %s", err.Error()))
			} else {
				log.Error(err, "could not remove finalizer")
			}
			return ctrl.Result{}, err
		}
		log.Info("Finalizer removed")
	}

	return ctrl.Result{}, nil
}

func (r *SparkPodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Complete(r)
}

func (r *SparkPodReconciler) handleDriver(ctx context.Context, pod *corev1.Pod, cr *v1alpha1.SparkApplication, log logr.Logger) error {
	log.Info("Handling driver pod")

	deepCopy := cr.DeepCopy()

	driverPodCr := newPodCR(pod)
	deepCopy.Status.Data.Driver = driverPodCr

	// Fetch information from Spark API
	// Let's update the driver pod information even though the Spark API call fails

	var sparkApiError error
	sparkApiApplicationInfo, err := r.getSparkApiApplicationInfo(r.ClientSet, pod, cr.Spec.ApplicationId, log)
	if err != nil {
		sparkApiError = fmt.Errorf("could not get spark api application information, %w", err)
	} else {
		mapSparkApiApplicationInfo(deepCopy, sparkApiApplicationInfo)
	}

	// Make sure we have an application name
	if deepCopy.Spec.ApplicationName == "" {
		deepCopy.Spec.ApplicationName = pod.Name
	}

	err = r.Client.Patch(ctx, deepCopy, client.MergeFrom(cr))
	if err != nil {
		return fmt.Errorf("patch error, %w", err)
	}

	if sparkApiError != nil {
		return sparkApiError
	}

	return nil
}

// setPodOwnerReference adds an owner reference to the spark application cr to the front of the pod's owner reference list
// returns true if pod's owner references were updated, false otherwise
func setPodOwnerReference(pod *corev1.Pod, cr *v1alpha1.SparkApplication) bool {
	foundIdx := -1
	for idx, ownerRef := range pod.OwnerReferences {
		if ownerRef.APIVersion == apiVersion &&
			ownerRef.Kind == sparkApplicationKind &&
			ownerRef.Name == cr.Name &&
			ownerRef.UID == cr.UID {
			foundIdx = idx
			break
		}
	}

	if foundIdx == 0 {
		// Owner reference found at the front of the list
		return false
	} else if foundIdx == -1 {
		// Owner reference not found
		updatedOwnerRefs := make([]v1.OwnerReference, 0, len(pod.OwnerReferences)+1)
		newOwnerRef := newOwnerReference(cr)
		updatedOwnerRefs = append(updatedOwnerRefs, newOwnerRef)
		updatedOwnerRefs = append(updatedOwnerRefs, pod.OwnerReferences...)
		pod.OwnerReferences = updatedOwnerRefs
		return true
	} else {
		// Owner reference found but not at the front of the list, move it to the front
		pod.OwnerReferences[0], pod.OwnerReferences[foundIdx] = pod.OwnerReferences[foundIdx], pod.OwnerReferences[0]
		return true
	}
}

func newOwnerReference(cr *v1alpha1.SparkApplication) v1.OwnerReference {
	// This controller is a pod controller, not a Spark Application CR controller
	ownedByController := false
	// We still want the pod to be garbage collected immediately when the CR is deleted
	blockOwnerDeletion := true
	ownerRef := v1.OwnerReference{
		APIVersion:         cr.APIVersion,
		Kind:               cr.Kind,
		Name:               cr.Name,
		UID:                cr.UID,
		Controller:         &ownedByController,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}
	return ownerRef
}

func addFinalizer(pod *corev1.Pod) bool {
	changed := false
	if pod.ObjectMeta.DeletionTimestamp.IsZero() {
		if !containsString(pod.ObjectMeta.Finalizers, sparkApplicationFinalizerName) {
			pod.ObjectMeta.Finalizers = append(pod.ObjectMeta.Finalizers, sparkApplicationFinalizerName)
			changed = true
		}
	}

	return changed
}

func removeFinalizer(pod *corev1.Pod) bool {
	changed := false
	if !pod.ObjectMeta.DeletionTimestamp.IsZero() {
		if containsString(pod.ObjectMeta.Finalizers, sparkApplicationFinalizerName) {
			pod.ObjectMeta.Finalizers = removeString(pod.ObjectMeta.Finalizers, sparkApplicationFinalizerName)
			changed = true
		}
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

func (r *SparkPodReconciler) handleExecutor(ctx context.Context, pod *corev1.Pod, cr *v1alpha1.SparkApplication, log logr.Logger) error {
	log.Info("Handling executor pod")

	deepCopy := cr.DeepCopy()

	// Do we already have an entry for this executor in the CR?
	foundIdx := -1
	for idx, executor := range deepCopy.Status.Data.Executors {
		if executor.UID == string(pod.UID) {
			foundIdx = idx
			break
		}
	}

	if foundIdx == -1 {
		// Create new executor entry
		newExecutor := newPodCR(pod)
		newExecutors := append(deepCopy.Status.Data.Executors, newExecutor)
		deepCopy.Status.Data.Executors = newExecutors
	} else {
		// Update existing executor entry
		updatedExecutor := newPodCR(pod)
		deepCopy.Status.Data.Executors[foundIdx] = updatedExecutor
	}

	err := r.Client.Patch(ctx, deepCopy, client.MergeFrom(cr))
	if err != nil {
		return fmt.Errorf("patch error, %w", err)
	}

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

func (r *SparkPodReconciler) getSparkApiApplicationInfo(clientSet kubernetes.Interface, driverPod *corev1.Pod, applicationId string, logger logr.Logger) (*sparkapi.ApplicationInfo, error) {

	manager, err := r.getSparkApiManager(clientSet, driverPod, logger)
	if err != nil {
		return nil, fmt.Errorf("could not get spark api manager, %w", err)
	}

	applicationInfo, err := manager.GetApplicationInfo(applicationId)
	if err != nil {
		return nil, fmt.Errorf("could not get spark api application info, %w", err)
	}

	return applicationInfo, nil
}

func mapSparkApiApplicationInfo(deepCopy *v1alpha1.SparkApplication, sparkApiInfo *sparkapi.ApplicationInfo) {
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
	if pod.Labels[AppLabel] == AppEnterpriseGatewayLabelValue {
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

func (r *SparkPodReconciler) createNewSparkApplicationCr(ctx context.Context, driverPod *corev1.Pod, applicationId string) error {
	cr := &v1alpha1.SparkApplication{}

	cr.Name = applicationId
	cr.Namespace = driverPod.Namespace
	cr.Spec.ApplicationId = applicationId
	// We always need an application name, set it to driver pod name (will be updated with application name from Spark API)
	cr.Spec.ApplicationName = driverPod.Name

	heritage, err := getHeritage(driverPod)
	if err != nil {
		return fmt.Errorf("could not get heritage, %w", err)
	}
	cr.Spec.Heritage = heritage

	cr.Status.Data.Driver = newPodCR(driverPod)

	cr.Status.Data.Executors = make([]v1alpha1.Pod, 0)
	cr.Status.Data.RunStatistics.Attempts = make([]v1alpha1.Attempt, 0)
	cr.Status.Data.SparkProperties = make(map[string]string)

	err = r.Create(ctx, cr)
	if err != nil {
		return fmt.Errorf("could not create cr, %w", err)
	}

	return nil
}
