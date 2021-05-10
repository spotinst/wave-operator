package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/spotinst/wave-operator/internal/spot"
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
	"github.com/spotinst/wave-operator/internal/storagesync"
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

	apiVersion                = "wave.spot.io/v1alpha1"
	sparkApplicationKind      = "SparkApplication"
	waveKindLabel             = "wave.spot.io/kind"
	waveApplicationIDLabel    = "wave.spot.io/application-id"
	sparkOperatorAppNameLabel = "sparkoperator.k8s.io/app-name"

	waveApplicationNameAnnotation     = "wave.spot.io/application-name"
	stageMetricsAggregationAnnotation = "wave.spot.io/stageMetricsAggregation"
	workloadTypeAnnotation            = "wave.spot.io/workloadType"

	requeueAfterTimeout                  = 10 * time.Second
	podDeletionTimeout                   = 5 * time.Minute
	maxSparkApiCommunicationAttemptCount = 20
)

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=wave.spot.io,resources=sparkapplications,verbs=get;list;watch;create;update;patch;delete

// SparkPodReconciler reconciles Pod objects to discover Spark applications
type SparkPodReconciler struct {
	client.Client
	ClientSet              kubernetes.Interface
	getSparkApiManager     SparkApiManagerGetter
	Log                    logr.Logger
	Scheme                 *runtime.Scheme
	sparkApiAttemptCounter map[string]int
}

func NewSparkPodReconciler(
	client client.Client,
	clientSet kubernetes.Interface,
	sparkApiManagerGetter SparkApiManagerGetter,
	log logr.Logger,
	scheme *runtime.Scheme,
	app spot.ApplicationClient) *SparkPodReconciler {

	return &SparkPodReconciler{
		Client:                 client,
		ClientSet:              clientSet,
		getSparkApiManager:     sparkApiManagerGetter,
		Log:                    log,
		Scheme:                 scheme,
		sparkApiAttemptCounter: make(map[string]int),
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
		return ctrl.Result{}, nil
	}

	sparkApplicationID, ok := p.Labels[SparkAppLabel]
	if !ok {
		// This is not a Spark application pod, ignore
		return ctrl.Result{}, nil
	}

	if sparkApplicationID == "" {
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

	log = r.Log.WithValues("role", sparkRole, "name", p.Name, "namespace", p.Namespace,
		"sparkApplicationID", sparkApplicationID, "phase", p.Status.Phase, "deleted", !p.ObjectMeta.DeletionTimestamp.IsZero())

	log.Info("Reconciling")

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

	// Pod deletion timeout
	// If we haven't been able to remove the finalizer and allow the pod to be deleted
	// within the timeout just give up and remove the finalizer
	if podDeletionTimeoutPassed(p) {
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
			log.Info("Finalizer removed due to deletion timeout")
		}
		return ctrl.Result{}, nil
	}

	// Get or create Spark application CR
	cr, err := r.getSparkApplicationCR(ctx, p.Namespace, sparkApplicationID)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			if !p.DeletionTimestamp.IsZero() && hasWaveSparkApplicationOwnerRef(p, sparkApplicationID) {
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
				err := r.createNewSparkApplicationCR(ctx, p, sparkApplicationID, log)
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
			if sparkapi.IsNotFoundError(err) ||
				sparkapi.IsServiceUnavailableError(err) {
				log.Info(fmt.Sprintf("Spark API error: %s", err.Error()))
				// Requeue non-running driver (wait for history server to respond)
				if p.Status.Phase != corev1.PodRunning {
					// Increment attempt counter
					r.sparkApiAttemptCounter[sparkApplicationID]++
					if r.sparkApiAttemptCounter[sparkApplicationID] < maxSparkApiCommunicationAttemptCount {
						log.Info("Requeue non-running driver pod",
							"sparkApiAttemptCount", r.sparkApiAttemptCounter[sparkApplicationID])
						// Let's requeue after a set amount of time, don't want exponential backoff
						return ctrl.Result{
							Requeue:      true,
							RequeueAfter: requeueAfterTimeout,
						}, nil
					} else {
						log.Info("Max Spark API communication attempts reached, will not requeue")
					}
				}
			} else if sparkapi.IsApiNotAvailableError(err) {
				// Spark API is not available, don't want to requeue
				log.Info(fmt.Sprintf("Spark API not available: %s", err.Error()))
			} else {
				log.Error(err, "error handling driver pod")
				return ctrl.Result{}, err
			}
		} else {
			// Reset Spark API attempt counter
			delete(r.sparkApiAttemptCounter, sparkApplicationID)
		}

		// Requeue running drivers
		if p.Status.Phase == corev1.PodRunning {
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

	if storagesync.ShouldStopSync(pod) {
		// Stop storage sync, best effort
		log.Info("Stopping storage sync")
		err := storagesync.StopSync(pod)
		if err != nil {
			log.Error(err, "could not stop storage sync")
		}
	}

	deepCopy := cr.DeepCopy()

	updatedDriverPodCR := newPodCR(pod, &deepCopy.Status.Data.Driver, log)
	deepCopy.Status.Data.Driver = updatedDriverPodCR

	// Fetch information from Spark API
	// Let's update the driver pod information even though the Spark API call fails

	stageMetricsAggregatorState, err := getStageMetricsAggregatorState(deepCopy)
	if err != nil {
		return fmt.Errorf("could not get stage metrics aggregation state, %w", err)
	}

	var sparkApiError error
	sparkApiApplicationInfo, err := r.getSparkApiApplicationInfo(r.ClientSet, pod, cr.Spec.ApplicationID, stageMetricsAggregatorState, log)
	if err != nil {
		sparkApiError = fmt.Errorf("could not get spark api application information, %w", err)
	} else {
		setSparkApiApplicationInfo(deepCopy, sparkApiApplicationInfo, log)
	}

	// Get an application name
	sparkApplicationName := getSparkApplicationName(pod, sparkApiApplicationInfo)
	deepCopy.Spec.ApplicationName = sparkApplicationName

	//set "wave.spot.io/application-name" annotation as an application name
	if deepCopy.Annotations == nil {
		deepCopy.Annotations = make(map[string]string)
	}

	deepCopy.Annotations[waveApplicationNameAnnotation] = sparkApplicationName

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

func podDeletionTimeoutPassed(pod *corev1.Pod) bool {
	if !pod.ObjectMeta.DeletionTimestamp.IsZero() {
		currentTime := time.Now().Unix()
		timeoutTime := pod.ObjectMeta.DeletionTimestamp.Add(podDeletionTimeout).Unix()
		if currentTime >= timeoutTime {
			return true
		}
	}
	return false
}

func hasWaveSparkApplicationOwnerRef(pod *corev1.Pod, applicationID string) bool {
	for _, ownerRef := range pod.OwnerReferences {
		if ownerRef.APIVersion == apiVersion &&
			ownerRef.Kind == sparkApplicationKind &&
			ownerRef.Name == applicationID {
			return true
		}
	}

	return false
}

func (r *SparkPodReconciler) handleExecutor(ctx context.Context, pod *corev1.Pod, cr *v1alpha1.SparkApplication, log logr.Logger) error {
	deepCopy := cr.DeepCopy()

	// Do we already have an entry for this executor in the CR?
	foundIDx := -1
	for idx, executor := range deepCopy.Status.Data.Executors {
		if executor.UID == string(pod.UID) {
			foundIDx = idx
			break
		}
	}

	if foundIDx == -1 {
		// Create new executor entry
		newExecutor := newPodCR(pod, nil, log)
		newExecutors := append(deepCopy.Status.Data.Executors, newExecutor)
		deepCopy.Status.Data.Executors = newExecutors
	} else {
		// Update existing executor entry
		existingExecutor := &deepCopy.Status.Data.Executors[foundIDx]
		updatedExecutor := newPodCR(pod, existingExecutor, log)
		deepCopy.Status.Data.Executors[foundIDx] = updatedExecutor
	}

	err := r.Client.Patch(ctx, deepCopy, client.MergeFrom(cr))
	if err != nil {
		return fmt.Errorf("patch error, %w", err)
	}

	if err := r.app.SaveApplication(cr); err != nil {
		return fmt.Errorf("could not create application in spot backend, %w", err)
	}

	return nil
}

func newPodCR(pod *corev1.Pod, existingPodCR *v1alpha1.Pod, log logr.Logger) v1alpha1.Pod {
	podCR := v1alpha1.Pod{}

	podCR.UID = string(pod.UID)
	podCR.Namespace = pod.Namespace
	podCR.Name = pod.Name
	podCR.Phase = pod.Status.Phase
	podCR.Statuses = pod.Status.ContainerStatuses
	podCR.CreationTimestamp = pod.CreationTimestamp
	podCR.DeletionTimestamp = pod.DeletionTimestamp
	podCR.Labels = pod.Labels
	podCR.StateHistory = getUpdatedPodStateHistory(pod, existingPodCR, log)

	if podCR.Statuses == nil {
		podCR.Statuses = make([]corev1.ContainerStatus, 0)
	}

	if podCR.Labels == nil {
		podCR.Labels = make(map[string]string)
	}

	return podCR
}

func getUpdatedPodStateHistory(pod *corev1.Pod, existingPodCR *v1alpha1.Pod, log logr.Logger) []v1alpha1.PodStateHistoryEntry {
	var stateHistory []v1alpha1.PodStateHistoryEntry

	if existingPodCR != nil {
		stateHistory = existingPodCR.StateHistory
	}

	if len(stateHistory) == 0 {
		// First entry
		newEntry := newPodStateHistoryEntry(pod)
		log.Info("New pod state", "podState", newEntry)
		stateHistory = append(stateHistory, newEntry)
	} else {
		latestEntry := stateHistory[len(stateHistory)-1]
		newEntry := newPodStateHistoryEntry(pod)
		if !podStateHistoryEntryEqual(latestEntry, newEntry) {
			log.Info("New pod state", "podState", newEntry)
			stateHistory = append(stateHistory, newEntry)
		}
	}

	return stateHistory
}

func newPodStateHistoryEntry(pod *corev1.Pod) v1alpha1.PodStateHistoryEntry {
	containerStatuses := make(map[string]v1alpha1.PodStateHistoryContainerStatus)

	for _, podContainerStatus := range pod.Status.ContainerStatuses {
		status := v1alpha1.PodStateHistoryContainerStatus{}

		if podContainerStatus.State.Terminated != nil {
			status.State = v1alpha1.ContainerStateTerminated
			status.ExitCode = &podContainerStatus.State.Terminated.ExitCode
		} else if podContainerStatus.State.Running != nil {
			status.State = v1alpha1.ContainerStateRunning
		} else if podContainerStatus.State.Waiting != nil {
			status.State = v1alpha1.ContainerStateWaiting
		} else {
			// Default to waiting
			status.State = v1alpha1.ContainerStateWaiting
		}

		containerStatuses[podContainerStatus.Name] = status
	}

	return v1alpha1.PodStateHistoryEntry{
		Timestamp:         v1.Now(),
		Phase:             pod.Status.Phase,
		ContainerStatuses: containerStatuses,
	}
}

func podStateHistoryEntryEqual(a v1alpha1.PodStateHistoryEntry, b v1alpha1.PodStateHistoryEntry) bool {
	if a.Phase != b.Phase {
		return false
	}
	if len(a.ContainerStatuses) != len(b.ContainerStatuses) {
		return false
	}
	for aContainerName, aContainerStatus := range a.ContainerStatuses {
		bContainerStatus, ok := b.ContainerStatuses[aContainerName]
		if !ok {
			return false
		}
		if aContainerStatus.State != bContainerStatus.State {
			return false
		}
		aExitCode := int32(-1)
		bExitCode := int32(-1)
		if aContainerStatus.ExitCode != nil {
			aExitCode = *aContainerStatus.ExitCode
		}
		if bContainerStatus.ExitCode != nil {
			bExitCode = *bContainerStatus.ExitCode
		}
		if aExitCode != bExitCode {
			return false
		}
	}
	return true
}

func (r *SparkPodReconciler) getSparkApiApplicationInfo(clientSet kubernetes.Interface, driverPod *corev1.Pod, applicationID string, metricsAggregatorState sparkapi.StageMetricsAggregatorState, logger logr.Logger) (*sparkapi.ApplicationInfo, error) {

	manager, err := r.getSparkApiManager(clientSet, driverPod, logger)
	if err != nil {
		return nil, fmt.Errorf("could not get spark api manager, %w", err)
	}

	applicationInfo, err := manager.GetApplicationInfo(applicationID, metricsAggregatorState, logger)
	if err != nil {
		return nil, fmt.Errorf("could not get spark api application info, %w", err)
	}

	return applicationInfo, nil
}

func setSparkApiApplicationInfo(deepCopy *v1alpha1.SparkApplication, sparkApiInfo *sparkapi.ApplicationInfo, log logr.Logger) {
	deepCopy.Status.Data.SparkProperties = sparkApiInfo.SparkProperties

	deepCopy.Status.Data.RunStatistics.TotalExecutorCpuTime += sparkApiInfo.TotalNewExecutorCpuTime
	deepCopy.Status.Data.RunStatistics.TotalInputBytes += sparkApiInfo.TotalNewInputBytes
	deepCopy.Status.Data.RunStatistics.TotalOutputBytes += sparkApiInfo.TotalNewOutputBytes

	err := setStageMetricsAggregatorState(deepCopy, sparkApiInfo.MetricsAggregatorState)
	if err != nil {
		// Best effort
		log.Error(err, "Could not set stage metrics aggregation state annotation")
	}

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

	executors := make([]v1alpha1.Executor, 0, len(sparkApiInfo.Executors))
	for _, apiExecutor := range sparkApiInfo.Executors {
		executor := v1alpha1.Executor{
			ID:                apiExecutor.ID,
			IsActive:          apiExecutor.IsActive,
			AddTime:           apiExecutor.AddTime,
			RemoveTime:        apiExecutor.RemoveTime,
			RemoveReason:      apiExecutor.RemoveReason,
			RddBlocks:         apiExecutor.RddBlocks,
			MemoryUsed:        apiExecutor.MemoryUsed,
			DiskUsed:          apiExecutor.DiskUsed,
			TotalCores:        apiExecutor.TotalCores,
			MaxTasks:          apiExecutor.MaxTasks,
			ActiveTasks:       apiExecutor.ActiveTasks,
			FailedTasks:       apiExecutor.FailedTasks,
			CompletedTasks:    apiExecutor.CompletedTasks,
			TotalTasks:        apiExecutor.TotalTasks,
			TotalDuration:     apiExecutor.TotalDuration,
			TotalGCTime:       apiExecutor.TotalGCTime,
			TotalInputBytes:   apiExecutor.TotalInputBytes,
			TotalShuffleRead:  apiExecutor.TotalShuffleRead,
			TotalShuffleWrite: apiExecutor.TotalShuffleWrite,
			IsBlacklisted:     apiExecutor.IsBlacklisted,
			MaxMemory:         apiExecutor.MaxMemory,
			MemoryMetrics: v1alpha1.ExecutorMemoryMetrics{
				UsedOnHeapStorageMemory:   apiExecutor.MemoryMetrics.UsedOnHeapStorageMemory,
				UsedOffHeapStorageMemory:  apiExecutor.MemoryMetrics.UsedOffHeapStorageMemory,
				TotalOnHeapStorageMemory:  apiExecutor.MemoryMetrics.TotalOnHeapStorageMemory,
				TotalOffHeapStorageMemory: apiExecutor.MemoryMetrics.TotalOffHeapStorageMemory,
			},
		}
		executors = append(executors, executor)
	}

	deepCopy.Status.Data.RunStatistics.Executors = executors

	if sparkApiInfo.WorkloadType != "" {
		setWorkloadType(deepCopy, sparkApiInfo.WorkloadType)
	}
}

func getSparkApplicationName(driverPod *corev1.Pod, sparkApiInfo *sparkapi.ApplicationInfo) string {
	var sparkApplicationName string

	// check if the `wave.spot.io/application-name` label has been set
	if applicationNameAnnotation, ok := driverPod.Annotations[waveApplicationNameAnnotation]; ok {
		sparkApplicationName = applicationNameAnnotation
		//sparkApp.Spec.ApplicationName = applicationNameAnnotation
	} else if operatorAppNameLabel, ok := driverPod.Labels[sparkOperatorAppNameLabel]; ok {
		sparkApplicationName = operatorAppNameLabel
	} else if sparkApiInfo != nil {
		sparkApplicationName = sparkApiInfo.ApplicationName
	}

	if sparkApplicationName == "" {
		sparkApplicationName = driverPod.Name
	}

	return sparkApplicationName
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

func (r *SparkPodReconciler) getSparkApplicationCR(ctx context.Context, namespace string, applicationID string) (*v1alpha1.SparkApplication, error) {
	app := v1alpha1.SparkApplication{}
	err := r.Get(ctx, ctrlclient.ObjectKey{Name: applicationID, Namespace: namespace}, &app)
	if err != nil {
		return nil, err
	}

	return &app, nil
}

func (r *SparkPodReconciler) createNewSparkApplicationCR(ctx context.Context, driverPod *corev1.Pod, applicationID string, log logr.Logger) error {
	cr := &v1alpha1.SparkApplication{}

	cr.Labels = map[string]string{
		waveKindLabel:          sparkApplicationKind, // Facilitates cost calculations
		waveApplicationIDLabel: applicationID,        // Facilitates cost calculations
	}

	cr.Annotations = make(map[string]string)

	cr.Name = applicationID
	cr.Namespace = driverPod.Namespace
	cr.Spec.ApplicationID = applicationID

	//get an application name
	sparkApplicationName := getSparkApplicationName(driverPod, nil)
	cr.Spec.ApplicationName = sparkApplicationName
	//set "wave.spot.io/application-name" annotation as an application name
	cr.Annotations[waveApplicationNameAnnotation] = sparkApplicationName

	heritage, err := getHeritage(driverPod)
	if err != nil {
		return fmt.Errorf("could not get heritage, %w", err)
	}
	cr.Spec.Heritage = heritage

	cr.Status.Data.Driver = newPodCR(driverPod, nil, log)

	cr.Status.Data.Executors = make([]v1alpha1.Pod, 0)
	cr.Status.Data.RunStatistics.Attempts = make([]v1alpha1.Attempt, 0)
	cr.Status.Data.RunStatistics.Executors = make([]v1alpha1.Executor, 0)
	cr.Status.Data.SparkProperties = make(map[string]string)

	err = r.Create(ctx, cr)
	if err != nil {
		return fmt.Errorf("could not create cr, %w", err)
	}

	return nil
}

func getStageMetricsAggregatorState(cr *v1alpha1.SparkApplication) (sparkapi.StageMetricsAggregatorState, error) {
	newState := sparkapi.NewStageMetricsAggregatorState()
	if cr.Annotations == nil {
		// We haven't processed any stages yet
		return newState, nil
	}
	val := cr.Annotations[stageMetricsAggregationAnnotation]
	if val == "" {
		// We haven't processed any stages yet
		return newState, nil
	}
	state := sparkapi.StageMetricsAggregatorState{}
	err := json.Unmarshal([]byte(val), &state)
	if err != nil {
		return newState, fmt.Errorf("could not unmarshal state %s, %w", val, err)
	}
	return state, nil
}

func setStageMetricsAggregatorState(cr *v1alpha1.SparkApplication, state sparkapi.StageMetricsAggregatorState) error {
	if cr.Annotations == nil {
		cr.Annotations = make(map[string]string)
	}
	marshalled, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("could not marshal state, %w", err)
	}
	cr.Annotations[stageMetricsAggregationAnnotation] = string(marshalled)
	return nil
}

func setWorkloadType(cr *v1alpha1.SparkApplication, workloadType sparkapi.WorkloadType) {
	if cr.Annotations == nil {
		cr.Annotations = make(map[string]string)
	}
	cr.Annotations[workloadTypeAnnotation] = string(workloadType)
}
