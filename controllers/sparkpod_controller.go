package controllers

import (
	"context"
	"fmt"
	"k8s.io/client-go/kubernetes"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/spotinst/wave-operator/api/v1alpha1"
	sparkapiclient "github.com/spotinst/wave-operator/internal/sparkapi/client"
)

const (
	SparkRoleLabel  = "spark-role"
	SparkAppLabel   = "spark-app-selector"
	DriverRole      = "driver"
	ExecutorRole    = "executor"
	SystemNamespace = "spot-system" // TODO Refactor to single source of truth

	AppLabel                       = "app"
	AppLabelValueEnterpriseGateway = "enterprise-gateway"
	SparkOperatorLaunchedByLabel   = "sparkoperator.k8s.io/launched-by-spark-operator"

	sparkDriverContainerName = "spark-kubernetes-driver"

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

	shouldRequeue := false
	sparkRole := p.Labels[SparkRoleLabel]
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
		err := fmt.Errorf("unknown spark role: %q", sparkRole)
		log.Error(err, "error handling spark pod")
		return ctrl.Result{}, nil // Just log error
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

func (r *SparkPodReconciler) handleDriverPod(ctx context.Context, applicationId string, driverPod *corev1.Pod) (shouldRequeue bool, err error) {
	log := r.Log.WithValues("name", driverPod.Name, "namespace", driverPod.Namespace, "sparkApplicationId", applicationId)
	log.Info("Handling driver pod", "phase", driverPod.Status.Phase)

	// Get application CR if it exists, otherwise build new one
	crExists := true
	cr, err := r.getSparkApplicationCr(ctx, applicationId)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			crExists = false
			cr = &v1alpha1.SparkApplication{}
		} else {
			return false, fmt.Errorf("could not get spark application cr, %w", err)
		}
	}

	deepCopy := cr.DeepCopy()

	deepCopy.Spec.ApplicationId = applicationId

	heritage, err := getHeritage(driverPod)
	if err != nil {
		return false, fmt.Errorf("could not get heritage, %w", err)
	}
	deepCopy.Spec.Heritage = heritage

	driverPodCr := newPodCR(driverPod)
	deepCopy.Status.Data.Driver = driverPodCr

	sparkApiInfo, err := getSparkApiInfo(r.ClientSet, driverPod, applicationId, log)
	if err != nil {
		// Best effort, just log error
		log.Error(err, "could not get spark api information")
	} else if sparkApiInfo != nil {
		mapSparkApplicationInfo(deepCopy, sparkApiInfo)
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

	// Requeue driver pods in running phase
	shouldRequeue = driverPod.Status.Phase == corev1.PodRunning
	log.Info("Finished handling driver pod", "requeue", shouldRequeue)

	return shouldRequeue, nil
}

func (r *SparkPodReconciler) handleExecutorPod(ctx context.Context, applicationId string, executorPod *corev1.Pod) error {
	log := r.Log.WithValues("name", executorPod.Name, "namespace", executorPod.Namespace, "sparkApplicationId", applicationId)
	log.Info("Handling executor pod", "phase", string(executorPod.Status.Phase))

	cr, err := r.getSparkApplicationCr(ctx, applicationId)
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

	if podCr.Statuses == nil {
		podCr.Statuses = make([]corev1.ContainerStatus, 0)
	}

	return podCr
}

func mapSparkApplicationInfo(deepCopy *v1alpha1.SparkApplication, sparkApiInfo *sparkApiInfo) {
	deepCopy.Spec.ApplicationName = sparkApiInfo.applicationName
	deepCopy.Status.Data.SparkProperties = sparkApiInfo.sparkProperties
	deepCopy.Status.Data.RunStatistics.TotalExecutorCpuTime = sparkApiInfo.totalExecutorCpuTime
	deepCopy.Status.Data.RunStatistics.TotalInputBytes = sparkApiInfo.totalInputBytes
	deepCopy.Status.Data.RunStatistics.TotalOutputBytes = sparkApiInfo.totalOutputBytes

	attempts := make([]v1alpha1.Attempt, 0, len(sparkApiInfo.attempts))
	for _, apiAttempt := range sparkApiInfo.attempts {
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

	deepCopy.Status.State = v1alpha1.SparkStateUnknown // TODO Where does this come from?
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

func (r *SparkPodReconciler) getSparkApplicationCr(ctx context.Context, applicationId string) (*v1alpha1.SparkApplication, error) {
	app := v1alpha1.SparkApplication{}
	err := r.Get(ctx, ctrlclient.ObjectKey{Name: applicationId, Namespace: SystemNamespace}, &app)
	if err != nil {
		return nil, err
	}

	return &app, nil
}

func (r *SparkPodReconciler) createSparkApplicationCr(ctx context.Context, application *v1alpha1.SparkApplication) error {
	application.Name = application.Spec.ApplicationId
	application.Namespace = SystemNamespace

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

// TODO Refactor - should be somewhere else? Async with locking on a CR access layer?
func getSparkApiInfo(clientSet kubernetes.Interface, driverPod *corev1.Pod, applicationId string, logger logr.Logger) (*sparkApiInfo, error) {

	// TODO Communicate with history server if pod is not running anymore, or always talk to history server?
	// TODO Or try pod first, and fall back on history server
	if !isSparkDriverRunning(driverPod) {
		logger.Info("driver pod/container not running or has been marked deleted, will not get spark api information")
		return nil, nil
	}

	sparkApiClient := sparkapiclient.NewDriverPodClient(driverPod, clientSet)
	sparkApiInfo := &sparkApiInfo{}

	logger.Info("Will call Spark API")

	application, err := sparkApiClient.GetApplication(applicationId)
	if err != nil {
		return nil, fmt.Errorf("could not get application, %w", err)
	}

	if application == nil {
		return nil, fmt.Errorf("application is nil")
	}

	sparkApiInfo.applicationName = application.Name
	sparkApiInfo.attempts = application.Attempts

	environment, err := sparkApiClient.GetEnvironment(applicationId)
	if err != nil {
		return nil, fmt.Errorf("could not get environment, %w", err)
	}

	sparkProperties, err := parseSparkProperties(environment, logger)
	if err != nil {
		return nil, fmt.Errorf("could not parse spark properties, %w", err)
	}

	sparkApiInfo.sparkProperties = sparkProperties

	stages, err := sparkApiClient.GetStages(applicationId)
	if err != nil {
		return nil, fmt.Errorf("could not get stages, %w", err)
	}

	if stages == nil {
		return nil, fmt.Errorf("stages are nil")
	}

	var totalExecutorCpuTime int64
	var totalInputBytes int64
	var totalOutputBytes int64

	for _, stage := range stages {
		totalExecutorCpuTime += stage.ExecutorCpuTime
		totalInputBytes += stage.InputBytes
		totalOutputBytes += stage.OutputBytes
	}

	sparkApiInfo.totalOutputBytes = totalOutputBytes
	sparkApiInfo.totalInputBytes = totalInputBytes
	sparkApiInfo.totalExecutorCpuTime = totalExecutorCpuTime

	logger.Info("Finished calling Spark API")

	return sparkApiInfo, nil
}

func isSparkDriverRunning(driverPod *corev1.Pod) bool {

	if driverPod.Status.Phase != corev1.PodRunning {
		return false
	}

	if driverPod.DeletionTimestamp != nil {
		return false
	}

	if driverPod.Status.ContainerStatuses == nil {
		return false
	}

	foundDriverContainer := false
	for _, containerStatus := range driverPod.Status.ContainerStatuses {
		if containerStatus.Name == sparkDriverContainerName {
			foundDriverContainer = true
			if !containerStatus.Ready {
				return false
			}
		}
	}

	if !foundDriverContainer {
		return false
	}

	return true
}

func parseSparkProperties(environment *sparkapiclient.Environment, logger logr.Logger) (map[string]string, error) {

	if environment == nil {
		return nil, fmt.Errorf("environment is nil")
	}

	if environment.SparkProperties == nil {
		return nil, fmt.Errorf("spark properties are nil")
	}

	sparkProperties := make(map[string]string, len(environment.SparkProperties))

	for _, propertyTuple := range environment.SparkProperties {
		if len(propertyTuple) == 2 {
			sparkProperties[propertyTuple[0]] = propertyTuple[1]
		} else {
			// Ignore, just log error
			err := fmt.Errorf("got spark property tuple of length %d, wanted 2: %s", len(propertyTuple), propertyTuple)
			logger.Error(err, "spark properties parse error")
		}
	}

	return sparkProperties, nil
}

type sparkApiInfo struct {
	applicationName      string
	sparkProperties      map[string]string
	totalInputBytes      int64
	totalOutputBytes     int64
	totalExecutorCpuTime int64
	attempts             []sparkapiclient.Attempt
}
