package controllers

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/spotinst/wave-operator/api/v1alpha1"
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

	spotKubernetesControllerCmName                       = "spotinst-kubernetes-cluster-controller-config"
	spotKubernetesControllerCmNamespace                  = "kube-system"
	spotKubernetesControllerCmClusterIdentifierFieldName = "spotinst.cluster-identifier"

	requeueAfterTimeout = 10 * time.Second
)

// SparkPodReconciler reconciles Pod objects to discover Spark applications
type SparkPodReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

func NewSparkPodReconciler(
	client client.Client,
	log logr.Logger,
	scheme *runtime.Scheme) *SparkPodReconciler {

	return &SparkPodReconciler{
		Client: client,
		Log:    log,
		Scheme: scheme,
	}
}

func (r *SparkPodReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
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

	// TODO - do we really need this? Or is this set by the kubernetes controller/core
	clusterIdentifier, err := r.getClusterIdentifier(ctx)
	if err != nil {
		return false, fmt.Errorf("could not get cluster identifier, %w", err)
	}
	deepCopy.Spec.ClusterIdentifier = clusterIdentifier

	heritage, err := getHeritage(driverPod)
	if err != nil {
		return false, fmt.Errorf("could not get heritage, %w", err)
	}
	deepCopy.Spec.Heritage = heritage

	driverPodCr := newPodCR(driverPod)
	deepCopy.Status.Data.Driver = driverPodCr

	// TODO Talk to Spark API to get the rest of the information

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

func (r *SparkPodReconciler) getClusterIdentifier(ctx context.Context) (string, error) {
	cm := corev1.ConfigMap{}
	err := r.Get(ctx, ctrlclient.ObjectKey{Name: spotKubernetesControllerCmName, Namespace: spotKubernetesControllerCmNamespace}, &cm)
	if err != nil {
		return "", fmt.Errorf("could not get config map, %w", err)
	}

	clusterIdentifier := ""
	if cm.Data != nil {
		clusterIdentifier = cm.Data[spotKubernetesControllerCmClusterIdentifierFieldName]
	}

	if clusterIdentifier == "" {
		return "", fmt.Errorf("value %q not found in config map", spotKubernetesControllerCmClusterIdentifierFieldName)
	}

	return clusterIdentifier, nil
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