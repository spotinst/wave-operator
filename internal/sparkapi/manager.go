package sparkapi

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	sparkapiclient "github.com/spotinst/wave-operator/internal/sparkapi/client"
)

const (
	systemNamespace          = "spot-system" // TODO Refactor to single source of truth
	sparkDriverContainerName = "spark-kubernetes-driver"
	historyServerServiceName = "wave-spark-history-server"
)

type Manager interface {
	GetApplicationInfo(applicationId string) (*ApplicationInfo, error)
}

type manager struct {
	client sparkapiclient.Client
	logger logr.Logger
}

type ApplicationInfo struct {
	ApplicationName      string
	SparkProperties      map[string]string
	TotalInputBytes      int64
	TotalOutputBytes     int64
	TotalExecutorCpuTime int64
	Attempts             []sparkapiclient.Attempt
}

func NewManager(clientSet kubernetes.Interface, driverPod *corev1.Pod, logger logr.Logger) (Manager, error) {

	var client sparkapiclient.Client

	// Get client for history server
	historyServerService, err := getHistoryServerService(clientSet)
	if err != nil {
		logger.Error(err, "could not get history server service")
	} else {
		logger.Info("Using history server Spark API client")
		client = sparkapiclient.NewHistoryServerClient(historyServerService, clientSet)
	}

	// TODO Health check?

	// Fall back on client for driver pod
	if client == nil {
		if isSparkDriverRunning(driverPod) {
			logger.Info("Falling back on driver pod Spark API client")
			client = sparkapiclient.NewDriverPodClient(driverPod, clientSet)
		} else {
			logger.Info("Driver pod/container not running, will not create Spark API client")
		}
	}

	if client == nil {
		return nil, fmt.Errorf("could not get spark api client")
	}

	return manager{
		client: client,
		logger: logger,
	}, nil
}

func (m manager) GetApplicationInfo(applicationId string) (*ApplicationInfo, error) {

	applicationInfo := &ApplicationInfo{}

	m.logger.Info("Will call Spark API")

	application, err := m.client.GetApplication(applicationId)
	if err != nil {
		return nil, fmt.Errorf("could not get application, %w", err)
	}

	if application == nil {
		return nil, fmt.Errorf("application is nil")
	}

	applicationInfo.ApplicationName = application.Name
	applicationInfo.Attempts = application.Attempts

	environment, err := m.client.GetEnvironment(applicationId)
	if err != nil {
		return nil, fmt.Errorf("could not get environment, %w", err)
	}

	sparkProperties, err := parseSparkProperties(environment, m.logger)
	if err != nil {
		return nil, fmt.Errorf("could not parse spark properties, %w", err)
	}

	applicationInfo.SparkProperties = sparkProperties

	stages, err := m.client.GetStages(applicationId)
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

	applicationInfo.TotalOutputBytes = totalOutputBytes
	applicationInfo.TotalInputBytes = totalInputBytes
	applicationInfo.TotalExecutorCpuTime = totalExecutorCpuTime

	m.logger.Info("Finished calling Spark API")

	return applicationInfo, nil
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

func getHistoryServerService(clientSet kubernetes.Interface) (*corev1.Service, error) {
	ctx := context.TODO()
	service, err := clientSet.CoreV1().Services(systemNamespace).Get(ctx, historyServerServiceName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return service, nil
}

func isSparkDriverRunning(driverPod *corev1.Pod) bool {

	if driverPod.Status.Phase != corev1.PodRunning {
		return false
	}

	if !driverPod.DeletionTimestamp.IsZero() {
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