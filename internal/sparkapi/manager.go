package sparkapi

import (
	"context"
	"errors"
	"fmt"
	"github.com/spotinst/wave-operator/internal/sparkapi/client/transport"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/spotinst/wave-operator/catalog"
	sparkapiclient "github.com/spotinst/wave-operator/internal/sparkapi/client"
)

const (
	sparkDriverContainerName       = "spark-kubernetes-driver"
	appNameLabel                   = "app.kubernetes.io/name"
	historyServerAppNameLabelValue = "spark-history-server"
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

var GetManager = func(clientSet kubernetes.Interface, driverPod *corev1.Pod, logger logr.Logger) (Manager, error) {
	client, err := getSparkApiClient(clientSet, driverPod, logger)
	if err != nil {
		return nil, fmt.Errorf("could not get spark api client, %w", err)
	}
	return manager{
		client: client,
		logger: logger,
	}, nil
}

func getSparkApiClient(clientSet kubernetes.Interface, driverPod *corev1.Pod, logger logr.Logger) (sparkapiclient.Client, error) {

	// Try the driver API first, to get information on running applications
	// Once the application is finished the info is written to history server

	// Get client for driver pod
	if isSparkDriverRunning(driverPod) {
		logger.Info("Using driver pod client")
		return sparkapiclient.NewDriverPodClient(driverPod, clientSet), nil
	}

	logger.Info("Driver pod/container not running, will use history server")

	// Get client for history server
	historyServerService, err := getHistoryServerService(clientSet)
	if err != nil {
		logger.Info(fmt.Sprintf("Could not get history server service, error: %s", err.Error()))
	} else {
		logger.Info("Using history server client")
		return sparkapiclient.NewHistoryServerClient(historyServerService, clientSet), nil
	}

	return nil, fmt.Errorf("could not get spark api client")
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
	foundServices, err := clientSet.CoreV1().Services(catalog.SystemNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", appNameLabel, historyServerAppNameLabelValue),
	})
	if err != nil {
		return nil, fmt.Errorf("could not list services, %w", err)
	}

	if len(foundServices.Items) != 1 {
		return nil, fmt.Errorf("could not find history server service, found %d but wanted 1", len(foundServices.Items))
	}

	service := foundServices.Items[0]

	return &service, nil
}

func isSparkDriverRunning(driverPod *corev1.Pod) bool {

	if driverPod.Status.Phase != corev1.PodRunning {
		return false
	}

	if !driverPod.DeletionTimestamp.IsZero() {
		return false
	}

	for _, containerStatus := range driverPod.Status.ContainerStatuses {
		if containerStatus.Name == sparkDriverContainerName && containerStatus.Ready {
			return true
		}
	}

	return false
}

func IsServiceUnavailableError(err error) bool {
	if errors.As(err, &transport.ServiceUnavailableError{}) {
		return true
	}
	return false
}

func IsApplicationNotFoundError(err error) bool {
	if errors.As(err, &transport.UnknownAppError{}) {
		return true
	}
	return false
}
