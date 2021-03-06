package sparkapi

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/spotinst/wave-operator/catalog"
	"github.com/spotinst/wave-operator/internal/config"
	sparkapiclient "github.com/spotinst/wave-operator/internal/sparkapi/client"
	"github.com/spotinst/wave-operator/internal/sparkapi/client/transport"
)

const (
	SparkDriverContainerName       = "spark-kubernetes-driver"
	appNameLabel                   = "app.kubernetes.io/name"
	historyServerAppNameLabelValue = "spark-history-server"

	SparkStreaming WorkloadType = "spark-streaming"
)

var ErrApiNotAvailable = errors.New("spark api not available")

type WorkloadType string

type Manager interface {
	GetApplicationInfo(applicationID string) (*ApplicationInfo, error)
}

type manager struct {
	client sparkapiclient.Client
	logger logr.Logger
}

type ApplicationInfo struct {
	ID                      string
	ApplicationName         string
	SparkProperties         map[string]string
	TotalNewInputBytes      int64
	TotalNewOutputBytes     int64
	TotalNewExecutorCpuTime int64
	Attempts                []sparkapiclient.Attempt
	Executors               []sparkapiclient.Executor
	WorkloadType            WorkloadType
	Metrics                 sparkapiclient.Metrics
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
		return sparkapiclient.NewDriverPodClient(driverPod, clientSet), nil
	}

	// Check if the event log sync feature is on
	if !config.IsEventLogSyncEnabled(driverPod.Annotations) {
		return nil, ErrApiNotAvailable
	}

	logger.Info("Driver pod/container not running, will use history server Spark API client")

	// Get client for history server
	historyServerService, err := getHistoryServerService(clientSet, logger)
	if err != nil {
		return nil, fmt.Errorf("could not get history server service, %w", err)
	}

	return sparkapiclient.NewHistoryServerClient(historyServerService, clientSet), nil
}

func (m manager) GetApplicationInfo(applicationID string) (*ApplicationInfo, error) {

	applicationInfo := &ApplicationInfo{}

	application, err := m.client.GetApplication(applicationID)
	if err != nil {
		return nil, fmt.Errorf("could not get application, %w", err)
	}

	if application == nil {
		return nil, fmt.Errorf("application is nil")
	}

	applicationInfo.ID = application.ID
	applicationInfo.ApplicationName = application.Name
	applicationInfo.Attempts = application.Attempts

	environment, err := m.client.GetEnvironment(applicationID)
	if err != nil {
		return nil, fmt.Errorf("could not get environment, %w", err)
	}

	sparkProperties, err := parseSparkProperties(environment, m.logger)
	if err != nil {
		return nil, fmt.Errorf("could not parse spark properties, %w", err)
	}

	applicationInfo.SparkProperties = sparkProperties

	executors, err := m.client.GetAllExecutors(applicationID)
	if err != nil {
		return nil, fmt.Errorf("could not get executors, %w", err)
	}

	applicationInfo.Executors = executors

	if dc, ok := m.client.(sparkapiclient.DriverClient); ok {
		applicationInfo.WorkloadType = m.getWorkloadType(dc, applicationID)
		metrics, err := dc.GetMetrics()
		if err != nil {
			m.logger.Error(err, "Unable to collect driver metrics")
		}

		applicationInfo.Metrics = metrics

		if _, err := registry.Register(applicationInfo); err != nil {
			m.logger.Error(err, "Unable to register application for metrics collection")
		}
	}

	return applicationInfo, nil
}

func (m manager) getWorkloadType(c sparkapiclient.DriverClient, applicationID string) WorkloadType {
	// Streaming statistics endpoint is only available on running driver
	_, err := c.GetStreamingStatistics(applicationID)
	if err == nil {
		return SparkStreaming
	}

	return ""
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

func getHistoryServerService(clientSet kubernetes.Interface, logger logr.Logger) (*corev1.Service, error) {
	ctx := context.TODO()
	foundServices, err := clientSet.CoreV1().Services(catalog.SystemNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", appNameLabel, historyServerAppNameLabelValue),
	})
	if err != nil {
		return nil, fmt.Errorf("could not list services, %w", err)
	}

	if len(foundServices.Items) != 1 {
		logger.Info(fmt.Sprintf("Found unexpected number of history server services, found %d but wanted 1", len(foundServices.Items)))
	}

	if len(foundServices.Items) > 0 {
		service := foundServices.Items[0]
		return &service, nil
	}

	return nil, fmt.Errorf("could not find history server service")
}

func isSparkDriverRunning(driverPod *corev1.Pod) bool {

	if driverPod.Status.Phase != corev1.PodRunning {
		return false
	}

	if !driverPod.DeletionTimestamp.IsZero() {
		return false
	}

	for _, containerStatus := range driverPod.Status.ContainerStatuses {
		if containerStatus.Name == SparkDriverContainerName && containerStatus.Ready {
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

func IsNotFoundError(err error) bool {
	if errors.As(err, &transport.NotFoundError{}) {
		return true
	}
	return false
}

func IsApiNotAvailableError(err error) bool {
	return errors.Is(err, ErrApiNotAvailable)
}
