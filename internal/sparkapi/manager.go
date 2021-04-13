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
	sparkapiclient "github.com/spotinst/wave-operator/internal/sparkapi/client"
	"github.com/spotinst/wave-operator/internal/sparkapi/client/transport"
)

const (
	SparkDriverContainerName       = "spark-kubernetes-driver"
	appNameLabel                   = "app.kubernetes.io/name"
	historyServerAppNameLabelValue = "spark-history-server"

	SparkStreaming WorkloadType = "spark-streaming"
)

type WorkloadType string

type Manager interface {
	GetApplicationInfo(applicationID string, maxProcessedStageID int, log logr.Logger) (*ApplicationInfo, error)
}

type manager struct {
	client sparkapiclient.Client
	logger logr.Logger
}

type ApplicationInfo struct {
	ID                      string
	MaxProcessedStageID     int
	ApplicationName         string
	SparkProperties         map[string]string
	TotalNewInputBytes      int64
	TotalNewOutputBytes     int64
	TotalNewExecutorCpuTime int64
	Attempts                []sparkapiclient.Attempt
	Executors               []sparkapiclient.Executor
	WorkloadType            WorkloadType
}

var GetManager = func(clientSet kubernetes.Interface, driverPod *corev1.Pod, logger logr.Logger) (Manager, error) {
	client, err := getSparkApiClient(clientSet, driverPod, logger)
	if err != nil {
		return nil, fmt.Errorf("could not get spark api client, %w", err)
	}
	return manager{
		client:  client,
		logger:  logger,
	}, nil
}

func getSparkApiClient(clientSet kubernetes.Interface, driverPod *corev1.Pod, logger logr.Logger) (sparkapiclient.Client, error) {

	// Try the driver API first, to get information on running applications
	// Once the application is finished the info is written to history server

	// Get client for driver pod
	if isSparkDriverRunning(driverPod) {
		return sparkapiclient.NewDriverPodClient(driverPod, clientSet), nil
	}

	logger.Info("Driver pod/container not running, will use history server Spark API client")

	// Get client for history server
	historyServerService, err := getHistoryServerService(clientSet, logger)
	if err != nil {
		return nil, fmt.Errorf("could not get history server service, %w", err)
	}

	return sparkapiclient.NewHistoryServerClient(historyServerService, clientSet), nil
}

func (m manager) GetApplicationInfo(applicationID string, maxProcessedStageID int, log logr.Logger) (*ApplicationInfo, error) {

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

	stages, err := m.client.GetStages(applicationID)
	if err != nil {
		return nil, fmt.Errorf("could not get stages, %w", err)
	}

	if stages == nil {
		return nil, fmt.Errorf("stages are nil")
	}

	stageAggregationResult := aggregateStagesWindow(stages, maxProcessedStageID, log)
	applicationInfo.TotalNewOutputBytes = stageAggregationResult.totalNewOutputBytes
	applicationInfo.TotalNewInputBytes = stageAggregationResult.totalNewInputBytes
	applicationInfo.TotalNewExecutorCpuTime = stageAggregationResult.totalNewExecutorCpuTime
	applicationInfo.MaxProcessedStageID = stageAggregationResult.newMaxProcessedStageID

	executors, err := m.client.GetAllExecutors(applicationID)
	if err != nil {
		return nil, fmt.Errorf("could not get executors, %w", err)
	}

	applicationInfo.Executors = executors

	workloadType := m.getWorkloadType(applicationID)
	applicationInfo.WorkloadType = workloadType

	registry.Register(applicationInfo)

	return applicationInfo, nil
}

func (m manager) getWorkloadType(applicationID string) WorkloadType {
	// Streaming statistics endpoint is only available on running driver
	if m.client.GetClientType() == sparkapiclient.DriverClient {
		_, err := m.client.GetStreamingStatistics(applicationID)
		if err == nil {
			return SparkStreaming
		}
	}
	return ""
}

type stageWindowAggregationResult struct {
	totalNewOutputBytes     int64
	totalNewInputBytes      int64
	totalNewExecutorCpuTime int64
	newMaxProcessedStageID  int
}

func aggregateStagesWindow(stages []sparkapiclient.Stage, oldMaxProcessedStageID int, log logr.Logger) stageWindowAggregationResult {

	// TODO Use proper metrics, not the REST API
	// The REST API only gives us the last ~1000 stages by default.
	// Let's only aggregate stage metrics from the stages we have not processed yet

	var totalNewExecutorCpuTime int64
	var totalNewInputBytes int64
	var totalNewOutputBytes int64

	// If the stage window has advanced,
	// and we don't find this stage ID in the window,
	// it means we have missed some stages
	expectedStageID := oldMaxProcessedStageID + 1
	foundExpectedStageID := false
	windowHasAdvanced := false

	// The max and min IDs of the received stages
	stageWindowMaxID := -1
	stageWindowMinID := -1

	// We can include stages up to this ID - 1 in our aggregation,
	// all stages up to this number should be finalized
	minNotFinalizedStageID := -1

	// First pass, analysis
	for _, stage := range stages {

		// Figure out min stage ID that is still active
		if !isStageFinalized(stage) {
			if minNotFinalizedStageID == -1 {
				minNotFinalizedStageID = stage.StageID
			}
			if stage.StageID < minNotFinalizedStageID {
				minNotFinalizedStageID = stage.StageID
			}
		}

		// Figure out the current stage window (logging purposes only)
		if stageWindowMinID == -1 {
			stageWindowMinID = stage.StageID
		}
		if stage.StageID < stageWindowMinID {
			stageWindowMinID = stage.StageID
		}
		if stageWindowMaxID == -1 {
			stageWindowMaxID = stage.StageID
		}
		if stage.StageID > stageWindowMaxID {
			stageWindowMaxID = stage.StageID
		}

		if stage.StageID > oldMaxProcessedStageID {
			windowHasAdvanced = true
		}

		if stage.StageID == expectedStageID {
			foundExpectedStageID = true
		}
	}

	// We only want to include stages in (aggregationWindowMin, aggregationWindowMax) (non-inclusive)
	aggregationWindowMin := oldMaxProcessedStageID // We have already processed stages up to and including oldMaxProcessedStageID
	aggregationWindowMax := minNotFinalizedStageID // Stages with IDs >= minNotFinalizedStageID may still be in progress

	newMaxProcessedStageID := oldMaxProcessedStageID

	// Second pass, aggregation
	for _, stage := range stages {

		// Only include stages within our aggregation window
		if stage.StageID <= aggregationWindowMin {
			continue
		}
		if aggregationWindowMax != -1 {
			// We have an upper bound on the aggregation window
			if stage.StageID >= aggregationWindowMax {
				continue
			}
		}

		totalNewExecutorCpuTime += stage.ExecutorCpuTime
		totalNewInputBytes += stage.InputBytes
		totalNewOutputBytes += stage.OutputBytes

		// Remember new max processed stage ID
		if stage.StageID > newMaxProcessedStageID {
			newMaxProcessedStageID = stage.StageID
		}
	}

	log.Info("Finished processing stage window", "stageCount", len(stages),
		"minStageID", stageWindowMinID, "maxStageID", stageWindowMaxID,
		"aggregationWindow", fmt.Sprintf("(%d,%d)", aggregationWindowMin, aggregationWindowMax),
		"oldMaxProcessedStageID", oldMaxProcessedStageID, "newMaxProcessedStageID", newMaxProcessedStageID)

	if !foundExpectedStageID && windowHasAdvanced {
		// Let's just log an error
		err := fmt.Errorf("did not find expected stage ID %d in stage window", expectedStageID)
		log.Error(err, "missing stage metrics")
	}

	return stageWindowAggregationResult{
		totalNewOutputBytes:     totalNewOutputBytes,
		totalNewInputBytes:      totalNewInputBytes,
		totalNewExecutorCpuTime: totalNewExecutorCpuTime,
		newMaxProcessedStageID:  newMaxProcessedStageID,
	}
}

func isStageFinalized(stage sparkapiclient.Stage) bool {
	// Stages can have the following statuses:
	// ACTIVE, COMPLETE, FAILED, PENDING, SKIPPED
	switch stage.Status {
	case "COMPLETE", "FAILED", "SKIPPED":
		return true
	default:
		return false
	}
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
