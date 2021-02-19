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
)

type Manager interface {
	GetApplicationInfo(applicationId string, maxProcessedStageId int, log logr.Logger) (*ApplicationInfo, error)
}

type manager struct {
	client sparkapiclient.Client
	logger logr.Logger
}

type ApplicationInfo struct {
	MaxProcessedStageId     int
	ApplicationName         string
	SparkProperties         map[string]string
	TotalNewInputBytes      int64
	TotalNewOutputBytes     int64
	TotalNewExecutorCpuTime int64
	Attempts                []sparkapiclient.Attempt
	Executors               []sparkapiclient.Executor
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

	logger.Info("Driver pod/container not running, will use history server Spark API client")

	// Get client for history server
	historyServerService, err := getHistoryServerService(clientSet)
	if err != nil {
		logger.Info(fmt.Sprintf("Could not get history server service, error: %s", err.Error()))
	} else {
		return sparkapiclient.NewHistoryServerClient(historyServerService, clientSet), nil
	}

	return nil, fmt.Errorf("could not get spark api client")
}

func (m manager) GetApplicationInfo(applicationId string, maxProcessedStageId int, log logr.Logger) (*ApplicationInfo, error) {

	applicationInfo := &ApplicationInfo{}

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

	stageAggregationResult := aggregateStagesWindow(stages, maxProcessedStageId, log)
	applicationInfo.TotalNewOutputBytes = stageAggregationResult.totalNewOutputBytes
	applicationInfo.TotalNewInputBytes = stageAggregationResult.totalNewInputBytes
	applicationInfo.TotalNewExecutorCpuTime = stageAggregationResult.totalNewExecutorCpuTime
	applicationInfo.MaxProcessedStageId = stageAggregationResult.newMaxProcessedStageId

	executors, err := m.client.GetAllExecutors(applicationId)
	if err != nil {
		return nil, fmt.Errorf("could not get executors, %w", err)
	}

	applicationInfo.Executors = executors

	return applicationInfo, nil
}

type stageWindowAggregationResult struct {
	totalNewOutputBytes     int64
	totalNewInputBytes      int64
	totalNewExecutorCpuTime int64
	newMaxProcessedStageId  int
}

func aggregateStagesWindow(stages []sparkapiclient.Stage, oldMaxProcessedStageId int, log logr.Logger) stageWindowAggregationResult {

	// TODO Use proper metrics, not the REST API
	// The REST API only gives us the last ~1000 stages by default.
	// Let's only aggregate stage metrics from the stages we have not processed yet

	var totalNewExecutorCpuTime int64
	var totalNewInputBytes int64
	var totalNewOutputBytes int64

	// If the stage window has advanced,
	// and we don't find this stage ID in the window,
	// it means we have missed some stages
	expectedStageId := oldMaxProcessedStageId + 1
	foundExpectedStageId := false
	windowHasAdvanced := false

	// The max and min IDs of the received stages
	stageWindowMaxId := -1
	stageWindowMinId := -1

	// We can include stages up to this ID - 1 in our aggregation,
	// all stages up to this number should be finalized
	minNotFinalizedStageId := -1

	// First pass, analysis
	for _, stage := range stages {

		// Figure out min stage ID that is still active
		if !isStageFinalized(stage) {
			if minNotFinalizedStageId == -1 {
				minNotFinalizedStageId = stage.StageId
			}
			if stage.StageId < minNotFinalizedStageId {
				minNotFinalizedStageId = stage.StageId
			}
		}

		// Figure out the current stage window (logging purposes only)
		if stageWindowMinId == -1 {
			stageWindowMinId = stage.StageId
		}
		if stage.StageId < stageWindowMinId {
			stageWindowMinId = stage.StageId
		}
		if stageWindowMaxId == -1 {
			stageWindowMaxId = stage.StageId
		}
		if stage.StageId > stageWindowMaxId {
			stageWindowMaxId = stage.StageId
		}

		if stage.StageId > oldMaxProcessedStageId {
			windowHasAdvanced = true
		}

		if stage.StageId == expectedStageId {
			foundExpectedStageId = true
		}
	}

	// We only want to include stages in (aggregationWindowMin, aggregationWindowMax) (non-inclusive)
	aggregationWindowMin := oldMaxProcessedStageId // We have already processed stages up to and including oldMaxProcessedStageId
	aggregationWindowMax := minNotFinalizedStageId // Stages with IDs >= minNotFinalizedStageId may still be in progress

	newMaxProcessedStageId := oldMaxProcessedStageId

	// Second pass, aggregation
	for _, stage := range stages {

		// Only include stages within our aggregation window
		if stage.StageId <= aggregationWindowMin {
			continue
		}
		if aggregationWindowMax != -1 {
			// We have an upper bound on the aggregation window
			if stage.StageId >= aggregationWindowMax {
				continue
			}
		}

		totalNewExecutorCpuTime += stage.ExecutorCpuTime
		totalNewInputBytes += stage.InputBytes
		totalNewOutputBytes += stage.OutputBytes

		// Remember new max processed stage ID
		if stage.StageId > newMaxProcessedStageId {
			newMaxProcessedStageId = stage.StageId
		}
	}

	log.Info("Finished processing stage window", "stageCount", len(stages),
		"minStageId", stageWindowMinId, "maxStageId", stageWindowMaxId,
		"aggregationWindow", fmt.Sprintf("(%d,%d)", aggregationWindowMin, aggregationWindowMax),
		"oldMaxProcessedStageId", oldMaxProcessedStageId, "newMaxProcessedStageId", newMaxProcessedStageId)

	if !foundExpectedStageId && windowHasAdvanced {
		// Let's just log an error
		err := fmt.Errorf("did not find expected stage ID %d in stage window", expectedStageId)
		log.Error(err, "missing stage metrics")
	}

	return stageWindowAggregationResult{
		totalNewOutputBytes:     totalNewOutputBytes,
		totalNewInputBytes:      totalNewInputBytes,
		totalNewExecutorCpuTime: totalNewExecutorCpuTime,
		newMaxProcessedStageId:  newMaxProcessedStageId,
	}
}

func isStageFinalized(stage sparkapiclient.Stage) bool {
	// Stages can have the following statuses:
	// ACTIVE, COMPLETE, FAILED, PENDING, SKIPPED
	switch stage.Status {
	case "ACTIVE":
		return false
	case "COMPLETE":
		return true
	case "FAILED":
		return true
	case "PENDING":
		return false
	case "SKIPPED":
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

func IsApplicationNotFoundError(err error) bool {
	if errors.As(err, &transport.UnknownAppError{}) {
		return true
	}
	return false
}
