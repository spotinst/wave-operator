package sparkapi

import (
	"fmt"
	"github.com/go-logr/logr"
	sparkapiclient "github.com/spotinst/wave-operator/internal/sparkapi/client"
)

type stageWindowAggregationResult struct {
	totalNewOutputBytes     int64
	totalNewInputBytes      int64
	totalNewExecutorCpuTime int64
	newState                StageMetricsAggregationState
}

type StageMetricsAggregationState struct {
	MaxProcessedFinalizedStageID int                  `json:"maxProcessedFinalizedStageId"`
	ActiveStageMetrics           map[int]StageMetrics `json:"activeStageMetrics"`
}

type StageMetrics struct {
	OutputBytes int64 `json:"outputBytes"`
	InputBytes  int64 `json:"inputBytes"`
	CPUTime     int64 `json:"cpuTime"`
}

func aggregateStagesWindow(stages []sparkapiclient.Stage, oldState StageMetricsAggregationState, log logr.Logger) stageWindowAggregationResult {

	// TODO Use proper metrics, not the REST API
	// The REST API only gives us the last ~1000 stages by default.
	// Let's only aggregate stage metrics from the stages we have not processed yet

	finalized, active := groupStages(stages)
	minID, maxID := getMinMaxIds(stages)

	windowHasAdvanced := false
	_, maxFinalizedId := getMinMaxIds(finalized)
	if maxFinalizedId > oldState.MaxProcessedFinalizedStageID {
		windowHasAdvanced = true
	}

	// If the stage window has advanced and we don't find this stage ID in the window
	// it means we have missed some stages
	expectedStageID := oldState.MaxProcessedFinalizedStageID + 1
	_, foundExpectedStageID := getStageById(stages, expectedStageID)

	if !foundExpectedStageID && windowHasAdvanced {
		// Let's just log an error
		err := fmt.Errorf("did not find expected stage ID %d in stage window", expectedStageID)
		log.Error(err, "missing stage metrics")
	}

	windowAggregate := &StageMetrics{}
	newState := StageMetricsAggregationState{
		MaxProcessedFinalizedStageID: oldState.MaxProcessedFinalizedStageID,
		ActiveStageMetrics:           make(map[int]StageMetrics),
	}

	// Aggregate finalized stages
	for _, stage := range finalized {
		if stage.StageID <= oldState.MaxProcessedFinalizedStageID {
			// Was this stage previously active, and just finalized? (stages finalize out of order)
			_, ok := oldState.ActiveStageMetrics[stage.StageID]
			if !ok {
				// We have already fully processed this stage
				continue
			}
		}
		addStageToMetrics(windowAggregate, oldState, stage)
		// Remember new max processed stage ID
		if stage.StageID > newState.MaxProcessedFinalizedStageID {
			newState.MaxProcessedFinalizedStageID = stage.StageID
		}
	}

	// Aggregate active stages
	for _, stage := range active {
		addStageToMetrics(windowAggregate, oldState, stage)
		newState.ActiveStageMetrics[stage.StageID] = StageMetrics{
			OutputBytes: stage.OutputBytes,
			InputBytes:  stage.InputBytes,
			CPUTime:     stage.ExecutorCpuTime,
		}
	}

	log.Info("Finished processing stage window", "stageCount", len(stages),
		"minStageID", minID, "maxStageID", maxID,
		"oldMaxProcessedFinalizedStageID", oldState.MaxProcessedFinalizedStageID,
		"newMaxProcessedFinalizedStageID", newState.MaxProcessedFinalizedStageID)

	return stageWindowAggregationResult{
		totalNewOutputBytes:     windowAggregate.OutputBytes,
		totalNewInputBytes:      windowAggregate.InputBytes,
		totalNewExecutorCpuTime: windowAggregate.CPUTime,
		newState:                newState,
	}
}

func addStageToMetrics(aggregatedMetrics *StageMetrics, oldState StageMetricsAggregationState, stage sparkapiclient.Stage) {
	aggregatedMetrics.CPUTime += stage.ExecutorCpuTime
	aggregatedMetrics.InputBytes += stage.InputBytes
	aggregatedMetrics.OutputBytes += stage.OutputBytes

	// Subtract values that we may have added to the aggregate previously
	alreadyAdded, ok := oldState.ActiveStageMetrics[stage.StageID]
	if ok {
		aggregatedMetrics.CPUTime -= alreadyAdded.CPUTime
		aggregatedMetrics.InputBytes -= alreadyAdded.InputBytes
		aggregatedMetrics.OutputBytes -= alreadyAdded.OutputBytes
	}
}

func getStageById(stages []sparkapiclient.Stage, id int) (sparkapiclient.Stage, bool) {
	for _, stage := range stages {
		if stage.StageID == id {
			return stage, true
		}
	}
	return sparkapiclient.Stage{}, false
}

func getMinMaxIds(stages []sparkapiclient.Stage) (minID, maxID int) {
	minID = -1
	maxID = -1
	for _, stage := range stages {
		if minID == -1 {
			minID = stage.StageID
		}
		if stage.StageID < minID {
			minID = stage.StageID
		}
		if maxID == -1 {
			maxID = stage.StageID
		}
		if stage.StageID > maxID {
			maxID = stage.StageID
		}
	}
	return minID, maxID
}

func groupStages(stages []sparkapiclient.Stage) (finalizedStages, activeStages []sparkapiclient.Stage) {
	finalizedStages = make([]sparkapiclient.Stage, 0)
	activeStages = make([]sparkapiclient.Stage, 0)
	for _, stage := range stages {
		if isStageFinalized(stage) {
			finalizedStages = append(finalizedStages, stage)
		} else {
			activeStages = append(activeStages, stage)
		}
	}
	return finalizedStages, activeStages
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
