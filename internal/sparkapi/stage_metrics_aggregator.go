package sparkapi

import (
	"fmt"

	"github.com/go-logr/logr"

	sparkapiclient "github.com/spotinst/wave-operator/internal/sparkapi/client"
)

type stageMetricsAggregator interface {
	processWindow(stages []sparkapiclient.Stage) stageWindowAggregationResult
}

type aggregator struct {
	state StageMetricsAggregatorState
	log   logr.Logger
}

func newStageMetricsAggregator(log logr.Logger, state StageMetricsAggregatorState) stageMetricsAggregator {
	return aggregator{
		state: state,
		log:   log,
	}
}

type stageWindowAggregationResult struct {
	totalNewOutputBytes     int64
	totalNewInputBytes      int64
	totalNewExecutorCpuTime int64
	newState                StageMetricsAggregatorState
}

type StageMetricsAggregatorState struct {
	MaxProcessedFinalizedStageID int                  `json:"maxProcessedFinalizedStageId"`
	ActiveStageMetrics           map[int]StageMetrics `json:"activeStageMetrics"`
}

type StageMetrics struct {
	OutputBytes int64 `json:"outputBytes"`
	InputBytes  int64 `json:"inputBytes"`
	CPUTime     int64 `json:"cpuTime"`
}

func (a aggregator) processWindow(stages []sparkapiclient.Stage) stageWindowAggregationResult {

	// TODO Use proper metrics, not the REST API
	// The REST API only gives us the last ~1000 stages by default.
	// Let's only aggregate stage metrics from the stages we have not processed yet

	finalized, active := a.groupStages(stages)
	minID, maxID := a.getMinMaxIds(stages)

	windowHasAdvanced := false
	_, maxFinalizedId := a.getMinMaxIds(finalized)
	if maxFinalizedId > a.state.MaxProcessedFinalizedStageID {
		windowHasAdvanced = true
	}

	// If the stage window has advanced and we don't find this stage ID in the window
	// it means we have missed some stages
	expectedStageID := a.state.MaxProcessedFinalizedStageID + 1
	_, foundExpectedStageID := a.getStageById(stages, expectedStageID)

	if !foundExpectedStageID && windowHasAdvanced {
		// Let's just log an error
		err := fmt.Errorf("did not find expected stage ID %d in stage window", expectedStageID)
		a.log.Error(err, "missing stage metrics")
	}

	windowAggregate := &StageMetrics{}
	newState := StageMetricsAggregatorState{
		MaxProcessedFinalizedStageID: a.state.MaxProcessedFinalizedStageID,
		ActiveStageMetrics:           make(map[int]StageMetrics),
	}

	// Aggregate finalized stages
	for _, stage := range finalized {
		if stage.StageID <= a.state.MaxProcessedFinalizedStageID {
			// Was this stage previously active, and just finalized? (stages finalize out of order)
			_, ok := a.state.ActiveStageMetrics[stage.StageID]
			if !ok {
				// We have already fully processed this stage
				continue
			}
		}
		a.addStageToMetrics(windowAggregate, a.state, stage)
		// Remember new max processed stage ID
		if stage.StageID > newState.MaxProcessedFinalizedStageID {
			newState.MaxProcessedFinalizedStageID = stage.StageID
		}
	}

	// Aggregate active stages
	for _, stage := range active {
		a.addStageToMetrics(windowAggregate, a.state, stage)
		newState.ActiveStageMetrics[stage.StageID] = StageMetrics{
			OutputBytes: stage.OutputBytes,
			InputBytes:  stage.InputBytes,
			CPUTime:     stage.ExecutorCpuTime,
		}
	}

	a.log.Info("Finished processing stage window", "stageCount", len(stages),
		"minStageID", minID, "maxStageID", maxID,
		"oldMaxProcessedFinalizedStageID", a.state.MaxProcessedFinalizedStageID,
		"newMaxProcessedFinalizedStageID", newState.MaxProcessedFinalizedStageID)

	return stageWindowAggregationResult{
		totalNewOutputBytes:     windowAggregate.OutputBytes,
		totalNewInputBytes:      windowAggregate.InputBytes,
		totalNewExecutorCpuTime: windowAggregate.CPUTime,
		newState:                newState,
	}
}

func (a aggregator) addStageToMetrics(aggregatedMetrics *StageMetrics, oldState StageMetricsAggregatorState, stage sparkapiclient.Stage) {
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

func (a aggregator) getStageById(stages []sparkapiclient.Stage, id int) (sparkapiclient.Stage, bool) {
	for _, stage := range stages {
		if stage.StageID == id {
			return stage, true
		}
	}
	return sparkapiclient.Stage{}, false
}

func (a aggregator) getMinMaxIds(stages []sparkapiclient.Stage) (minID, maxID int) {
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

func (a aggregator) groupStages(stages []sparkapiclient.Stage) (finalizedStages, activeStages []sparkapiclient.Stage) {
	finalizedStages = make([]sparkapiclient.Stage, 0)
	activeStages = make([]sparkapiclient.Stage, 0)
	for _, stage := range stages {
		if a.isStageFinalized(stage) {
			finalizedStages = append(finalizedStages, stage)
		} else {
			activeStages = append(activeStages, stage)
		}
	}
	return finalizedStages, activeStages
}

func (a aggregator) isStageFinalized(stage sparkapiclient.Stage) bool {
	// Stages can have the following statuses:
	// ACTIVE, COMPLETE, FAILED, PENDING, SKIPPED
	switch stage.Status {
	case "COMPLETE", "FAILED", "SKIPPED":
		return true
	default:
		return false
	}
}
