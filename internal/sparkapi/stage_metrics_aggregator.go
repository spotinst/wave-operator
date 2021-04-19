package sparkapi

import (
	"fmt"
	"strconv"
	"strings"

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
	MaxProcessedFinalizedStage StageKey                  `json:"maxProcessedFinalizedStage"`
	ActiveStageMetrics         map[StageKey]StageMetrics `json:"activeStageMetrics"`
	PendingStages              []StageKey                `json:"pendingStages"`
}

type StageMetrics struct {
	OutputBytes int64 `json:"outputBytes"`
	InputBytes  int64 `json:"inputBytes"`
	CPUTime     int64 `json:"cpuTime"`
}

// StageKey uniquely identifies a stage attempt
type StageKey struct {
	StageID   int `json:"stageId"`
	AttemptID int `json:"attemptId"`
}

func (k StageKey) MarshalText() (text []byte, err error) {
	s := fmt.Sprintf("%d_%d", k.StageID, k.AttemptID)
	return []byte(s), nil
}

func (k *StageKey) UnmarshalText(text []byte) error {
	s := string(text)
	split := strings.Split(s, "_")
	if len(split) != 2 {
		return fmt.Errorf("illegal value %q", s)
	}
	stageID, err := strconv.Atoi(split[0])
	if err != nil {
		return err
	}
	attemptID, err := strconv.Atoi(split[1])
	if err != nil {
		return err
	}
	k.StageID = stageID
	k.AttemptID = attemptID
	return nil
}

func (k StageKey) compare(that StageKey) int {
	if k.StageID < that.StageID {
		return -1
	}
	if k.StageID > that.StageID {
		return 1
	}
	// Stage IDs are equal, check attempt IDs
	if k.AttemptID < that.AttemptID {
		return -1
	}
	if k.AttemptID > that.AttemptID {
		return 1
	}
	// Stage IDs and attempt IDs are equal
	return 0
}

func getStageKey(s sparkapiclient.Stage) StageKey {
	return StageKey{
		StageID:   s.StageID,
		AttemptID: s.AttemptID,
	}
}

func newStageKey() StageKey {
	return StageKey{
		StageID:   -1,
		AttemptID: -1,
	}
}

func NewStageMetricsAggregatorState() StageMetricsAggregatorState {
	return StageMetricsAggregatorState{
		MaxProcessedFinalizedStage: newStageKey(),
		ActiveStageMetrics:         make(map[StageKey]StageMetrics),
		PendingStages:              make([]StageKey, 0),
	}
}

func (a aggregator) processWindow(stages []sparkapiclient.Stage) stageWindowAggregationResult {

	// TODO Use proper metrics, not the REST API
	// The REST API only gives us the last ~1000 stages by default.
	// Let's only aggregate stage metrics from the stages we have not processed yet

	finalized, active, pending := a.groupStages(stages)
	minStage, maxStage := a.getMinMaxStages(stages)

	windowHasAdvanced := false
	_, maxFinalized := a.getMinMaxStages(finalized)
	if maxFinalized.compare(a.state.MaxProcessedFinalizedStage) > 0 {
		windowHasAdvanced = true
	}

	// If the stage window has advanced and we don't find one of these stages in the window
	// it means we have missed some stages
	var foundExpectedStage bool
	expectedStages := []StageKey{
		{
			StageID:   a.state.MaxProcessedFinalizedStage.StageID + 1,
			AttemptID: 0,
		},
		{
			StageID:   a.state.MaxProcessedFinalizedStage.StageID,
			AttemptID: a.state.MaxProcessedFinalizedStage.AttemptID + 1,
		},
	}
	for _, expected := range expectedStages {
		_, ok := a.getStageByKey(stages, expected)
		if ok {
			foundExpectedStage = true
			break
		}
	}

	if !foundExpectedStage && windowHasAdvanced {
		// Let's just log an error
		err := fmt.Errorf("did not find expected stages %+v in stage window", expectedStages)
		a.log.Error(err, "missing stage metrics")
	}

	windowAggregate := &StageMetrics{}
	newState := NewStageMetricsAggregatorState()
	newState.MaxProcessedFinalizedStage = a.state.MaxProcessedFinalizedStage

	// Aggregate finalized stages
	for _, stage := range finalized {
		key := getStageKey(stage)
		if key.compare(a.state.MaxProcessedFinalizedStage) <= 0 {
			// Was this stage previously active or pending, and just finalized? (stages finalize out of order)
			_, stageWasActive := a.state.ActiveStageMetrics[key]
			stageWasPending := a.wasStagePending(stage)
			if !(stageWasActive || stageWasPending) {
				continue
			}
		}
		a.addStageToMetrics(windowAggregate, stage)
		// Remember new max processed stage ID
		if key.compare(newState.MaxProcessedFinalizedStage) > 0 {
			newState.MaxProcessedFinalizedStage = key
		}
	}

	// Aggregate active stages
	for _, stage := range active {
		a.addStageToMetrics(windowAggregate, stage)
		newState.ActiveStageMetrics[getStageKey(stage)] = StageMetrics{
			OutputBytes: stage.OutputBytes,
			InputBytes:  stage.InputBytes,
			CPUTime:     stage.ExecutorCpuTime,
		}
	}

	// Aggregate pending stages
	for _, stage := range pending {
		newState.PendingStages = append(newState.PendingStages, getStageKey(stage))
	}

	a.log.Info("Finished processing stage window", "stageCount", len(stages),
		"minStage", minStage, "maxStage", maxStage,
		"oldMaxProcessedFinalizedStage", a.state.MaxProcessedFinalizedStage,
		"newMaxProcessedFinalizedStage", newState.MaxProcessedFinalizedStage)

	return stageWindowAggregationResult{
		totalNewOutputBytes:     windowAggregate.OutputBytes,
		totalNewInputBytes:      windowAggregate.InputBytes,
		totalNewExecutorCpuTime: windowAggregate.CPUTime,
		newState:                newState,
	}
}

func (a aggregator) addStageToMetrics(aggregatedMetrics *StageMetrics, stage sparkapiclient.Stage) {
	aggregatedMetrics.CPUTime += stage.ExecutorCpuTime
	aggregatedMetrics.InputBytes += stage.InputBytes
	aggregatedMetrics.OutputBytes += stage.OutputBytes

	// Subtract values that we may have added to the aggregate previously
	alreadyAdded, ok := a.state.ActiveStageMetrics[getStageKey(stage)]
	if ok {
		aggregatedMetrics.CPUTime -= alreadyAdded.CPUTime
		aggregatedMetrics.InputBytes -= alreadyAdded.InputBytes
		aggregatedMetrics.OutputBytes -= alreadyAdded.OutputBytes
	}
}

func (a aggregator) getStageByKey(stages []sparkapiclient.Stage, key StageKey) (sparkapiclient.Stage, bool) {
	for _, stage := range stages {
		if getStageKey(stage).compare(key) == 0 {
			return stage, true
		}
	}
	return sparkapiclient.Stage{}, false
}

func (a aggregator) getMinMaxStages(stages []sparkapiclient.Stage) (min, max StageKey) {
	min = newStageKey()
	max = newStageKey()
	for _, stage := range stages {
		current := getStageKey(stage)
		if min.compare(newStageKey()) == 0 {
			min = current
		}
		if current.compare(min) < 0 {
			min = current
		}
		if max.compare(newStageKey()) == 0 {
			max = current
		}
		if current.compare(max) > 0 {
			max = current
		}
	}
	return min, max
}

func (a aggregator) groupStages(stages []sparkapiclient.Stage) (finalizedStages, activeStages, pendingStages []sparkapiclient.Stage) {
	finalizedStages = make([]sparkapiclient.Stage, 0)
	activeStages = make([]sparkapiclient.Stage, 0)
	pendingStages = make([]sparkapiclient.Stage, 0)
	for _, stage := range stages {
		if a.isStageFinalized(stage) {
			finalizedStages = append(finalizedStages, stage)
		} else if a.isStageActive(stage) {
			activeStages = append(activeStages, stage)
		} else if a.isStagePending(stage) {
			pendingStages = append(pendingStages, stage)
		}
	}
	return finalizedStages, activeStages, pendingStages
}

func (a aggregator) wasStagePending(stage sparkapiclient.Stage) bool {
	for _, pending := range a.state.PendingStages {
		if getStageKey(stage).compare(pending) == 0 {
			return true
		}
	}
	return false
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

func (a aggregator) isStageActive(stage sparkapiclient.Stage) bool {
	return stage.Status == "ACTIVE"
}

func (a aggregator) isStagePending(stage sparkapiclient.Stage) bool {
	return stage.Status == "PENDING"
}
