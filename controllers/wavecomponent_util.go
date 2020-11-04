package controllers

import (
	"github.com/spotinst/wave-operator/api/v1alpha1"
)

const (
	// ConditionReasons
	//
	// Progressing
	UninstalledReason        = "Uninstalled"
	InstallingReason         = "Installing"
	UpgradingReason          = "Upgrading"
	DeletingReason           = "Deleting"
	InProgressReason         = "InProgress"
	InstallationFailedReason = "HelmFailure"
	// Available
	AvailableReason = "Available"
	// Degraded
	// Failed
	UnsupportedTypeReason = "UnsupportedComponentType"
)

// GetWaveComponentCondition returns the condition with the provided type.
func GetWaveComponentCondition(status v1alpha1.WaveComponentStatus, condType v1alpha1.WaveComponentConditionType) *v1alpha1.WaveComponentCondition {
	for i := range status.Conditions {
		c := status.Conditions[i]
		if c.Type == condType {
			return &c
		}
	}
	return nil
}

// SetWaveComponentCondition updates the WaveComponent to include the provided condition. If the condition that
// we are about to add already exists and has the same status and reason then we are not going to update.
func SetWaveComponentCondition(status *v1alpha1.WaveComponentStatus, condition v1alpha1.WaveComponentCondition) bool {
	currentCond := GetWaveComponentCondition(*status, condition.Type)
	if currentCond != nil && currentCond.Status == condition.Status && currentCond.Reason == condition.Reason {
		return false
	}
	// Do not update lastTransitionTime if the status of the condition doesn't change.
	if currentCond != nil && currentCond.Status == condition.Status {
		condition.LastTransitionTime = currentCond.LastTransitionTime
	}
	newConditions := filterOutCondition(status.Conditions, condition.Type)
	status.Conditions = append(newConditions, condition)
	return true
}

// RemoveWaveComponentCondition removes the WaveComponent condition with the provided type.
func RemoveWaveComponentCondition(status *v1alpha1.WaveComponentStatus, condType v1alpha1.WaveComponentConditionType) {
	status.Conditions = filterOutCondition(status.Conditions, condType)
}

// filterOutCondition returns a new slice of WaveComponent conditions without conditions with the provided type.
func filterOutCondition(conditions []v1alpha1.WaveComponentCondition, condType v1alpha1.WaveComponentConditionType) []v1alpha1.WaveComponentCondition {
	var newConditions []v1alpha1.WaveComponentCondition
	for _, c := range conditions {
		if c.Type == condType {
			continue
		}
		newConditions = append(newConditions, c)
	}
	return newConditions
}
