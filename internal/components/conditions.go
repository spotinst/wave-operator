package components

import (
	"github.com/spotinst/wave-operator/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NewWaveComponentCondition creates a new WaveComponent condition.
func NewWaveComponentCondition(condType v1alpha1.WaveComponentConditionType, status v1.ConditionStatus, reason, message string) *v1alpha1.WaveComponentCondition {
	return &v1alpha1.WaveComponentCondition{
		Type:               condType,
		Status:             status,
		LastUpdateTime:     v12.Now(),
		LastTransitionTime: v12.Now(),
		Reason:             reason,
		Message:            message,
	}
}
