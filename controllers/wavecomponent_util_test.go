package controllers

import (
	"testing"
	"time"

	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/internal/components"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSortMostRecent(t *testing.T) {
	c1 := components.NewWaveComponentCondition(
		v1alpha1.WaveComponentProgressing,
		v1.ConditionTrue,
		InstallingReason,
		"Helm installation started",
	)
	c2 := components.NewWaveComponentCondition(
		v1alpha1.WaveComponentAvailable,
		v1.ConditionFalse,
		"PodsUnavailable",
		"Helm installation started",
	)
	c3 := components.NewWaveComponentCondition(
		v1alpha1.WaveComponentFailure,
		v1.ConditionTrue,
		InstallationFailedReason,
		"Helm installation started",
	)
	c1.LastUpdateTime = metav1.Date(2020, 11, 16, 17, 30, 0, 0, time.Local)
	c2.LastUpdateTime = metav1.Date(2020, 11, 16, 17, 30, 10, 0, time.Local)
	c3.LastUpdateTime = metav1.Date(2020, 11, 16, 17, 30, 20, 0, time.Local)

	wc := v1alpha1.WaveComponent{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-component",
		},
		Spec: v1alpha1.WaveComponentSpec{},
		Status: v1alpha1.WaveComponentStatus{
			Conditions: []v1alpha1.WaveComponentCondition{*c2, *c3, *c1}},
	}
	sortMostRecent(&wc.Status)
	assert.Equal(t, c3.Type, wc.Status.Conditions[0].Type)

}
