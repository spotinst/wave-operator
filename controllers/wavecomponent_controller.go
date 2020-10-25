/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"

	"github.com/go-logr/logr"
	v1alpha1 "github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/controllers/internal/components"
	"github.com/spotinst/wave-operator/install"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// WaveComponentReconciler reconciles a WaveComponent object
type WaveComponentReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=wave.spot.io,resources=wavecomponents,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=wave.spot.io,resources=wavecomponents/status,verbs=get;update;patch

func (r *WaveComponentReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := r.Log.WithValues("wavecomponent", req.NamespacedName)

	comp := &v1alpha1.WaveComponent{}
	var err error
	err = r.Get(ctx, req.NamespacedName, comp)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err, "cannot retrieve")
		}
		return ctrl.Result{}, nil
	}

	if comp.Spec.Type != v1alpha1.HelmComponentType {
		new := comp.DeepCopy()
		condition := NewWaveComponentCondition(
			v1alpha1.WaveComponentFailure,
			v1.ConditionTrue,
			UnsupportedTypeReason,
			"Only helm charts are supported",
		)
		changed := SetWaveComponentCondition(&(new.Status), *condition)
		if changed {
			err := r.Client.Patch(ctx, new, client.MergeFrom(comp))
			if err != nil {
				log.Error(err, "patch error")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// get status of component
	new := comp.DeepCopy()
	condition, err := r.GetCurrentCondition(comp)
	if condition != nil {
		changed := SetWaveComponentCondition(&(new.Status), *condition)
		if changed || condition.Status != v1.ConditionTrue {
			installer := install.NewHelmInstaller(r.Log)
			err = installer.Install(string(comp.Spec.Name), comp.Spec.URL, comp.Spec.Version, comp.Spec.ValuesConfiguration)
			if err != nil {
				log.Error(err, "cannot install")
				return ctrl.Result{}, err
			}
		}
	}
	return ctrl.Result{}, nil
}

func (r *WaveComponentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.WaveComponent{}).
		Complete(r)
}

// GetCurrentCondition examines the current environment and returns a condition for the component
func (r *WaveComponentReconciler) GetCurrentCondition(comp *v1alpha1.WaveComponent) (*v1alpha1.WaveComponentCondition, error) {

	switch comp.Spec.Name {
	case v1alpha1.SparkHistoryChartName:
		return components.GetSparkHistoryCondition()
	case v1alpha1.EnterpriseGatewayChartName:
	case v1alpha1.SparkOperatorChartName:
	case v1alpha1.WaveIngressChartName:
	default:
		// (a) check helm
		// (b) return not installed
		return NewWaveComponentCondition(
				v1alpha1.WaveComponentProgressing,
				v1.ConditionFalse,
				UninstalledReason,
				"component not installed",
			),
			nil
	}
	return nil, nil
}
