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
	"time"

	"github.com/go-logr/logr"
	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/catalog"
	"github.com/spotinst/wave-operator/install"
	"github.com/spotinst/wave-operator/internal/components"
	"github.com/spotinst/wave-operator/internal/version"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	Wave                   = "wave"
	FinalizerName          = "operator.wave.spot.io"
	AnnotationWaveVersion  = "operator.wave.spot.io/version"
	AnnotationSparkVersion = "spark.wave.spot.io/version"
)

// WaveComponentReconciler reconciles a WaveComponent object
type WaveComponentReconciler struct {
	Client       client.Client
	Log          logr.Logger
	getClient    genericclioptions.RESTClientGetter
	getInstaller InstallerGetter
	scheme       *runtime.Scheme
}

// InstallerGetter is an factory function that returns an implementation of Installer
type InstallerGetter func(name string, getter genericclioptions.RESTClientGetter, log logr.Logger) install.Installer

func NewWaveComponentReconciler(
	client client.Client,
	config *rest.Config,
	installerGetter InstallerGetter,
	log logr.Logger,
	scheme *runtime.Scheme) *WaveComponentReconciler {

	kubeConfig := genericclioptions.NewConfigFlags(false)
	kubeConfig.APIServer = &config.Host
	kubeConfig.BearerToken = &config.BearerToken
	kubeConfig.CAFile = &config.CAFile
	ns := catalog.SystemNamespace
	kubeConfig.Namespace = &ns

	return &WaveComponentReconciler{
		Client:       client,
		getClient:    kubeConfig,
		getInstaller: installerGetter,
		Log:          log,
		scheme:       scheme,
	}
}

// helm requires cluster-admin access, but here we'll explicitly mention
// a few resources that the wave operator accesses directly

// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;
// +kubebuilder:rbac:groups="apps",resources=deployments,verbs=get;list;watch;create;update;patch;delete

// +kubebuilder:rbac:groups=wave.spot.io,resources=wavecomponents,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=wave.spot.io,resources=wavecomponents/status,verbs=get;update;patch

func (r *WaveComponentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("wavecomponent", req.NamespacedName)

	comp := &v1alpha1.WaveComponent{}
	err := r.Client.Get(ctx, req.NamespacedName, comp)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err, "cannot retrieve")
		}
		return ctrl.Result{}, nil
	}

	changed := r.setInitialValues(comp)

	if changed {
		err := r.Client.Update(ctx, comp)
		if err != nil {
			return ctrl.Result{}, err
		}

	}
	if !comp.ObjectMeta.DeletionTimestamp.IsZero() {
		resp, err := r.reconcileAbsent(ctx, req, comp)
		if err != nil {
			return resp, err
		}
		// remove finalizer, but fetch again since it's been patched
		err = r.Client.Get(ctx, req.NamespacedName, comp)
		if err != nil {
			if !k8serrors.IsNotFound(err) {
				log.Error(err, "cannot retrieve")
			}
			return ctrl.Result{}, nil
		}
		comp.ObjectMeta.Finalizers = removeString(comp.ObjectMeta.Finalizers, FinalizerName)
		err = r.Client.Update(ctx, comp)
		return resp, err
	}

	if comp.Spec.Type != v1alpha1.HelmComponentType {
		return r.unsupportedType(ctx, req, comp)
	}

	if comp.Spec.State == v1alpha1.PresentComponentState {
		return r.reconcilePresent(ctx, req, comp)
	} else {
		return r.reconcileAbsent(ctx, req, comp)
	}
}

func (r *WaveComponentReconciler) setInitialValues(comp *v1alpha1.WaveComponent) bool {
	changed := false
	if comp.ObjectMeta.DeletionTimestamp.IsZero() {
		if !containsString(comp.ObjectMeta.Finalizers, FinalizerName) {
			comp.ObjectMeta.Finalizers = append(comp.ObjectMeta.Finalizers, FinalizerName)
			changed = true
		}
	}
	if comp.Annotations == nil {
		comp.Annotations = make(map[string]string, 1)
	}
	if comp.Annotations[AnnotationWaveVersion] == "" {
		comp.Annotations[AnnotationWaveVersion] = version.BuildVersion
		changed = true
	}

	return changed
}

func removeString(names []string, name string) []string {
	m := []string{}
	k := 0
	for i := 0; i < len(names); i++ {
		if names[i] == name {
			m = append(m, names[k:i]...)
			k = i + 1
		}
	}
	return append(m, names[k:]...)
}

func containsString(names []string, name string) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

func (r *WaveComponentReconciler) unsupportedType(ctx context.Context, req ctrl.Request, comp *v1alpha1.WaveComponent) (ctrl.Result, error) {
	deepCopy := comp.DeepCopy()
	condition := components.NewWaveComponentCondition(
		v1alpha1.WaveComponentFailure,
		v1.ConditionTrue,
		UnsupportedTypeReason,
		"Only helm charts are supported",
	)
	changed := SetWaveComponentCondition(&(deepCopy.Status), *condition)
	if changed {
		err := r.Client.Patch(ctx, deepCopy, client.MergeFrom(comp))
		if err != nil {
			r.Log.Error(err, "patch error")
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *WaveComponentReconciler) reconcilePresent(ctx context.Context, req ctrl.Request, comp *v1alpha1.WaveComponent) (ctrl.Result, error) {
	log := r.Log.WithValues("wavecomponent", req.NamespacedName)

	i := r.getInstaller(Wave, r.getClient, log)

	inst, err := i.Get(i.GetReleaseName(req.Name))
	if err != nil {
		if err != install.ErrReleaseNotFound {
			return ctrl.Result{}, err
		} else {
			return r.install(ctx, log, comp)
		}
	}
	if i.IsUpgrade(comp, inst) {
		return r.upgrade(ctx, log, comp)
	}

	// release is present, and it's not an upgrade
	switch inst.Status {
	case install.Failed:
		// mark as failed, and return (a change in values should trigger an upgrade)
		deepCopy := comp.DeepCopy()
		condition := components.NewWaveComponentCondition(
			v1alpha1.WaveComponentFailure,
			v1.ConditionTrue,
			InstallationFailedReason,
			inst.Description)
		changed := SetWaveComponentCondition(&(deepCopy.Status), *condition)
		if changed {
			err := r.Client.Patch(ctx, deepCopy, client.MergeFrom(comp))
			if err != nil {
				log.Error(err, "patch error")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, err
	case install.Progressing:
		// progressing, requeue
		deepCopy := comp.DeepCopy()
		condition := components.NewWaveComponentCondition(
			v1alpha1.WaveComponentProgressing,
			v1.ConditionTrue,
			InProgressReason,
			inst.Description)
		changed := SetWaveComponentCondition(&(deepCopy.Status), *condition)
		if changed {
			err := r.Client.Patch(ctx, deepCopy, client.MergeFrom(comp))
			if err != nil {
				log.Error(err, "patch error")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{
			RequeueAfter: 15 * time.Second,
		}, err
	case install.Uninstalled:
		// well, reinstall it.
		deepCopy := comp.DeepCopy()
		condition := components.NewWaveComponentCondition(
			v1alpha1.WaveComponentAvailable,
			v1.ConditionFalse,
			UninstalledReason,
			inst.Description)
		changed := SetWaveComponentCondition(&(deepCopy.Status), *condition)
		if changed {
			err := r.Client.Patch(ctx, deepCopy, client.MergeFrom(comp))
			if err != nil {
				log.Error(err, "patch error")
				return ctrl.Result{}, err
			}
			// update comp
			err = r.Client.Get(
				ctx,
				types.NamespacedName{
					Name:      comp.Name,
					Namespace: comp.Namespace,
				},
				comp,
			)
			if err != nil {
				log.Error(err, "retrieve error")
				return ctrl.Result{}, err
			}

		}
		return r.install(ctx, log, comp)

		// remaining conditions are Deployed and Unknown, continue on to component-specific condition
	}

	// check updated conditions and properties
	// note that underlying components may fail without triggering a reconciliation event. TODO figure out how to fix
	deepCopy := comp.DeepCopy()
	changed := false

	conditions, err := r.GetCurrentConditions(comp)
	if err != nil {
		log.Error(err, "cannot get current conditions")
		return ctrl.Result{}, err
	}
	if conditions != nil {
		for _, c := range conditions {
			up := SetWaveComponentCondition(&(deepCopy.Status), *c)
			changed = changed || up
		}
	}

	properties, err := r.GetCurrentProperties(deepCopy)
	if err != nil {
		log.Error(err, "cannot get current properties")
		return ctrl.Result{}, err
	}
	up := r.updateStatusProperties(deepCopy, properties)
	changed = changed || up

	if changed {
		err := r.Client.Patch(ctx, deepCopy, client.MergeFrom(comp))
		if err != nil {
			log.Error(err, "patch error")
			return ctrl.Result{}, err
		}
	}

	condition := GetCurrentComponentCondition(deepCopy.Status)
	requeue := true
	if condition.Type == v1alpha1.WaveComponentAvailable && condition.Status == v1.ConditionTrue {
		requeue = false
	}
	return ctrl.Result{
		Requeue: requeue,
	}, nil

}

func (r *WaveComponentReconciler) install(ctx context.Context, log logr.Logger, comp *v1alpha1.WaveComponent) (ctrl.Result, error) {
	log.Info("install is required")
	i := r.getInstaller(Wave, r.getClient, log)
	deepCopy := comp.DeepCopy()
	condition := components.NewWaveComponentCondition(
		v1alpha1.WaveComponentProgressing,
		v1.ConditionTrue,
		InstallingReason,
		"Helm installation started",
	)
	changed := SetWaveComponentCondition(&(deepCopy.Status), *condition)
	if changed {
		err := r.Client.Patch(ctx, deepCopy, client.MergeFrom(comp))
		if err != nil {
			log.Error(err, "patch error")
			return ctrl.Result{}, err
		}
	}

	if err := r.EnsureNamespace(catalog.SystemNamespace); err != nil {
		r.Log.Error(err, "unable to create namespace", "namespace", catalog.SystemNamespace)
		return ctrl.Result{}, err
	}
	if err := r.EnsureNamespace(catalog.SparkJobsNamespace); err != nil {
		r.Log.Error(err, "unable to create namespace", "namespace", catalog.SparkJobsNamespace)
		return ctrl.Result{}, err
	}

	helmError := i.Install(string(comp.Spec.Name), comp.Spec.URL, comp.Spec.Version, comp.Spec.ValuesConfiguration)
	if helmError != nil {
		return ctrl.Result{}, helmError
	}
	return ctrl.Result{
		RequeueAfter: 60 * time.Second,
	}, nil

}

func (r *WaveComponentReconciler) delete(ctx context.Context, log logr.Logger, comp *v1alpha1.WaveComponent) (ctrl.Result, error) {
	log.Info("delete is required")
	i := r.getInstaller(Wave, r.getClient, log)
	deepCopy := comp.DeepCopy()
	condition := components.NewWaveComponentCondition(
		v1alpha1.WaveComponentProgressing,
		v1.ConditionTrue,
		DeletingReason,
		"Helm deletion started",
	)
	changed := SetWaveComponentCondition(&(deepCopy.Status), *condition)
	if changed {
		err := r.Client.Patch(ctx, deepCopy, client.MergeFrom(comp))
		if err != nil {
			log.Error(err, "patch error")
			return ctrl.Result{}, err
		}
	}

	helmError := i.Delete(string(comp.Spec.Name), comp.Spec.URL, comp.Spec.Version, comp.Spec.ValuesConfiguration)
	if helmError != nil {
		return ctrl.Result{}, helmError
	}
	return ctrl.Result{
		RequeueAfter: 60 * time.Second,
	}, nil

}

func (r *WaveComponentReconciler) upgrade(ctx context.Context, log logr.Logger, comp *v1alpha1.WaveComponent) (ctrl.Result, error) {

	i := r.getInstaller(Wave, r.getClient, log)

	log.Info("upgrade is required")
	deepCopy := comp.DeepCopy()
	condition := components.NewWaveComponentCondition(
		v1alpha1.WaveComponentProgressing,
		v1.ConditionTrue,
		UpgradingReason,
		"Helm upgrade started",
	)
	changed := SetWaveComponentCondition(&(deepCopy.Status), *condition)
	if changed {
		err := r.Client.Patch(ctx, deepCopy, client.MergeFrom(comp))
		if err != nil {
			log.Error(err, "patch error")
			return ctrl.Result{}, err
		}
	}
	helmError := i.Upgrade(string(comp.Spec.Name), comp.Spec.URL, comp.Spec.Version, comp.Spec.ValuesConfiguration)
	if helmError != nil {
		return ctrl.Result{}, helmError
	}
	return ctrl.Result{
		RequeueAfter: 60 * time.Second,
	}, nil
}

func (r *WaveComponentReconciler) reconcileAbsent(ctx context.Context, req ctrl.Request, comp *v1alpha1.WaveComponent) (ctrl.Result, error) {
	log := r.Log.WithValues("wavecomponent", req.NamespacedName)

	i := r.getInstaller(Wave, r.getClient, log)

	_, err := i.Get(i.GetReleaseName(req.Name))
	if err != nil {
		if err == install.ErrReleaseNotFound {
			deepCopy := comp.DeepCopy()
			condition := components.NewWaveComponentCondition(
				v1alpha1.WaveComponentAvailable,
				v1.ConditionFalse,
				UninstalledReason,
				"component not present",
			)
			changed := SetWaveComponentCondition(&(deepCopy.Status), *condition)
			if changed {
				err := r.Client.Patch(ctx, deepCopy, client.MergeFrom(comp))
				if err != nil {
					log.Error(err, "patch error")
					return ctrl.Result{}, err
				}
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	return r.delete(ctx, log, comp)

}

func (r *WaveComponentReconciler) EnsureNamespace(namespace string) error {
	ns := &v1.Namespace{}
	ctx := context.TODO()
	err := r.Client.Get(ctx, client.ObjectKey{Namespace: "", Name: namespace}, ns)
	r.Log.Info("checking existence", "namespace", namespace)
	if k8serrors.IsNotFound(err) {
		ns.Name = namespace
		r.Log.Info("creating", "namespace", namespace)
		return r.Client.Create(ctx, ns)
	}
	return err
}

func (r *WaveComponentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.WaveComponent{}).
		Complete(r)
}

// GetCurrentCondition examines the current environment and returns a condition for the component
func (r *WaveComponentReconciler) GetCurrentConditions(comp *v1alpha1.WaveComponent) ([]*v1alpha1.WaveComponentCondition, error) {

	var conditionOK = components.NewWaveComponentCondition(
		v1alpha1.WaveComponentAvailable,
		v1.ConditionTrue,
		AvailableReason,
		"component running",
	)
	var conditionError = components.NewWaveComponentCondition(
		v1alpha1.WaveComponentAvailable,
		v1.ConditionFalse,
		"",
		"",
	)

	switch comp.Spec.Name {
	case v1alpha1.SparkHistoryChartName:
		return components.GetSparkHistoryConditions(r.Client, r.Log)
	case v1alpha1.EnterpriseGatewayChartName:
		restconfig, _ := r.getClient.ToRESTConfig()
		return components.GetEnterpriseGatewayConditions(restconfig, r.Client, r.Log)
	case v1alpha1.SparkOperatorChartName:
		config, err := r.getClient.ToRESTConfig()
		if err != nil {
			conditionError.Message = err.Error()
			return []*v1alpha1.WaveComponentCondition{conditionError}, err
		}
		return components.GetSparkOperatorConditions(config, r.Client, r.Log)
	case v1alpha1.WaveIngressChartName:
		return []*v1alpha1.WaveComponentCondition{conditionOK}, nil
	default:
		// (a) check helm
		// (b) return not installed
		return []*v1alpha1.WaveComponentCondition{
			components.NewWaveComponentCondition(
				v1alpha1.WaveComponentAvailable,
				v1.ConditionFalse,
				UninstalledReason,
				"component not installed",
			),
		}, nil
	}
}

func (r *WaveComponentReconciler) GetCurrentProperties(comp *v1alpha1.WaveComponent) (map[string]string, error) {
	switch comp.Spec.Name {
	case v1alpha1.SparkHistoryChartName:
		return components.GetSparkHistoryProperties(comp, r.Client, r.Log)
	case v1alpha1.EnterpriseGatewayChartName:
		return components.GetEnterpriseGatewayProperties(comp, r.Client, r.Log)
	case v1alpha1.SparkOperatorChartName:
		return components.GetSparkOperatorProperties(comp, r.Client, r.Log)
	case v1alpha1.WaveIngressChartName:
		return map[string]string{}, nil
	default:
		return map[string]string{}, nil
	}
}

func (r *WaveComponentReconciler) updateStatusProperties(c *v1alpha1.WaveComponent, props map[string]string) bool {
	updated := false
	if c.Status.Properties == nil {
		c.Status.Properties = map[string]string{}
	}

	for key, value := range props {
		if c.Status.Properties[key] != value {
			c.Status.Properties[key] = value
			updated = true
		}
	}

	return updated
}
