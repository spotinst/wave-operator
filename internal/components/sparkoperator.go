package components

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/catalog"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func GetSparkOperatorConditions(config *rest.Config, client client.Client, releaseName string) ([]*v1alpha1.WaveComponentCondition, error) {

	conditions := []*v1alpha1.WaveComponentCondition{}
	ctx := context.TODO()
	var err error

	k, err := clientset.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	_, err = k.ApiextensionsV1().CustomResourceDefinitions().Get(
		context.TODO(),
		"sparkapplications.sparkoperator.k8s.io",
		metav1.GetOptions{},
	)
	if err != nil {
		conditions = append(conditions,
			NewWaveComponentCondition(
				v1alpha1.WaveComponentAvailable,
				v1.ConditionFalse,
				"CRDNotDefined",
				fmt.Sprintf("CRD sparkapplications not retrieved, %s", err.Error()),
			),
		)
		return conditions, nil
	}

	deployment := &appsv1.Deployment{}
	err = client.Get(ctx, types.NamespacedName{Namespace: catalog.SystemNamespace, Name: releaseName}, deployment)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			conditions = append(conditions,
				NewWaveComponentCondition(v1alpha1.WaveComponentAvailable, v1.ConditionFalse, "DeploymentAbsent", "deployment does not exist"))
			return conditions, nil // enough, return
		} else {
			return nil, err
		}
	}

	if deployment.Status.AvailableReplicas == 0 {
		conditions = append(conditions,
			NewWaveComponentCondition(v1alpha1.WaveComponentAvailable, v1.ConditionFalse, "PodsUnavailable", "no pods are available"))
	} else {
		conditions = append(conditions,
			NewWaveComponentCondition(v1alpha1.WaveComponentAvailable, v1.ConditionTrue, "DeploymentAvailable", "pods are available"))

	}
	return conditions, nil
}

func GetSparkOperatorProperties(c *v1alpha1.WaveComponent, client client.Client, log logr.Logger) (map[string]string, error) {
	props := map[string]string{}
	if c.Spec.Version == "0.8.4" { // app version "v1beta2-1.2.0-3.0.0" {
		props["SparkVersion"] = "3.0.0"
	}
	return props, nil
}
