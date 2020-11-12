package components

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/catalog"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/api/extensions/v1beta1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"

	// "k8s.io/kubernetes/pkg/apis/networking"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	EnterpriseGatewayChartName   = "enterprise-gateway"
	EnterpriseGatewayIngressName = "enterprise-gateway-ingress"
	EnterpriseGatewayReleaseName = "wave-enterprise-gateway"
)

func GetEnterpriseGatewayConditions(config *rest.Config, client client.Client, log logr.Logger) ([]*v1alpha1.WaveComponentCondition, error) {

	conditions := []*v1alpha1.WaveComponentCondition{}
	ctx := context.TODO()
	var err error

	deployment := &appsv1.Deployment{}
	err = client.Get(ctx, types.NamespacedName{Namespace: catalog.SystemNamespace, Name: EnterpriseGatewayReleaseName}, deployment)
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

func GetEnterpriseGatewayProperties(c *v1alpha1.WaveComponent, client client.Client, log logr.Logger) (map[string]string, error) {
	props := map[string]string{}

	ctx := context.TODO()
	ingress := &v1beta1.Ingress{}
	err := client.Get(ctx, types.NamespacedName{Namespace: catalog.SystemNamespace, Name: EnterpriseGatewayIngressName}, ingress)
	if err != nil {
		log.Error(err, "failed to read ingress", "name", EnterpriseGatewayIngressName)
	} else {
		if ingress.Status.LoadBalancer.Ingress != nil && len(ingress.Status.LoadBalancer.Ingress) > 0 {
			props["Endpoint"] = ingress.Status.LoadBalancer.Ingress[0].Hostname + "/gateway"
		}
	}
	return props, nil
}
