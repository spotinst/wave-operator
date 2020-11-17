package components

import (
	"context"
	"fmt"

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

	// TODO this method should probably return only a single condition, success or failure

	conditions := []*v1alpha1.WaveComponentCondition{}
	ctx := context.TODO()
	var err error

	deployment := &appsv1.Deployment{}
	// in this chart, the deployment does not get the full releasename, just the chartname ¯\_(ツ)_/¯
	err = client.Get(ctx, types.NamespacedName{Namespace: catalog.SystemNamespace, Name: EnterpriseGatewayChartName}, deployment)
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
		return conditions, nil
	}
	_, err = getEnterpriseGatewayEndpoint(ctx, client, log)
	if err != nil {
		conditions = append(conditions,
			NewWaveComponentCondition(v1alpha1.WaveComponentDegraded, v1.ConditionTrue,
				"EndpointUnavailable",
				fmt.Sprintf("Enterprise gateway endpoint not available, %s", err.Error())))
		return conditions, nil
	}

	conditions = append(conditions,
		NewWaveComponentCondition(v1alpha1.WaveComponentAvailable, v1.ConditionTrue, "DeploymentAvailable", "resources are available"))
	return conditions, nil
}

func getEnterpriseGatewayEndpoint(ctx context.Context, client client.Client, log logr.Logger) (string, error) {
	ingress := &v1beta1.Ingress{}
	err := client.Get(ctx, types.NamespacedName{Namespace: catalog.SystemNamespace, Name: EnterpriseGatewayIngressName}, ingress)
	if err != nil {
		return "", err
	} else {
		if ingress.Status.LoadBalancer.Ingress != nil && len(ingress.Status.LoadBalancer.Ingress) > 0 {
			return ingress.Status.LoadBalancer.Ingress[0].Hostname + "/gateway", nil
		}
	}
	return "", fmt.Errorf("ingress hostname is not set")
}

func GetEnterpriseGatewayProperties(c *v1alpha1.WaveComponent, client client.Client, log logr.Logger) (map[string]string, error) {
	ctx := context.TODO()
	props := map[string]string{}
	endpoint, err := getEnterpriseGatewayEndpoint(ctx, client, log)
	if err == nil {
		props["Endpoint"] = endpoint
	}
	return props, nil
}
