package components

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/catalog"
	"github.com/spotinst/wave-operator/cloudstorage"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/api/extensions/v1beta1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	SparkHistoryIngressName = "spark-history-server"
)

func ConfigureHistoryServer(comp *v1alpha1.WaveComponent, storageProvider cloudstorage.CloudStorageProvider) (bool, error) {
	info, err := storageProvider.ConfigureHistoryServerStorage()
	if err != nil {
		return false, err
	}
	newVals, err := configureS3BucketValues(info, []byte(comp.Spec.ValuesConfiguration))
	if err != nil {
		return false, err
	}
	newVals, err = configureIngressLogin(info, newVals)
	if err != nil {
		return false, err
	}
	comp.Spec.ValuesConfiguration = string(newVals)
	return true, nil
}

func configureIngressLogin(b *cloudstorage.StorageInfo, valuesConfiguration []byte) ([]byte, error) {
	var hi historyIngress
	err := yaml.Unmarshal(valuesConfiguration, &hi)
	if err != nil {
		return nil, fmt.Errorf("invalid yaml specification for ingress, %w", err)
	}
	if !hi.Ingress.Enabled || !hi.Ingress.BasicAuth.Enabled {
		return valuesConfiguration, nil
	}
	// extract & set values
	var vals map[string]interface{}
	err = yaml.Unmarshal(valuesConfiguration, &vals)
	if err != nil {
		return nil, err
	}
	if vals == nil {
		vals = map[string]interface{}{}
	}
	if vals["ingress"] == nil {
		return valuesConfiguration, nil
	}
	ingressOpts := vals["ingress"].(map[string]interface{})
	ba := ingressOpts["basicAuth"].(map[string]interface{})
	if ba["username"] == nil || ba["username"] == "" {
		return nil, fmt.Errorf("invalid user specification for ingress login, username is empty")
	}
	if ba["password"] == nil || ba["password"] == "" {
		ba["password"] = rand.String(8)
	}
	ingressOpts["basicAuth"] = ba
	vals["ingress"] = ingressOpts

	return yaml.Marshal(vals)
}

func configureS3BucketValues(b *cloudstorage.StorageInfo, valuesConfiguration []byte) ([]byte, error) {
	var newVals map[string]interface{}
	err := yaml.Unmarshal(valuesConfiguration, &newVals)
	if err != nil {
		return nil, err
	}
	if newVals == nil {
		newVals = map[string]interface{}{}
	}
	newVals["s3"] = map[string]interface{}{
		"enableS3":     true,
		"enableIAM":    true,
		"logDirectory": b.Path,
	}
	return yaml.Marshal(newVals)
}

func GetSparkHistoryConditions(client client.Client, releaseName string) ([]*v1alpha1.WaveComponentCondition, error) {

	conditions := make([]*v1alpha1.WaveComponentCondition, 0)
	ctx := context.TODO()

	deployment := &appsv1.Deployment{}
	err := client.Get(ctx, types.NamespacedName{Namespace: catalog.SystemNamespace, Name: releaseName}, deployment)
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

func GetSparkHistoryProperties(c *v1alpha1.WaveComponent, client client.Client, log logr.Logger, releaseName string) (map[string]string, error) {
	ctx := context.TODO()
	props := map[string]string{}

	// TODO this is wrong in many ways
	if c.Spec.Version == "1.4.0" {
		props["SparkVersion"] = "2.4.0"
	} else {
		props["SparkVersion"] = "3.0.1"
	}

	config := &v1.ConfigMap{}
	key := types.NamespacedName{
		Namespace: catalog.SystemNamespace,
		Name:      releaseName,
	}
	err := client.Get(ctx, key, config)
	if err != nil {
		log.Info("failed to read configmap", "name", releaseName, "error", err.Error())
	} else {
		props["LogDirectory"] = config.Data["logDirectory"]
	}

	var hi historyIngress
	err = yaml.Unmarshal([]byte(c.Spec.ValuesConfiguration), &hi)
	if err != nil {
		return nil, err
	}
	if !hi.Ingress.Enabled {
		return props, nil
	}

	ep, err := getSparkHistoryEndpoint(ctx, client, log)
	if err != nil {
		return nil, fmt.Errorf("failed to get history server endpoint, %w", err)
	}
	props["Endpoint"] = ep
	if !hi.Ingress.BasicAuth.Enabled {
		return props, nil
	}

	user, pass, err := getUserPasswordFrom(c)
	if err != nil {
		return nil, fmt.Errorf("failed to get user and password, %w", err)
	}
	props["User"] = user
	props["Password"] = pass

	return props, nil
}

type historyIngress struct {
	Ingress ingress `yaml:"ingress"`
}
type ingress struct {
	Enabled   bool      `yaml:"enabled"`
	BasicAuth basicAuth `yaml:"basicAuth"`
}
type basicAuth struct {
	Enabled  bool   `yaml:"enabled"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

func getUserPasswordFrom(c *v1alpha1.WaveComponent) (string, string, error) {
	var hi historyIngress
	err := yaml.Unmarshal([]byte(c.Spec.ValuesConfiguration), &hi)
	if err != nil {
		return "", "", fmt.Errorf("invalid yaml specification for ingress, %w", err)
	}
	return hi.Ingress.BasicAuth.Username, hi.Ingress.BasicAuth.Password, nil
}

func getSparkHistoryEndpoint(ctx context.Context, c client.Client, log logr.Logger) (string, error) {
	ingress := &v1beta1.Ingress{}
	err := c.Get(ctx, types.NamespacedName{Namespace: catalog.SystemNamespace, Name: SparkHistoryIngressName}, ingress)
	if err != nil {
		return "", err
	} else {
		if ingress.Status.LoadBalancer.Ingress != nil && len(ingress.Status.LoadBalancer.Ingress) > 0 {
			return ingress.Status.LoadBalancer.Ingress[0].Hostname + "/", nil
		}
	}
	return "", fmt.Errorf("ingress hostname is not set")
}
