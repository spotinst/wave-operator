package components

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/catalog"
	"github.com/spotinst/wave-operator/cloudstorage"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	HistoryServerChartName   = "spark-history-server"
	HistoryServerReleaseName = "wave-spark-history-server"
)

func ConfigureHistoryServer(comp *v1alpha1.WaveComponent, storageProvider cloudstorage.CloudStorageProvider) (bool, error) {
	info, err := storageProvider.ConfigureHistoryServerStorage()
	if err != nil {
		return false, err
	}
	newVals, err := configureS3BucketValues(info, comp.Spec.ValuesConfiguration)
	if err != nil {
		return false, err
	}
	comp.Spec.ValuesConfiguration = string(newVals)
	return true, nil
}

func configureS3BucketValues(b *cloudstorage.StorageInfo, valuesConfiguration string) ([]byte, error) {
	var newVals map[string]interface{}
	err := yaml.Unmarshal([]byte(valuesConfiguration), &newVals)
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

func GetSparkHistoryConditions(client client.Client, log logr.Logger) ([]*v1alpha1.WaveComponentCondition, error) {

	conditions := []*v1alpha1.WaveComponentCondition{}
	ctx := context.TODO()

	deployment := &appsv1.Deployment{}
	err := client.Get(ctx, types.NamespacedName{Namespace: catalog.SystemNamespace, Name: HistoryServerReleaseName}, deployment)
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

func GetSparkHistoryProperties(c *v1alpha1.WaveComponent, client client.Client, log logr.Logger) (map[string]string, error) {
	ctx := context.TODO()
	props := map[string]string{}
	if c.Spec.Version == "1.4.0" {
		props["SparkVersion"] = "2.4.0"
	}

	config := &v1.ConfigMap{}
	key := types.NamespacedName{
		Namespace: catalog.SystemNamespace,
		Name:      HistoryServerReleaseName,
	}
	err := client.Get(ctx, key, config)
	if err != nil {
		log.Info("failed to read configmap", "name", HistoryServerReleaseName, "error", err.Error())
	} else {
		props["LogDirectory"] = config.Data["logDirectory"]
	}
	return props, nil
}
