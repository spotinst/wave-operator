package admission

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/magiconair/properties"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/spotinst/wave-operator/cloudstorage"
	"github.com/spotinst/wave-operator/internal/config"
)

type ConfigMapMutator struct {
	log      logr.Logger
	client   kubernetes.Interface
	provider cloudstorage.CloudStorageProvider
}

func NewConfigMapMutator(log logr.Logger, client kubernetes.Interface, provider cloudstorage.CloudStorageProvider) ConfigMapMutator {
	return ConfigMapMutator{
		log:      log,
		client:   client,
		provider: provider,
	}
}

func (m ConfigMapMutator) Mutate(req *admissionv1.AdmissionRequest) (*admissionv1.AdmissionResponse, error) {

	ctx := context.TODO()

	gvk := corev1.SchemeGroupVersion.WithKind("ConfigMap")
	sourceObj := &corev1.ConfigMap{}

	_, _, err := deserializer.Decode(req.Object.Raw, &gvk, sourceObj)
	if err != nil {
		return nil, fmt.Errorf("deserialization failed, %w", err)
	}

	log := m.log.WithValues("configmap", sourceObj.Name)

	resp := &admissionv1.AdmissionResponse{
		UID:     req.UID,
		Allowed: true,
	}

	if len(sourceObj.ObjectMeta.OwnerReferences) == 0 {
		return resp, nil
	}

	if sourceObj.Data["spark.properties"] == "" {
		return resp, nil
	}

	ownerPod, err := m.client.CoreV1().Pods(sourceObj.Namespace).Get(ctx, sourceObj.OwnerReferences[0].Name, metav1.GetOptions{})
	if err != nil {
		log.Error(err, "could not get owner pod")
		return resp, nil
	}

	log.Info("Got config map owner pod",
		"name", ownerPod.Name, "namespace", ownerPod.Namespace, "labels", ownerPod.Labels, "annotations", ownerPod.Annotations)

	if ownerPod.Labels[SparkRoleLabel] != SparkRoleDriverValue {
		log.Info("Not a driver config map, will not mutate config map")
		return resp, nil
	}

	propOverride := map[string]string{
		"spark.metrics.appStatusSource.enabled": "true",
	}

	if config.IsEventLogSyncEnabled(ownerPod.Annotations) {
		log.Info("Event log sync enabled, configuring storage")
		storageInfo, err := m.provider.GetStorageInfo()
		if err != nil {
			log.Error(err, "cannot get storage configuration")
			return resp, nil
		}

		if storageInfo == nil {
			log.Error(err, "storage information from provider is nil")
			return resp, nil
		}

		propOverride["spark.eventLog.dir"] = "file:///var/log/spark"
		propOverride["spark.eventLog.enabled"] = "true"
	}

	modObj := sourceObj.DeepCopy()
	log.Info("constructing patch", "owner", sourceObj.OwnerReferences[0].Name)
	propertyString := modObj.Data["spark.properties"]

	props, err := properties.LoadString(propertyString)
	if err != nil {
		log.Error(err, "un-parsable spark property data in configmap")
		return resp, nil
	}

	if props == nil {
		props = properties.NewProperties()
	}

	props.Merge(properties.LoadMap(propOverride))

	modObj.Data["spark.properties"] = props.String()

	patch, err := GetJsonPatch(sourceObj, modObj)
	if err != nil {
		log.Error(err, "unable to generate patch, continuing")
		return resp, nil
	}

	log.Info("patching configmap", "patch", string(patch))

	resp.Patch = patch
	resp.PatchType = &jsonPatchType
	return resp, nil
}
