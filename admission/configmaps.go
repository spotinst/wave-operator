package admission

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	"github.com/magiconair/properties"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/spotinst/wave-operator/cloudstorage"
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

	// check to make sure it's a spark configmap
	// "[namespace]-[spark-id]-driver-conf-map."
	if !strings.HasSuffix(sourceObj.Name, "-driver-conf-map") {
		return resp, nil
	}
	// XX don't check this. for spark-operator, no namespace prefix
	// if !strings.HasPrefix(sourceObj.Name, sourceObj.Namespace) {
	// 	return resp, nil
	// }
	if len(sourceObj.ObjectMeta.OwnerReferences) == 0 {
		return resp, nil
	}
	if sourceObj.Data["spark.properties"] == "" {
		return resp, nil
	}

	ownerPod, err := m.client.CoreV1().Pods(sourceObj.Namespace).Get(ctx, sourceObj.OwnerReferences[0].Name, v1.GetOptions{})
	if err != nil {
		log.Error(err, "could not get owner pod")
		return resp, nil
	}

	log.Info("Got config map owner pod",
		"name", ownerPod.Name, "namespace", ownerPod.Namespace, "annotations", ownerPod.Annotations)

	if !isEventLogSyncEnabled(ownerPod.Annotations) {
		log.Info("Event log sync not enabled, will not mutate config map")
		return resp, nil
	}

	storageInfo, err := m.provider.GetStorageInfo()
	if err != nil {
		log.Error(err, "cannot get storage configuration")
		return resp, nil
	}

	if storageInfo == nil {
		log.Error(err, "storage information from provider is nil")
		return resp, nil
	}

	modObj := sourceObj.DeepCopy()
	log.Info("constructing patch", "owner", sourceObj.OwnerReferences[0].Name)
	propertyString := modObj.Data["spark.properties"]
	props, err := properties.LoadString(propertyString)
	if err != nil {
		log.Error(err, "unparseable spark property data in configmap")
		return resp, nil
	}
	if props == nil {
		props = properties.NewProperties()
	}
	props.Set("spark.eventLog.dir", "file:///var/log/spark") //storageInfo.Path)
	props.Set("spark.eventLog.enabled", "true")

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
