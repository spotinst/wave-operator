package admission

import (
	"context"
	"fmt"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"strings"

	"github.com/go-logr/logr"
	"github.com/magiconair/properties"
	"github.com/spotinst/wave-operator/cloudstorage"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
)

func MutateConfigMap(client kubernetes.Interface, provider cloudstorage.CloudStorageProvider, log logr.Logger, req *admissionv1.AdmissionRequest) (*admissionv1.AdmissionResponse, error) {

	ctx := context.TODO()

	gvk := corev1.SchemeGroupVersion.WithKind("ConfigMap")
	sourceObj := &corev1.ConfigMap{}
	_, _, err := deserializer.Decode(req.Object.Raw, &gvk, sourceObj)
	if err != nil {
		return nil, err
	}
	if sourceObj == nil {
		return nil, fmt.Errorf("deserialization failed")
	}
	log = log.WithValues("configmap", sourceObj.Name)

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

	ownerPod, err := client.CoreV1().Pods(sourceObj.Namespace).Get(ctx, sourceObj.OwnerReferences[0].Name, v1.GetOptions{})
	if err != nil {
		log.Error(err, "cannot get owner pod")
		return resp, nil
	}

	log.Info("Got owner pod", "name", ownerPod.Name, "annotations", ownerPod.Annotations)

	if !isEventLogSyncEnabled(ownerPod.Annotations) {
		log.Info("Event log sync not enabled, will not modify cm")
		return resp, nil
	}

	storageInfo, err := provider.GetStorageInfo()
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
	// TODO Don't override customer configuration?
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
