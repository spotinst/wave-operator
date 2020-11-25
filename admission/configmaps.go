package admission

import (
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	"github.com/magiconair/properties"
	"github.com/spotinst/wave-operator/cloudstorage"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
)

func MutateConfigMap(provider cloudstorage.CloudStorageProvider, log logr.Logger, req *admissionv1.AdmissionRequest) (*admissionv1.AdmissionResponse, error) {

	gvk := corev1.SchemeGroupVersion.WithKind("ConfigMap")
	sourceObj := &corev1.ConfigMap{}
	_, _, err := deserializer.Decode(req.Object.Raw, &gvk, sourceObj)
	if err != nil {
		return nil, err
	}
	if sourceObj == nil {
		return nil, fmt.Errorf("deserialization failed")
	}
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
	log.Info("constructing patch", "configmap", sourceObj.Name, "owner", sourceObj.OwnerReferences[0].Name)
	propertyString := modObj.Data["spark.properties"]
	props, err := properties.LoadString(propertyString)
	if err != nil {
		log.Error(err, "unparseable spark property data in configmap", "configmap", sourceObj.Name)
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
		log.Error(err, "unable to generate patch, continuing", "configmap", sourceObj.Name)
		return resp, nil
	}
	log.Info("patching configmap", "patch", string(patch))

	resp.Patch = patch
	resp.PatchType = &jsonPatchType
	return resp, nil
}
