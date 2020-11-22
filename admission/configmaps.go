package admission

import (
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	"github.com/magiconair/properties"
	"github.com/spotinst/wave-operator/cloudstorage"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

var (
	runtimeScheme = runtime.NewScheme()
	codecs        = serializer.NewCodecFactory(runtimeScheme)
	deserializer  = codecs.UniversalDeserializer()
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
		resp.Allowed = false
		resp.Warnings = []string{fmt.Sprintf("cannot get storage configuration, %s", err.Error())}
		return resp, nil
	}
	if storageInfo == nil {
		log.Error(err, "storage information from provider is nil")
		resp.Allowed = false
		resp.Warnings = []string{"storage unavailable from provider"}
		return resp, nil
	}

	log.Info("constructing patch", "configmap", sourceObj.Name, "owner", sourceObj.OwnerReferences[0].Name)
	propertyString := sourceObj.Data["spark.properties"]
	props, err := properties.LoadString(propertyString)
	if err != nil {
		return nil, fmt.Errorf("unparseable spark property data, %w", err)
	}
	if props == nil {
		props = properties.NewProperties()
	}
	props.Set("spark.eventLog.dir", storageInfo.Path)

	patchType := admissionv1.PatchTypeJSONPatch
	var patchBuilder strings.Builder
	patchBuilder.WriteString("[{ \"op\": \"replace\", \"path\": \"/data\", \"value\": ")
	patchBuilder.WriteString("{\"spark.properties\": \"")
	for k, v := range props.Map() {
		patchBuilder.Write([]byte(k))
		patchBuilder.WriteString("=")
		patchBuilder.Write([]byte(v))
		patchBuilder.WriteString("\\n")
		fmt.Println(v)
	}
	patchBuilder.WriteString("\"}}]")
	patchString := patchBuilder.String()

	resp.Patch = []byte(patchString)
	resp.PatchType = &patchType

	return resp, nil
	// return nil, fmt.Errorf("no configmaps mutated today")
}
