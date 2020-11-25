package admission

import (
	"fmt"

	"github.com/go-logr/logr"
	"github.com/spotinst/wave-operator/cloudstorage"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func MutatePod(provider cloudstorage.CloudStorageProvider, log logr.Logger, req *admissionv1.AdmissionRequest) (*admissionv1.AdmissionResponse, error) {

	gvk := corev1.SchemeGroupVersion.WithKind("Pod")
	sourceObj := &corev1.Pod{}
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
	storageInfo, err := provider.GetStorageInfo()
	if err != nil {
		message := "cannot get storage configuration"
		log.Error(err, message)
		resp.Allowed = false
		resp.Result = &metav1.Status{
			Status:  "Failure",
			Message: message,
			Reason:  metav1.StatusReasonInvalid,
			Details: &metav1.StatusDetails{
				Name:  req.Name,
				Group: gvk.Group,
				Kind:  gvk.Kind,
				UID:   req.UID,
				Causes: []metav1.StatusCause{metav1.StatusCause{
					Type:    metav1.CauseTypeUnexpectedServerResponse,
					Message: message,
				}},
				RetryAfterSeconds: 0,
			},
		}
		resp.Warnings = []string{fmt.Sprintf("%s, %s", message, err.Error())}
		return resp, nil
	}
	if storageInfo == nil {
		message := "storage configuration is nil"
		resp.Allowed = false
		resp.Result = &metav1.Status{
			Status:  "Failure",
			Message: message,
			Reason:  metav1.StatusReasonInvalid,
		}
		resp.Warnings = []string{message}
		return resp, nil
	}

	// it would be more efficient to construct a series of patches, rather than replacing the entire spec...
	modObj := sourceObj.DeepCopy()
	newSpec := modObj.Spec

	newSpec.Affinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{
				{
					Weight: 1,
					Preference: corev1.NodeSelectorTerm{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      "spotinst.io/node-lifecycle",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{"od"},
							},
						},
					},
				},
			},
		},
	}

	volumeMount := corev1.VolumeMount{
		Name:      "spark-logs",
		MountPath: "/var/log/spark",
	}

	log.Info("pod admission control", "mountPath", volumeMount.MountPath)

	newSpec.Containers = append(newSpec.Containers, corev1.Container{
		Name:            "storage-sync",
		Image:           "ntfrnzn/cloud-storage-sync",
		ImagePullPolicy: corev1.PullAlways,
		Command:         []string{"/tini"},
		Args:            []string{"--", "python3", "sync.py", volumeMount.MountPath, "spark:" + storageInfo.Name},
		Env:             []corev1.EnvVar{{Name: "S3_REGION", Value: storageInfo.Region}},
	})

	for i := range newSpec.Containers {
		if newSpec.Containers[i].VolumeMounts == nil {
			newSpec.Containers[i].VolumeMounts = []corev1.VolumeMount{}
		}
		newSpec.Containers[i].VolumeMounts = append(newSpec.Containers[i].VolumeMounts, volumeMount)
	}
	newSpec.Volumes = append(newSpec.Volumes, corev1.Volume{
		Name: "spark-logs",
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	})

	// specBytes, err := json.Marshal(newSpec)
	// if err != nil {
	// 	log.Error(err, "cannot serialize spec")
	// 	resp.Allowed = false
	// 	resp.Warnings = []string{fmt.Sprintf("cannot serialize spec, %s", err.Error())}
	// 	return resp, nil
	// }

	// patch := patchOperation{
	// 	Op:    "replace",
	// 	Path:  "/spec",
	// 	Value: string(specBytes),
	// }
	modObj.Spec = newSpec
	patchBytes, err := GetJsonPatch(sourceObj, modObj)
	if err != nil {
		log.Error(err, "cannot generate patch")
		resp.Allowed = false
		resp.Warnings = []string{fmt.Sprintf("cannot generate patch, %s", err.Error())}
		return resp, nil
	}
	patchType := admissionv1.PatchTypeJSONPatch

	log.Info("patching pod", "patch", string(patchBytes))
	resp.PatchType = &patchType
	resp.Patch = patchBytes

	return resp, nil
}
