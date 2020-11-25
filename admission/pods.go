package admission

import (
	"fmt"

	"github.com/go-logr/logr"
	"github.com/spotinst/wave-operator/cloudstorage"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
)

var (
	ondemandAffinity = &corev1.Affinity{
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
	log = log.WithValues("pod", sourceObj.Name)

	resp := &admissionv1.AdmissionResponse{
		UID:     req.UID,
		Allowed: true,
	}
	storageInfo, err := provider.GetStorageInfo()
	if err != nil {
		log.Error(err, "cannot get storage configuration, not patching pod")
		return resp, nil
	}
	if storageInfo == nil {
		log.Error(fmt.Errorf("storage configuration is nil"), "not patching pod")
		return resp, nil
	}

	modObj := sourceObj.DeepCopy()
	newSpec := modObj.Spec

	newSpec.Affinity = ondemandAffinity

	volumeMount := corev1.VolumeMount{
		Name:      "spark-logs",
		MountPath: "/var/log/spark",
	}

	log.Info("pod admission control", "mountPath", volumeMount.MountPath)

	newSpec.Containers = append(newSpec.Containers, corev1.Container{
		Name:            "storage-sync",
		Image:           "ntfrnzn/cloud-storage-sync",
		ImagePullPolicy: corev1.PullIfNotPresent,
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

	modObj.Spec = newSpec
	patchBytes, err := GetJsonPatch(sourceObj, modObj)
	if err != nil {
		log.Error(err, "unable to generate patch, continuing", "pod", sourceObj.Name)
		return resp, nil
	}

	log.Info("patching pod", "patch", string(patchBytes))
	resp.PatchType = &jsonPatchType
	resp.Patch = patchBytes

	return resp, nil
}
