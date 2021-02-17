package admission

import (
	"fmt"
	"strconv"

	"github.com/go-logr/logr"
	"github.com/spotinst/wave-operator/cloudstorage"
	"github.com/spotinst/wave-operator/internal/storagesync"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
)

var (
	ondemandAffinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			// TODO make this required
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
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
	volume = corev1.Volume{
		Name: "spark-logs",
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	}
	volumeMount = corev1.VolumeMount{
		Name:      volume.Name,
		MountPath: "/var/log/spark",
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

	log.Info("pod admission control", "mountPath", volumeMount.MountPath)

	webServerPort := strconv.Itoa(int(storagesync.Port))
	storageContainer := corev1.Container{
		Name:            storagesync.ContainerName,
		Image:           "public.ecr.aws/l8m2k1n1/netapp/cloud-storage-sync:v0.4.0",
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"/tini"},
		Args:            []string{"--", "./run.sh", volumeMount.MountPath, "spark:" + storageInfo.Name, "forever", webServerPort},
		Env:             []corev1.EnvVar{{Name: "S3_REGION", Value: storageInfo.Region}},
		Lifecycle: &corev1.Lifecycle{
			PreStop: &corev1.Handler{
				Exec: &corev1.ExecAction{
					// If a driver pod is deleted while running, the driver container and the storage-sync container
					// are killed in parallel. There is no guarantee that the driver container writes the final log file
					// before the storage-sync container's preStop hook executes.
					// Let's just sync "forever", until we either see the final log file and exit successfully,
					// or the pod's grace period passes.
					Command: []string{"./run.sh", volumeMount.MountPath, "spark:" + storageInfo.Name, "forever"},
				},
			},
		},
		Ports: []corev1.ContainerPort{
			{
				ContainerPort: storagesync.Port,
			},
		},
	}

	exists := false
	for _, c := range newSpec.Containers {
		if c.Name == storageContainer.Name {
			exists = true
			break
		}
	}
	if !exists {
		// Add storage sidecar container to front of containers list so it gets started first
		newContainers := make([]corev1.Container, 0)
		newContainers = append(newContainers, storageContainer)
		newContainers = append(newContainers, newSpec.Containers...)
		newSpec.Containers = newContainers
	}

	// mount shared volume to all
	for i := range newSpec.Containers {
		if newSpec.Containers[i].VolumeMounts == nil {
			newSpec.Containers[i].VolumeMounts = []corev1.VolumeMount{}
		}
		exists := false
		for _, v := range newSpec.Containers[i].VolumeMounts {
			if v.Name == volumeMount.Name {
				exists = true
				break
			}
		}
		if !exists {
			newSpec.Containers[i].VolumeMounts = append(newSpec.Containers[i].VolumeMounts, volumeMount)
		}
	}
	exists = false
	for _, v := range newSpec.Volumes {
		if v.Name == volume.Name {
			exists = true
			break
		}
	}
	if !exists {
		newSpec.Volumes = append(newSpec.Volumes, volume)
	}

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
