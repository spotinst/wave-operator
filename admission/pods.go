package admission

import (
	"fmt"
	"strconv"

	"github.com/go-logr/logr"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/spotinst/wave-operator/cloudstorage"
	"github.com/spotinst/wave-operator/internal/storagesync"
)

const (
	SparkRoleLabel         = "spark-role"
	SparkRoleDriverValue   = "driver"
	SparkRoleExecutorValue = "executor"
)

var (
	onDemandAffinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
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
	onDemandAntiAffinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{
				{
					Weight: 1,
					Preference: corev1.NodeSelectorTerm{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      "spotinst.io/node-lifecycle",
								Operator: corev1.NodeSelectorOpNotIn,
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

type PodMutator struct {
	storageProvider cloudstorage.CloudStorageProvider
	log             logr.Logger
}

func NewPodMutator(log logr.Logger, storageProvider cloudstorage.CloudStorageProvider) PodMutator {
	return PodMutator{
		storageProvider: storageProvider,
		log:             log,
	}
}

func (m PodMutator) Mutate(req *admissionv1.AdmissionRequest) (*admissionv1.AdmissionResponse, error) {

	gvk := corev1.SchemeGroupVersion.WithKind("Pod")
	sourceObj := &corev1.Pod{}

	_, _, err := deserializer.Decode(req.Object.Raw, &gvk, sourceObj)
	if err != nil {
		return nil, fmt.Errorf("deserialization failed, %w", err)
	}
	log := m.log.WithValues("pod", sourceObj.Name)

	resp := &admissionv1.AdmissionResponse{
		UID:     req.UID,
		Allowed: true,
	}

	if sourceObj.Labels == nil {
		return resp, nil
	}

	sparkRole := sourceObj.Labels[SparkRoleLabel]

	if sparkRole == "" {
		return resp, nil
	}

	modObj := &corev1.Pod{}
	if sparkRole == SparkRoleDriverValue {
		log.Info("Mutating driver pod", "annotations", sourceObj.Annotations)
		modObj = m.mutateDriverPod(sourceObj)
	} else {
		log.Info("Mutating executor pod")
		modObj = m.mutateExecutorPod(sourceObj)
	}

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

func (m PodMutator) mutateDriverPod(sourceObj *corev1.Pod) *corev1.Pod {

	modObj := sourceObj.DeepCopy()

	// node affinity
	modObj.Spec.Affinity = onDemandAffinity

	if !isEventLogSyncEnabled(sourceObj.Annotations) {
		m.log.Info("Event log sync not enabled, will not add storage sync container")
		return modObj
	}

	storageInfo, err := m.storageProvider.GetStorageInfo()
	if err != nil {
		m.log.Error(err, "could not get storage info, will not add storage sync container")
		return modObj
	}

	if storageInfo == nil {
		m.log.Error(fmt.Errorf("storage configuration is nil"), "will not add storage sync container")
		return modObj
	}

	m.log.Info("driver pod admission control", "mountPath", volumeMount.MountPath)

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

	// add storage sync container
	exists := false
	for _, c := range modObj.Spec.Containers {
		if c.Name == storageContainer.Name {
			exists = true
			break
		}
	}
	if !exists {
		// Add storage sidecar container to front of containers list so it gets started first
		newContainers := make([]corev1.Container, 0)
		newContainers = append(newContainers, storageContainer)
		newContainers = append(newContainers, modObj.Spec.Containers...)
		modObj.Spec.Containers = newContainers
	}

	// add volume mount to all containers
	for i := range modObj.Spec.Containers {
		if modObj.Spec.Containers[i].VolumeMounts == nil {
			modObj.Spec.Containers[i].VolumeMounts = []corev1.VolumeMount{}
		}
		exists := false
		for _, v := range modObj.Spec.Containers[i].VolumeMounts {
			if v.Name == volumeMount.Name {
				exists = true
				break
			}
		}
		if !exists {
			modObj.Spec.Containers[i].VolumeMounts = append(modObj.Spec.Containers[i].VolumeMounts, volumeMount)
		}
	}

	// add volume
	exists = false
	for _, v := range modObj.Spec.Volumes {
		if v.Name == volume.Name {
			exists = true
			break
		}
	}
	if !exists {
		modObj.Spec.Volumes = append(modObj.Spec.Volumes, volume)
	}

	return modObj
}

func (m PodMutator) mutateExecutorPod(sourceObj *corev1.Pod) *corev1.Pod {
	modObj := sourceObj.DeepCopy()
	// node affinity
	modObj.Spec.Affinity = onDemandAntiAffinity
	return modObj
}
