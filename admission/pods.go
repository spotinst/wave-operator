package admission

import (
	"fmt"
	"strconv"

	"github.com/go-logr/logr"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/spotinst/wave-operator/cloudstorage"
	"github.com/spotinst/wave-operator/internal/config"
	"github.com/spotinst/wave-operator/internal/storagesync"
)

const (
	SparkRoleLabel         = "spark-role"
	SparkRoleDriverValue   = "driver"
	SparkRoleExecutorValue = "executor"

	nodeLifeCycleKey           = "spotinst.io/node-lifecycle"
	nodeLifeCycleValueOnDemand = "od"

	nodeInstanceTypeKey     = "node.kubernetes.io/instance-type"
	nodeInstanceTypeKeyBeta = "beta.kubernetes.io/instance-type" // k8s version <v1.17
)

var (
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
	log := m.log.WithValues("pod", sourceObj.Name, "annotations", sourceObj.Annotations)

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

	var modObj *corev1.Pod
	if sparkRole == SparkRoleDriverValue {
		log.Info("Mutating driver pod")
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
	m.buildAffinityDriver(modObj)
	//modObj.Spec.Affinity = onDemandAffinity

	if !config.IsEventLogSyncEnabled(sourceObj.Annotations) {
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
		Name:            storagesync.SyncContainerName,
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
	m.buildAffinityExecutor(modObj)
	//modObj.Spec.Affinity = onDemandAntiAffinity
	return modObj
}

type nodeAffinityConfig struct {
	// instanceLifecycle is the life cycle of the node (on-demand vs spot)
	instanceLifecycle config.InstanceLifecycle
	// instanceTypes is a list of allowed instance types for the pod to run on
	instanceTypes []string
}

func (m PodMutator) getNodeAffinityConfig(annotations map[string]string, defaultLifecycle config.InstanceLifecycle) nodeAffinityConfig {
	lifecycle := config.GetInstanceLifecycle(annotations, m.log)
	if lifecycle == "" {
		// Use default
		lifecycle = defaultLifecycle
	}

	instanceTypes := config.GetConfiguredInstanceTypes(annotations, m.log)

	return nodeAffinityConfig{
		instanceLifecycle: lifecycle,
		instanceTypes:     instanceTypes,
	}
}

func (m PodMutator) buildAffinityDriver(pod *corev1.Pod) {
	conf := m.getNodeAffinityConfig(pod.Annotations, config.InstanceLifecycleOnDemand)
	m.buildAffinity(pod, conf)
}

func (m PodMutator) buildAffinityExecutor(pod *corev1.Pod) {
	conf := m.getNodeAffinityConfig(pod.Annotations, config.InstanceLifecycleSpot)
	m.buildAffinity(pod, conf)
}

func (m PodMutator) buildAffinity(pod *corev1.Pod, conf nodeAffinityConfig) {
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}

	// If any of the node affinity keys we want to set have been set already by some other means we will not touch them.
	// TODO(thorsteinn) Should we override the existing configuration?

	// Set node lifecycle
	if isNodeAffinityKeySet(pod.Spec.Affinity.NodeAffinity, nodeLifeCycleKey) {
		m.log.Info(fmt.Sprintf("Node affinity key %q already set, will not be mutated", nodeLifeCycleKey))
	} else {
		switch conf.instanceLifecycle {
		case config.InstanceLifecycleOnDemand:
			m.buildRequiredOnDemandAffinity(pod.Spec.Affinity.NodeAffinity)
		case config.InstanceLifecycleSpot:
			m.buildPreferredOnDemandAntiAffinity(pod.Spec.Affinity.NodeAffinity)
		}
	}

	if len(conf.instanceTypes) > 0 {
		if isNodeAffinityKeySet(pod.Spec.Affinity.NodeAffinity, nodeInstanceTypeKey) ||
			isNodeAffinityKeySet(pod.Spec.Affinity.NodeAffinity, nodeInstanceTypeKeyBeta) {
			m.log.Info(fmt.Sprintf("Node affinity keys %q or %q already set, will not be mutated",
				nodeInstanceTypeKey, nodeInstanceTypeKeyBeta))
		} else {
			m.buildRequiredInstanceTypeAffinity(pod.Spec.Affinity.NodeAffinity, conf.instanceTypes)
		}
	}
}

// buildRequiredOnDemandAffinity builds a required affinity to on-demand nodes
func (m PodMutator) buildRequiredOnDemandAffinity(nodeAffinity *corev1.NodeAffinity) {
	nodeSelectorRequirement := corev1.NodeSelectorRequirement{
		Key:      nodeLifeCycleKey,
		Operator: corev1.NodeSelectorOpIn,
		Values:   []string{nodeLifeCycleValueOnDemand},
	}

	if nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{}
	}

	nodeSelector := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution

	// Node selector terms are ORed, and expressions are ANDed.
	// Let's add the node lifecycle requirement to all node selector terms.

	if len(nodeSelector.NodeSelectorTerms) == 0 {
		nodeSelector.NodeSelectorTerms = []corev1.NodeSelectorTerm{{}}
	}

	for i := range nodeSelector.NodeSelectorTerms {
		nodeSelector.NodeSelectorTerms[i].MatchExpressions = append(
			nodeSelector.NodeSelectorTerms[i].MatchExpressions,
			nodeSelectorRequirement)
	}
}

// buildPreferredOnDemandAntiAffinity builds a preferred anti affinity to on-demand nodes
func (m PodMutator) buildPreferredOnDemandAntiAffinity(nodeAffinity *corev1.NodeAffinity) {
	nodeSelectorRequirement := corev1.NodeSelectorRequirement{
		Key:      nodeLifeCycleKey,
		Operator: corev1.NodeSelectorOpNotIn,
		Values:   []string{nodeLifeCycleValueOnDemand},
	}

	// Add new preferred scheduling term
	// The weights of preferred scheduling terms are summed up to find the most suitable node
	nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
		nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
		corev1.PreferredSchedulingTerm{
			Weight: 1,
			Preference: corev1.NodeSelectorTerm{
				MatchExpressions: []corev1.NodeSelectorRequirement{
					nodeSelectorRequirement,
				},
			},
		})
}

func (m PodMutator) buildRequiredInstanceTypeAffinity(nodeAffinity *corev1.NodeAffinity, instanceTypes []string) {
	nodeSelectorRequirement := corev1.NodeSelectorRequirement{
		Key:      nodeInstanceTypeKey,
		Operator: corev1.NodeSelectorOpIn,
		Values:   instanceTypes,
	}

	nodeSelectorRequirementBeta := corev1.NodeSelectorRequirement{
		Key:      nodeInstanceTypeKeyBeta,
		Operator: corev1.NodeSelectorOpIn,
		Values:   instanceTypes,
	}

	if nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{}
	}

	nodeSelector := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution

	// Node selector terms are ORed, and expressions are ANDed.
	// Let's add the node selector requirement to all node selector terms.

	if len(nodeSelector.NodeSelectorTerms) == 0 {
		nodeSelector.NodeSelectorTerms = []corev1.NodeSelectorTerm{{}}
	}

	for i := range nodeSelector.NodeSelectorTerms {
		nodeSelector.NodeSelectorTerms[i].MatchExpressions = append(
			nodeSelector.NodeSelectorTerms[i].MatchExpressions,
			nodeSelectorRequirement, nodeSelectorRequirementBeta) // TODO This does not work - they are ANDed
	}
}

// isNodeAffinityKeySet determines if the given node affinity key set as either preferred or required
func isNodeAffinityKeySet(na *corev1.NodeAffinity, key string) bool {
	if na.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		nodeSelector := na.RequiredDuringSchedulingIgnoredDuringExecution
		for _, term := range nodeSelector.NodeSelectorTerms {
			for _, expr := range term.MatchExpressions {
				if expr.Key == key {
					return true
				}
			}
		}
	}

	for _, pst := range na.PreferredDuringSchedulingIgnoredDuringExecution {
		for _, expr := range pst.Preference.MatchExpressions {
			if expr.Key == key {
				return true
			}
		}
	}

	return false
}
