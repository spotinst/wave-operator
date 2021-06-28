package config

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/go-logr/logr"

	"github.com/spotinst/wave-operator/internal/config/instances"
)

const (
	WaveConfigAnnotationSyncEventLogs     = "wave.spot.io/sync-event-logs"
	WaveConfigAnnotationSyncEventLogsOld  = "wave.spot.io/synceventlogs"
	WaveConfigAnnotationInstanceType      = "wave.spot.io/instance-type"
	WaveConfigAnnotationInstanceLifecycle = "wave.spot.io/instance-lifecycle"
	WaveConfigAnnotationApplicationName   = "wave.spot.io/application-name"

	InstanceLifecycleOnDemand InstanceLifecycle = "od"
	InstanceLifecycleSpot     InstanceLifecycle = "spot"
)

type InstanceLifecycle string

func IsEventLogSyncEnabled(annotations map[string]string) bool {
	if annotations == nil {
		return false
	}
	conf := annotations[WaveConfigAnnotationSyncEventLogs]

	// TODO(thorsteinn) Backwards compatibility, remove when documentation up to date
	if conf == "" {
		conf = annotations[WaveConfigAnnotationSyncEventLogsOld]
	}

	enabled, err := strconv.ParseBool(conf)
	if err != nil {
		return false
	}
	return enabled
}

func GetInstanceLifecycle(annotations map[string]string, log logr.Logger) InstanceLifecycle {
	conf := annotations[WaveConfigAnnotationInstanceLifecycle]
	if conf == "" {
		return ""
	}
	conf = strings.ToLower(conf)
	conf = strings.TrimSpace(conf)
	switch conf {
	case "od":
		return InstanceLifecycleOnDemand
	case "spot":
		return InstanceLifecycleSpot
	default:
		log.Info(fmt.Sprintf("Unknown instance lifecycle configuration value: %q", conf))
		return ""
	}
}

func GetConfiguredInstanceTypes(annotations map[string]string, instanceTypeManager instances.InstanceTypeManager, log logr.Logger) []string {
	if annotations == nil {
		return []string{}
	}
	conf := annotations[WaveConfigAnnotationInstanceType]
	if conf == "" {
		return []string{}
	}
	instanceTypes := make(map[string]bool)
	split := strings.Split(conf, ",")
	for _, s := range split {
		trimmed := strings.TrimSpace(s)
		// Is this a valid instance type?
		err := instanceTypeManager.ValidateInstanceType(trimmed)
		if err == nil {
			instanceTypes[trimmed] = true
		} else {
			// Is this a valid instance type family?
			instanceTypesInFamily, err := instanceTypeManager.GetValidInstanceTypesInFamily(trimmed)
			if err == nil {
				for _, it := range instanceTypesInFamily {
					instanceTypes[it] = true
				}
			} else {
				log.Info(fmt.Sprintf("Ignoring invalid instance type %q", trimmed))
			}
		}
	}
	return mapKeysToSlice(instanceTypes)
}

func mapKeysToSlice(m map[string]bool) []string {
	res := make([]string, len(m))
	i := 0
	for k := range m {
		res[i] = k
		i++
	}
	sort.Strings(res) // Enforce stable order
	return res
}
