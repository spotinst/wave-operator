package config

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
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

func GetConfiguredInstanceTypes(annotations map[string]string, log logr.Logger) []string {
	instanceTypes := make([]string, 0)
	if annotations == nil {
		return instanceTypes
	}
	conf := annotations[WaveConfigAnnotationInstanceType]
	if conf == "" {
		return instanceTypes
	}
	split := strings.Split(conf, ",")
	for _, s := range split {
		trimmed := strings.TrimSpace(s)
		if validateInstanceType(trimmed) {
			instanceTypes = append(instanceTypes, trimmed)
		} else {
			log.Info(fmt.Sprintf("Got invalid instance type %q, ignoring", trimmed))
		}
	}
	return instanceTypes
}

// TODO(thorsteinn) Make sure that the instance type is valid in the cluster region,
// and allowed in the cluster configuration
func validateInstanceType(instanceType string) bool {
	// Instance types should be of the form family.type (e.g. m5.xlarge)
	split := strings.Split(instanceType, ".")
	if len(split) != 2 {
		return false
	}
	for _, s := range split {
		if len(s) == 0 {
			return false
		}
	}
	return true
}
