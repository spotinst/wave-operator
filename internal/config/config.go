package config

import (
	"fmt"
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
		expanded, err := instances.ValidateAndExpandFamily(trimmed, instanceTypeManager.GetAllowedInstanceTypes())
		if err != nil {
			log.Info(fmt.Sprintf("Ignoring invalid instance type %q, error: %s", trimmed, err))
		} else {
			for _, instanceType := range expanded {
				if !containsString(instanceTypes, instanceType) {
					instanceTypes = append(instanceTypes, instanceType)
				}
			}
		}
	}
	return instanceTypes
}

func containsString(list []string, target string) bool {
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
}
