package config

import (
	"strings"
)

const (
	WaveConfigAnnotationSyncEventLogs = "wave.spot.io/synceventlogs"
)

func IsEventLogSyncEnabled(annotations map[string]string) bool {
	if annotations == nil {
		return false
	}
	storageSyncOn, ok := annotations[WaveConfigAnnotationSyncEventLogs]
	if !ok {
		return false
	}
	if strings.ToUpper(storageSyncOn) == "TRUE" {
		return true
	}
	return false
}
