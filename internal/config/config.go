package config

import (
	"strconv"
)

const (
	WaveConfigAnnotationSyncEventLogs = "wave.spot.io/synceventlogs"
)

func IsEventLogSyncEnabled(annotations map[string]string) bool {
	if annotations == nil {
		return false
	}
	enabled, err := strconv.ParseBool(annotations[WaveConfigAnnotationSyncEventLogs])
	if err != nil {
		return false
	}
	return enabled
}
