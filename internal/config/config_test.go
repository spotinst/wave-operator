package config

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func TestIsEventLogSyncEnabled(t *testing.T) {

	res := IsEventLogSyncEnabled(nil)
	assert.Equal(t, false, res)

	annotations := make(map[string]string)
	res = IsEventLogSyncEnabled(annotations)
	assert.Equal(t, false, res)

	annotations = make(map[string]string)
	annotations[WaveConfigAnnotationSyncEventLogs] = "true"
	res = IsEventLogSyncEnabled(annotations)
	assert.Equal(t, true, res)

	annotations = make(map[string]string)
	annotations[WaveConfigAnnotationSyncEventLogsOld] = "true"
	res = IsEventLogSyncEnabled(annotations)
	assert.Equal(t, true, res)

	annotations = make(map[string]string)
	annotations[WaveConfigAnnotationSyncEventLogs] = "false"
	res = IsEventLogSyncEnabled(annotations)
	assert.Equal(t, false, res)

	annotations = make(map[string]string)
	annotations[WaveConfigAnnotationSyncEventLogsOld] = "false"
	res = IsEventLogSyncEnabled(annotations)
	assert.Equal(t, false, res)

	annotations = make(map[string]string)
	annotations[WaveConfigAnnotationSyncEventLogs] = "false"
	annotations[WaveConfigAnnotationSyncEventLogsOld] = "true"
	res = IsEventLogSyncEnabled(annotations)
	assert.Equal(t, false, res)

	annotations = make(map[string]string)
	annotations[WaveConfigAnnotationSyncEventLogs] = "true"
	annotations[WaveConfigAnnotationSyncEventLogsOld] = "false"
	res = IsEventLogSyncEnabled(annotations)
	assert.Equal(t, true, res)

}

func TestGetConfiguredInstanceTypes(t *testing.T) {

	logger := getTestLogger()

	res := GetConfiguredInstanceTypes(nil, logger)
	assert.Equal(t, 0, len(res))

	res = GetConfiguredInstanceTypes(make(map[string]string), logger)
	assert.Equal(t, 0, len(res))

	annotations := make(map[string]string)

	annotations[WaveConfigAnnotationInstanceType] = ""
	res = GetConfiguredInstanceTypes(annotations, logger)
	assert.Equal(t, 0, len(res))

	annotations[WaveConfigAnnotationInstanceType] = "nonsense"
	res = GetConfiguredInstanceTypes(annotations, logger)
	assert.Equal(t, 0, len(res))

	annotations[WaveConfigAnnotationInstanceType] = "."
	res = GetConfiguredInstanceTypes(annotations, logger)
	assert.Equal(t, 0, len(res))

	annotations[WaveConfigAnnotationInstanceType] = "m5."
	res = GetConfiguredInstanceTypes(annotations, logger)
	assert.Equal(t, 0, len(res))

	annotations[WaveConfigAnnotationInstanceType] = ".large"
	res = GetConfiguredInstanceTypes(annotations, logger)
	assert.Equal(t, 0, len(res))

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge" // Valid input
	res = GetConfiguredInstanceTypes(annotations, logger)
	assert.Equal(t, []string{"m5.xlarge"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5,t2.micro" // Instance family specified
	res = GetConfiguredInstanceTypes(annotations, logger)
	assert.Equal(t, []string{"m5.xlarge", "t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.,.xlarge,t2.micro" // Malformed input
	res = GetConfiguredInstanceTypes(annotations, logger)
	assert.Equal(t, []string{"t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5,2xlarge,t2.micro" // Malformed input with comma
	res = GetConfiguredInstanceTypes(annotations, logger)
	assert.Equal(t, []string{"m5.xlarge", "t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5.2xlarge,,t2.micro" // Malformed input with extra comma
	res = GetConfiguredInstanceTypes(annotations, logger)
	assert.Equal(t, []string{"m5.xlarge", "m5.2xlarge", "t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5.2xlarge,t2.micro," // Malformed input with extra comma
	res = GetConfiguredInstanceTypes(annotations, logger)
	assert.Equal(t, []string{"m5.xlarge", "m5.2xlarge", "t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5.2xlarge,.,t2.micro" // Malformed input with dot
	res = GetConfiguredInstanceTypes(annotations, logger)
	assert.Equal(t, []string{"m5.xlarge", "m5.2xlarge", "t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge, m5.2xlarge, t2.micro" // Valid input with spaces
	res = GetConfiguredInstanceTypes(annotations, logger)
	assert.Equal(t, []string{"m5.xlarge", "m5.2xlarge", "t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge , m5.2xlarge , t2.micro" // Valid input with spaces
	res = GetConfiguredInstanceTypes(annotations, logger)
	assert.Equal(t, []string{"m5.xlarge", "m5.2xlarge", "t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5.2xlarge,t2.micro" // Valid input
	res = GetConfiguredInstanceTypes(annotations, logger)
	assert.Equal(t, []string{"m5.xlarge", "m5.2xlarge", "t2.micro"}, res)

}

func getTestLogger() logr.Logger {
	return zap.New(zap.UseDevMode(true))
}
