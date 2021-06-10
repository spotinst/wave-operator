package config

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/spotinst/wave-operator/internal/config/instances/mock_instances"
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

func TestGetConfiguredInstanceTypes_whenAllowedInstanceTypesUnknown(t *testing.T) {

	logger := getTestLogger()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockManager := mock_instances.NewMockInstanceTypeManager(ctrl)
	mockManager.EXPECT().GetAllowedInstanceTypes().Return(nil).MinTimes(1)

	res := GetConfiguredInstanceTypes(nil, mockManager, logger)
	assert.Equal(t, 0, len(res))

	res = GetConfiguredInstanceTypes(make(map[string]string), mockManager, logger)
	assert.Equal(t, 0, len(res))

	annotations := make(map[string]string)

	annotations[WaveConfigAnnotationInstanceType] = ""
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, 0, len(res))

	annotations[WaveConfigAnnotationInstanceType] = "nonsense"
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, 0, len(res))

	annotations[WaveConfigAnnotationInstanceType] = "."
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, 0, len(res))

	annotations[WaveConfigAnnotationInstanceType] = "m5."
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, 0, len(res))

	annotations[WaveConfigAnnotationInstanceType] = ".large"
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, 0, len(res))

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge" // Valid input
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"m5.xlarge"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5,t2.micro" // Instance family specified
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"m5.xlarge", "t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.,.xlarge,t2.micro" // Malformed input
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5,2xlarge,t2.micro" // Malformed input with comma
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"m5.xlarge", "t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5.2xlarge,,t2.micro" // Malformed input with extra comma
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"m5.xlarge", "m5.2xlarge", "t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5.2xlarge,t2.micro," // Malformed input with extra comma
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"m5.xlarge", "m5.2xlarge", "t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5.2xlarge,.,t2.micro" // Malformed input with dot
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"m5.xlarge", "m5.2xlarge", "t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge, m5.2xlarge, t2.micro" // Valid input with spaces
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"m5.xlarge", "m5.2xlarge", "t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge , m5.2xlarge , t2.micro" // Valid input with spaces
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"m5.xlarge", "m5.2xlarge", "t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5.2xlarge,t2.micro" // Valid input
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"m5.xlarge", "m5.2xlarge", "t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5.xlarge,t2.micro" // Duplicate input
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"m5.xlarge", "t2.micro"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5.2xlarge.test,t2.micro" // Invalid input
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"m5.xlarge", "t2.micro"}, res)

}

func TestGetConfiguredInstanceTypes_whenAllowedInstanceTypes(t *testing.T) {

	logger := getTestLogger()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	allowedInstanceTypes := map[string]map[string]bool{
		"m5": {
			"m5.large":  true,
			"m5.xlarge": true,
		},
		"r3": {
			"r3.small": true,
			"r3.large": true,
		},
		"h1": {
			"h1.small":  true,
			"h1.medium": true,
			"h1.large":  true,
		},
		"g5": {},
	}

	mockManager := mock_instances.NewMockInstanceTypeManager(ctrl)
	mockManager.EXPECT().GetAllowedInstanceTypes().Return(allowedInstanceTypes).MinTimes(1)

	annotations := make(map[string]string)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5.large,r3.small" // All valid
	res := GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"m5.xlarge", "m5.large", "r3.small"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5.1024large,r3.small" // One not allowed, family allowed
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"m5.xlarge", "r3.small"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,c5.large,r3.small" // One not allowed, family not allowed
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"m5.xlarge", "r3.small"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5.large.test,r3.small" // Invalid input
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"m5.xlarge", "r3.small"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,h1,r3.small" // Instance type family specified
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, 5, len(res))
	assert.Contains(t, res, "m5.xlarge")
	assert.Contains(t, res, "h1.small")
	assert.Contains(t, res, "h1.medium")
	assert.Contains(t, res, "h1.large")
	assert.Contains(t, res, "r3.small")

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,nonsense,r3.small" // Invalid family specified
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"m5.xlarge", "r3.small"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,withdot.,,r3.small" // Invalid input
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, []string{"m5.xlarge", "r3.small"}, res)

	annotations[WaveConfigAnnotationInstanceType] = "m5,r3,h1,r3" // Duplicate input
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, 7, len(res))
	assert.Contains(t, res, "m5.large")
	assert.Contains(t, res, "m5.xlarge")
	assert.Contains(t, res, "r3.small")
	assert.Contains(t, res, "r3.large")
	assert.Contains(t, res, "h1.small")
	assert.Contains(t, res, "h1.medium")
	assert.Contains(t, res, "h1.large")

	annotations[WaveConfigAnnotationInstanceType] = "m5,g5" // Empty family
	res = GetConfiguredInstanceTypes(annotations, mockManager, logger)
	assert.Equal(t, 2, len(res))
	assert.Contains(t, res, "m5.large")
	assert.Contains(t, res, "m5.xlarge")

}

func getTestLogger() logr.Logger {
	return zap.New(zap.UseDevMode(true))
}
