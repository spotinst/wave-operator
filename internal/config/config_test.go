package config

import (
	"fmt"
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

func TestGetConfiguredInstanceTypes(t *testing.T) {

	logger := getTestLogger()

	type testCase struct {
		annotations                             map[string]string
		expectedInstanceTypeValidationCallCount int
		expectedFamilyValidationCallCount       int
		expected                                []string
	}

	testInstanceTypeValidationFunc := func(s string) error {
		switch s {
		case "m5.xlarge", "m5.2xlarge", "t2.micro":
			return nil
		default:
			return fmt.Errorf("invalid instance type %q", s)
		}
	}

	testInstanceTypeFamilyValidationFunc := func(s string) ([]string, error) {
		switch s {
		case "h1":
			return []string{"h1.small", "h1.medium", "h1.large"}, nil
		default:
			return nil, fmt.Errorf("invalid instance type family %q", s)
		}
	}

	testFunc := func(tt *testing.T, tc testCase) {

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockManager := mock_instances.NewMockInstanceTypeManager(ctrl)
		mockManager.EXPECT().ValidateInstanceType(gomock.Any()).DoAndReturn(testInstanceTypeValidationFunc).Times(tc.expectedInstanceTypeValidationCallCount)
		mockManager.EXPECT().GetValidInstanceTypesInFamily(gomock.Any()).DoAndReturn(testInstanceTypeFamilyValidationFunc).Times(tc.expectedFamilyValidationCallCount)

		res := GetConfiguredInstanceTypes(tc.annotations, mockManager, logger)
		assert.Equal(tt, tc.expected, res)

	}

	t.Run("whenNilAnnotations", func(tt *testing.T) {
		tc := testCase{
			annotations:                             nil,
			expectedInstanceTypeValidationCallCount: 0,
			expectedFamilyValidationCallCount:       0,
			expected:                                []string{},
		}
		testFunc(tt, tc)
	})

	t.Run("whenEmptyAnnotations", func(tt *testing.T) {
		tc := testCase{
			annotations:                             make(map[string]string),
			expectedInstanceTypeValidationCallCount: 0,
			expectedFamilyValidationCallCount:       0,
			expected:                                []string{},
		}
		testFunc(tt, tc)
	})

	t.Run("whenValidInstanceTypes", func(tt *testing.T) {
		annotations := make(map[string]string)

		annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge, m5.2xlarge, t2.micro " // With spaces
		tc := testCase{
			annotations:                             annotations,
			expectedInstanceTypeValidationCallCount: 3,
			expectedFamilyValidationCallCount:       0,
			expected:                                []string{"m5.2xlarge", "m5.xlarge", "t2.micro"},
		}
		testFunc(tt, tc)

		annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5.2xlarge,t2.micro" // Without spaces
		tc = testCase{
			annotations:                             annotations,
			expectedInstanceTypeValidationCallCount: 3,
			expectedFamilyValidationCallCount:       0,
			expected:                                []string{"m5.2xlarge", "m5.xlarge", "t2.micro"},
		}
		testFunc(tt, tc)
	})

	t.Run("whenValidInstanceTypeFamily", func(tt *testing.T) {
		annotations := make(map[string]string)
		annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge, m5.2xlarge, h1, t2.micro"
		tc := testCase{
			annotations:                             annotations,
			expectedInstanceTypeValidationCallCount: 4,
			expectedFamilyValidationCallCount:       1,
			expected:                                []string{"h1.large", "h1.medium", "h1.small", "m5.2xlarge", "m5.xlarge", "t2.micro"},
		}
		testFunc(tt, tc)
	})

	t.Run("whenInvalidInstanceType", func(tt *testing.T) {
		annotations := make(map[string]string)
		annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge, nonsense, m5.2xlarge, t2.micro"
		tc := testCase{
			annotations:                             annotations,
			expectedInstanceTypeValidationCallCount: 4,
			expectedFamilyValidationCallCount:       1,
			expected:                                []string{"m5.2xlarge", "m5.xlarge", "t2.micro"},
		}
		testFunc(tt, tc)
	})

	t.Run("whenMalformedInput", func(tt *testing.T) {
		annotations := make(map[string]string)

		annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5,2xlarge,t2.micro" // Malformed input with comma
		tc := testCase{
			annotations:                             annotations,
			expectedInstanceTypeValidationCallCount: 4,
			expectedFamilyValidationCallCount:       2,
			expected:                                []string{"m5.xlarge", "t2.micro"},
		}
		testFunc(tt, tc)

		annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5.2xlarge,,t2.micro" // Malformed input with extra comma
		tc = testCase{
			annotations:                             annotations,
			expectedInstanceTypeValidationCallCount: 4,
			expectedFamilyValidationCallCount:       1,
			expected:                                []string{"m5.2xlarge", "m5.xlarge", "t2.micro"},
		}
		testFunc(tt, tc)

		annotations[WaveConfigAnnotationInstanceType] = "m5.xlarge,m5.2xlarge,t2.micro," // Malformed input with extra comma
		tc = testCase{
			annotations:                             annotations,
			expectedInstanceTypeValidationCallCount: 4,
			expectedFamilyValidationCallCount:       1,
			expected:                                []string{"m5.2xlarge", "m5.xlarge", "t2.micro"},
		}
		testFunc(tt, tc)
	})

	t.Run("whenDuplicateInput", func(tt *testing.T) {
		annotations := make(map[string]string)
		annotations[WaveConfigAnnotationInstanceType] = "h1, m5.xlarge, m5.2xlarge, m5.2xlarge, t2.micro, h1"
		tc := testCase{
			annotations:                             annotations,
			expectedInstanceTypeValidationCallCount: 6,
			expectedFamilyValidationCallCount:       2,
			expected:                                []string{"h1.large", "h1.medium", "h1.small", "m5.2xlarge", "m5.xlarge", "t2.micro"},
		}
		testFunc(tt, tc)
	})

}

func getTestLogger() logr.Logger {
	return zap.New(zap.UseDevMode(true))
}
