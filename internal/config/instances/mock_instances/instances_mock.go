// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/spotinst/wave-operator/internal/config/instances (interfaces: InstanceTypeManager)

// Package mock_instances is a generated GoMock package.
package mock_instances

import (
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
)

// MockInstanceTypeManager is a mock of InstanceTypeManager interface
type MockInstanceTypeManager struct {
	ctrl     *gomock.Controller
	recorder *MockInstanceTypeManagerMockRecorder
}

// MockInstanceTypeManagerMockRecorder is the mock recorder for MockInstanceTypeManager
type MockInstanceTypeManagerMockRecorder struct {
	mock *MockInstanceTypeManager
}

// NewMockInstanceTypeManager creates a new mock instance
func NewMockInstanceTypeManager(ctrl *gomock.Controller) *MockInstanceTypeManager {
	mock := &MockInstanceTypeManager{ctrl: ctrl}
	mock.recorder = &MockInstanceTypeManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockInstanceTypeManager) EXPECT() *MockInstanceTypeManagerMockRecorder {
	return m.recorder
}

// GetValidInstanceTypesInFamily mocks base method
func (m *MockInstanceTypeManager) GetValidInstanceTypesInFamily(arg0 string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetValidInstanceTypesInFamily", arg0)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetValidInstanceTypesInFamily indicates an expected call of GetValidInstanceTypesInFamily
func (mr *MockInstanceTypeManagerMockRecorder) GetValidInstanceTypesInFamily(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetValidInstanceTypesInFamily", reflect.TypeOf((*MockInstanceTypeManager)(nil).GetValidInstanceTypesInFamily), arg0)
}

// Start mocks base method
func (m *MockInstanceTypeManager) Start() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Start")
	ret0, _ := ret[0].(error)
	return ret0
}

// Start indicates an expected call of Start
func (mr *MockInstanceTypeManagerMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockInstanceTypeManager)(nil).Start))
}

// ValidateInstanceType mocks base method
func (m *MockInstanceTypeManager) ValidateInstanceType(arg0 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ValidateInstanceType", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// ValidateInstanceType indicates an expected call of ValidateInstanceType
func (mr *MockInstanceTypeManagerMockRecorder) ValidateInstanceType(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ValidateInstanceType", reflect.TypeOf((*MockInstanceTypeManager)(nil).ValidateInstanceType), arg0)
}
