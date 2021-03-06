// Code generated by MockGen. DO NOT EDIT.
// Source: cloudstorage/storage.go

// Package mock_cloudstorage is a generated GoMock package.
package mock_cloudstorage

import (
	gomock "github.com/golang/mock/gomock"
	cloudstorage "github.com/spotinst/wave-operator/cloudstorage"
	reflect "reflect"
)

// MockCloudStorageProvider is a mock of CloudStorageProvider interface
type MockCloudStorageProvider struct {
	ctrl     *gomock.Controller
	recorder *MockCloudStorageProviderMockRecorder
}

// MockCloudStorageProviderMockRecorder is the mock recorder for MockCloudStorageProvider
type MockCloudStorageProviderMockRecorder struct {
	mock *MockCloudStorageProvider
}

// NewMockCloudStorageProvider creates a new mock instance
func NewMockCloudStorageProvider(ctrl *gomock.Controller) *MockCloudStorageProvider {
	mock := &MockCloudStorageProvider{ctrl: ctrl}
	mock.recorder = &MockCloudStorageProviderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockCloudStorageProvider) EXPECT() *MockCloudStorageProviderMockRecorder {
	return m.recorder
}

// ConfigureHistoryServerStorage mocks base method
func (m *MockCloudStorageProvider) ConfigureHistoryServerStorage() (*cloudstorage.StorageInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ConfigureHistoryServerStorage")
	ret0, _ := ret[0].(*cloudstorage.StorageInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ConfigureHistoryServerStorage indicates an expected call of ConfigureHistoryServerStorage
func (mr *MockCloudStorageProviderMockRecorder) ConfigureHistoryServerStorage() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ConfigureHistoryServerStorage", reflect.TypeOf((*MockCloudStorageProvider)(nil).ConfigureHistoryServerStorage))
}

// GetStorageInfo mocks base method
func (m *MockCloudStorageProvider) GetStorageInfo() (*cloudstorage.StorageInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetStorageInfo")
	ret0, _ := ret[0].(*cloudstorage.StorageInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetStorageInfo indicates an expected call of GetStorageInfo
func (mr *MockCloudStorageProviderMockRecorder) GetStorageInfo() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStorageInfo", reflect.TypeOf((*MockCloudStorageProvider)(nil).GetStorageInfo))
}
