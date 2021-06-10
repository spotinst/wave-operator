package util

import (
	"fmt"
	"time"

	"github.com/spotinst/wave-operator/cloudstorage"
	"github.com/spotinst/wave-operator/internal/config/instances"
)

var FakeStorage = &cloudstorage.StorageInfo{
	Name:    "fake",
	Region:  "nowhere",
	Path:    "s3://fake/",
	Created: time.Date(2020, 6, 20, 21, 43, 0, 0, time.UTC),
}

type FakeStorageProvider struct{}

func (f FakeStorageProvider) ConfigureHistoryServerStorage() (*cloudstorage.StorageInfo, error) {
	return FakeStorage, nil
}
func (f FakeStorageProvider) GetStorageInfo() (*cloudstorage.StorageInfo, error) {
	return FakeStorage, nil
}

type FailedStorageProvider struct{}

func (f FailedStorageProvider) ConfigureHistoryServerStorage() (*cloudstorage.StorageInfo, error) {
	return nil, fmt.Errorf("FailedStorageProvider fails")
}
func (f FailedStorageProvider) GetStorageInfo() (*cloudstorage.StorageInfo, error) {
	return nil, fmt.Errorf("FailedStorageProvider fails")
}

type NilStorageProvider struct{}

func (f NilStorageProvider) ConfigureHistoryServerStorage() (*cloudstorage.StorageInfo, error) {
	return nil, nil
}
func (f NilStorageProvider) GetStorageInfo() (*cloudstorage.StorageInfo, error) {
	return nil, nil
}

type FakeInstanceTypeManager struct{}

func (m FakeInstanceTypeManager) Start() error {
	return nil
}

func (m FakeInstanceTypeManager) GetAllowedInstanceTypes() instances.InstanceTypes {
	return nil
}
