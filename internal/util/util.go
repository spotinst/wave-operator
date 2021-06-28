package util

import (
	"fmt"
	"time"

	"github.com/spotinst/wave-operator/cloudstorage"
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

func (m FakeInstanceTypeManager) Stop() {
	return
}

func (m FakeInstanceTypeManager) ValidateInstanceType(instanceType string) error {
	switch instanceType {
	case "m5.xlarge", "m5.2xlarge", "t2.micro":
		return nil
	default:
		return fmt.Errorf("invalid instance type %q", instanceType)
	}
}

func (m FakeInstanceTypeManager) GetValidInstanceTypesInFamily(family string) ([]string, error) {
	switch family {
	case "h1":
		return []string{"h1.small", "h1.medium", "h1.large"}, nil
	default:
		return nil, fmt.Errorf("invalid instance type family %q", family)
	}
}
