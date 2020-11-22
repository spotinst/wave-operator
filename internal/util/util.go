package util

import (
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
