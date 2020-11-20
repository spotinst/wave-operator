package cloudstorage

import (
	"time"
)

type CloudStorageProvider interface {
	ConfigureHistoryServerStorage() (*StorageInfo, error)
}

type StorageInfo struct {
	Name    string
	Region  string
	Path    string
	Created time.Time
}
