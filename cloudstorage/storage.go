package cloudstorage

import (
	"time"
)

type CloudStorageProvider interface {
	ConfigureHistoryServerStorage() (*StorageInfo, error)
	GetStorageInfo() (*StorageInfo, error)
}

// General information about the cloud storage, which for AWS is an S3 bucket
type StorageInfo struct {
	// name of the storage bucket
	Name string

	// cloud provider region
	Region string

	// path used for access. For the history server and AWS, this path will have the prefix "s3a://"
	// other users may prefer to construct a path from the name and region
	Path string

	// time that the storage was created
	Created time.Time
}
