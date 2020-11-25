package cloudstorage

import (
	"time"
)

type CloudStorageProvider interface {
	ConfigureHistoryServerStorage() (*StorageInfo, error)
	GetStorageInfo() (*StorageInfo, error)
}

type StorageType string

const (
	S3          StorageType = "S3"
	GoogleCloud StorageType = "GoogleCloud"
	AzureBlob   StorageType = "AzureBlob"
)

// General information about the cloud storage, which for AWS is an S3 bucket
type StorageInfo struct {
	// name of the storage bucket
	Name string

	// provider class, one of ["S3","GoogleCloud", "AzureBlob"]
	Provider StorageType

	// cloud provider region
	Region string

	// path used for access. For the history server, this path will have the prefix "s3a://"
	// other users may prefer to construct a path from the name and region
	Path string

	// time that the storage was created
	Created time.Time
}
