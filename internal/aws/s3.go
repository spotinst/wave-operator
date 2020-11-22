package aws

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"text/template"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spotinst/wave-operator/cloudstorage"
)

func NewS3Provider(clusterName string) cloudstorage.CloudStorageProvider {
	return &s3Provider{
		clusterName: clusterName,
	}
}

type s3Provider struct {
	clusterName string
	storageInfo *cloudstorage.StorageInfo
}

func (s *s3Provider) ConfigureHistoryServerStorage() (*cloudstorage.StorageInfo, error) {
	name := "spark-history-" + s.clusterName
	bucket, err := createBucket(name)
	if err != nil {
		return nil, err
	}
	s.storageInfo = bucket

	aboutText, err := getAboutStorageText(bucket)
	if err != nil {
		return nil, err
	}

	err = writeFile(name, "ABOUT.txt", aboutText)
	if err != nil {
		return nil, err
	}
	return bucket, nil
}

func (s *s3Provider) GetStorageInfo() (*cloudstorage.StorageInfo, error) {
	return s.storageInfo, nil
}

func createBucket(name string) (*cloudstorage.StorageInfo, error) {

	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, err
	}
	region := *(sess.Config.Region)
	if region == "" {
		region, err = getRegionFromMetadata()
		if err != nil {
			return nil, err
		}
		sess.Config.Region = &region
	}

	svc := s3.New(sess)
	_, err = svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: &name,
	})
	if err != nil && (!strings.HasPrefix(err.Error(), "BucketAlreadyOwnedByYou")) {
		return nil, err
	}

	bl, err := svc.ListBuckets(&s3.ListBucketsInput{})
	for _, b := range bl.Buckets {
		if *(b.Name) == name {
			return &cloudstorage.StorageInfo{
				Name:    name,
				Region:  region,
				Path:    fmt.Sprintf("s3a://%s/", name),
				Created: *(b.CreationDate),
			}, nil
		}
	}
	return nil, err
}

func writeFile(bucketName, fileName, contents string) error {
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return err
	}

	region := *(sess.Config.Region)
	if region == "" {
		region, err = getRegionFromMetadata()
		if err != nil {
			return err
		}
		sess.Config.Region = &region
	}

	svc := s3.New(sess)
	input := &s3.PutObjectInput{
		Body:   strings.NewReader(contents),
		Bucket: &bucketName,
		Key:    &fileName,
	}
	_, err = svc.PutObjectWithContext(context.TODO(), input)
	return err
}

func getAboutStorageText(bucket *cloudstorage.StorageInfo) (string, error) {

	aboutText := `Bucket: {{.Name}}
Region: {{.Region}}
Path: {{.Path}}
Created: {{.Created}}
`
	t := template.New("about")

	var err error
	t, err = t.Parse(aboutText)
	if err != nil {
		return "", err
	}

	var output bytes.Buffer
	if err := t.Execute(&output, bucket); err != nil {
		return "", err
	}

	return output.String(), nil
}

func getRegionFromMetadata() (string, error) {
	// curl http://169.254.169.254/latest/dynamic/instance-identity/document
	r, err := http.Get("http://169.254.169.254/latest/dynamic/instance-identity/document")
	if err != nil {
		return "", err
	}

	metadata := &struct {
		Region string
	}{}

	err = json.NewDecoder(r.Body).Decode(metadata)
	if err != nil {
		return "", err
	}
	return metadata.Region, nil
}
