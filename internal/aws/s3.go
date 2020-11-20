package aws

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spotinst/wave-operator/cloudstorage"
)

func NewS3Provider(clusterName string) cloudstorage.CloudStorageProvider {
	return &s3Provider{clusterName}
}

type s3Provider struct {
	clusterName string
}

func (s s3Provider) ConfigureHistoryServerStorage() (*cloudstorage.StorageInfo, error) {
	name := "spark-history-" + s.clusterName
	bucket, err := CreateBucket(name)
	if err != nil {
		return nil, err
	}

	aboutText, err := GetAboutStorageText(bucket)
	if err != nil {
		return nil, err
	}

	err = WriteFile(name, "ABOUT.txt", aboutText)
	if err != nil {
		return nil, err
	}
	return bucket, nil

}

func CreateBucket(name string) (*cloudstorage.StorageInfo, error) {

	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, err
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
				Region:  *(sess.Config.Region),
				Path:    fmt.Sprintf("s3://%s/", name),
				Created: *(b.CreationDate),
			}, nil
		}
	}
	return nil, err
}

func WriteFile(bucketName, fileName, contents string) error {
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return err
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

func GetAboutStorageText(bucket *cloudstorage.StorageInfo) (string, error) {

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
