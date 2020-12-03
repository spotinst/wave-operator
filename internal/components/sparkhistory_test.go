package components

import (
	"strings"
	"testing"
	"time"

	"github.com/spotinst/wave-operator/cloudstorage"
	"github.com/stretchr/testify/assert"
)

func TestSparkHistoryConfiguration(t *testing.T) {
	b := &cloudstorage.StorageInfo{
		Name:    "spark-history-myclustername",
		Region:  "us-west-2",
		Path:    "s3://spark-history-myclustername/",
		Created: time.Now(),
	}
	oldvalues := `
    nfs:
      enableExampleNFS: false
    pvc:
      enablePVC: false
    s3:
      enableS3: true
      enableIAM: true
      logDirectory: s3a://spark-hs-natef/`
	newbytes, err := configureS3BucketValues(b, oldvalues)
	assert.NoError(t, err)
	newvalues := string(newbytes)
	assert.True(t, strings.Contains(newvalues, "logDirectory: s3://spark-history-myclustername/"))
}
