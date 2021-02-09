package components

import (
	"strings"
	"testing"
	"time"

	"github.com/spotinst/wave-operator/api/v1alpha1"
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

func TestExtractUserAndPassword(t *testing.T) {
	v1 := `
    ingress:
      enabled: true
      basicAuth:
        enabled: true
        secretName: spark-history-basic-auth
        username: spark
        password: history`

	c := &v1alpha1.WaveComponent{
		Spec: v1alpha1.WaveComponentSpec{
			ValuesConfiguration: v1,
		},
	}
	user, pass, err := getUserPasswordFrom(c)
	assert.NoError(t, err)
	assert.Equal(t, "spark", user)
	assert.Equal(t, "history", pass)

	v2 := `
    ingress:
      enabled: false
      basicAuth:
        enabled: true
        secretName: spark-history-basic-auth
        username: spark
        password: history`

	c = &v1alpha1.WaveComponent{
		Spec: v1alpha1.WaveComponentSpec{
			ValuesConfiguration: v2,
		},
	}
	user, pass, err = getUserPasswordFrom(c)
	assert.NoError(t, err)
	assert.Equal(t, "", user)
	assert.Equal(t, "", pass)

	v3 := `
    ingress:
      enabled: true
      basicAuth:
        enabled: false
        secretName: spark-history-basic-auth
        username: spark
        password: history`

	c = &v1alpha1.WaveComponent{
		Spec: v1alpha1.WaveComponentSpec{
			ValuesConfiguration: v3,
		},
	}
	user, pass, err = getUserPasswordFrom(c)
	assert.NoError(t, err)
	assert.Equal(t, "", user)
	assert.Equal(t, "", pass)

}
