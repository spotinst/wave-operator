package components

import (
	"strings"
	"testing"
	"time"

	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/cloudstorage"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
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
	newbytes, err := configureS3BucketValues(b, []byte(oldvalues))
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

	fullValues := `
    image:
      repository: public.ecr.aws/l8m2k1n1/netapp/spark-history-server
      tag: v3.0.1
    s3:
      enableS3: true
      enableIAM: true
      logDirectory: s3a://spark-history/
    gcs:
      enableGCS: false
    pvc:
      enablePVC: false
    wasbs:
      enableWASBS: false
    ingress:
      enabled: true
      annotations:
        kubernetes.io/ingress.class: "nginx"
        cert-manager.io/cluster-issuer: wave-issuer
        nginx.ingress.kubernetes.io/auth-type: basic
        nginx.ingress.kubernetes.io/auth-secret: spark-history-basic-auth
        nginx.ingress.kubernetes.io/auth-realm: 'Authentication Required - Spark History Server'
      path: /
      hosts:
      - ""
      tls:
      - secretName: spark-history-server-tls
        hosts:
        - spark-history-server.wave.spot.io
      basicAuth:
        enabled: true
        secretName: spark-history-basic-auth
        username: spark
        password: history`

	c = &v1alpha1.WaveComponent{
		Spec: v1alpha1.WaveComponentSpec{
			ValuesConfiguration: fullValues,
		},
	}
	user, pass, err = getUserPasswordFrom(c)
	assert.NoError(t, err)
	assert.Equal(t, "spark", user)
	assert.Equal(t, "history", pass)
}

func TestDontSetPassword(t *testing.T) {

	v1 := `
    ingress:
      enabled: true
      basicAuth:
        enabled: true
        secretName: spark-history-basic-auth
        username: spark
        password: history
      additionalField: 43`

	out, err := configureIngressLogin(nil, []byte(v1))
	assert.NoError(t, err)

	c := &v1alpha1.WaveComponent{
		Spec: v1alpha1.WaveComponentSpec{
			ValuesConfiguration: string(out),
		},
	}
	u, p, err := getUserPasswordFrom(c)
	assert.NoError(t, err)
	assert.Equal(t, "spark", u)
	assert.Equal(t, "history", p)

	var checkVals map[string]map[string]interface{}
	err = yaml.Unmarshal(out, &checkVals)
	assert.NoError(t, err)
	assert.Equal(t, 43, checkVals["ingress"]["additionalField"])

	// -----

	v2 := `
    ingress:
      enabled: true
      basicAuth:
        enabled: false
        secretName: spark-history-basic-auth
        username: spark
        password:
      additionalField: 43`

	out, err = configureIngressLogin(nil, []byte(v2))
	assert.NoError(t, err)

	c = &v1alpha1.WaveComponent{
		Spec: v1alpha1.WaveComponentSpec{
			ValuesConfiguration: string(out),
		},
	}
	u, p, err = getUserPasswordFrom(c)
	assert.NoError(t, err)
	assert.Equal(t, "spark", u)
	assert.Equal(t, "", p)

	err = yaml.Unmarshal(out, &checkVals)
	assert.NoError(t, err)
	assert.Equal(t, 43, checkVals["ingress"]["additionalField"])

}

func TestSetPassword(t *testing.T) {

	v1 := `
    ingress:
      enabled: true
      basicAuth:
        enabled: true
        secretName: spark-history-basic-auth
        username: spark
        password: 
      somethingMore: 43`

	out, err := configureIngressLogin(nil, []byte(v1))
	assert.NoError(t, err)

	c := &v1alpha1.WaveComponent{
		Spec: v1alpha1.WaveComponentSpec{
			ValuesConfiguration: string(out),
		},
	}
	u, p, err := getUserPasswordFrom(c)
	assert.NoError(t, err)
	assert.Equal(t, "spark", u)
	assert.NotEqual(t, "", p)
	// assert.Equal(t, "12345678", p)

	var checkVals map[string]map[string]interface{}
	err = yaml.Unmarshal(out, &checkVals)
	assert.NoError(t, err)
	assert.Equal(t, 43, checkVals["ingress"]["somethingMore"])

}

func TestDontFailOnSetPassword(t *testing.T) {

	var err error
	var out []byte

	v1 := ``
	out, err = configureIngressLogin(nil, []byte(v1))
	assert.NoError(t, err)

	v2 := `
    ingress: {}`
	out, err = configureIngressLogin(nil, []byte(v2))
	assert.NoError(t, err)
	assert.NotEmpty(t, out)

	v3 := `
    replicaCount: 1
    nameOverride: ""
    fullnameOverride: ""

    rbac:
      create: true

    serviceAccount:
      create: true
      name:`
	out, err = configureIngressLogin(nil, []byte(v3))
	assert.NoError(t, err)
	assert.NotEmpty(t, out)
}
