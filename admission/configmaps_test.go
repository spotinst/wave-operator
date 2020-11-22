package admission

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/spotinst/wave-operator/internal/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/spotinst/wave-operator/cloudstorage"
	admissionv1 "k8s.io/api/admission/v1"
)

var (
	log         = zap.New(zap.UseDevMode(true))
	storageInfo = &cloudstorage.StorageInfo{
		Name:    "test",
		Region:  "utah",
		Path:    "s3://test/",
		Created: time.Now(),
	}
)

var (
	emptyConfigMap = &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			UID:  "acdb8db9-c789-4b2d-b8d2-c067a92a3c48",
			Name: "empty",
		},
	}
	nonsparkConfigMap = &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			UID:       "e2c495e4-9726-4e0c-a209-8d6fa389b3b9",
			Name:      "coredns",
			Namespace: "kube-system",
		},
		Data: map[string]string{"Corefile": ".:53 {\n    errors\n    health\n    kubernetes cluster.local in-addr.arpa ip6.arpa {\n      pods insecure\n      upstream\n      fallthrough in-addr.arpa ip6.arpa\n    }\n    prometheus :9153\n    forward . /etc/resolv.conf\n    cache 30\n    loop\n    reload\n    loadbalance\n}\n"},
	}
)

func getAdmissionRequest(t *testing.T, cm *corev1.ConfigMap) *admissionv1.AdmissionRequest {
	raw, err := json.Marshal(cm)
	require.NoError(t, err)
	return &admissionv1.AdmissionRequest{
		UID:    cm.UID,
		Object: runtime.RawExtension{Raw: raw},
	}
}

func TestMutateEmpty(t *testing.T) {
	req := getAdmissionRequest(t, emptyConfigMap)
	r, err := MutateConfigMap(&util.FakeStorageProvider{}, log, req)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, emptyConfigMap.UID, r.UID)
	assert.Nil(t, r.PatchType)
	assert.Nil(t, r.Patch)
	assert.True(t, r.Allowed)
}

func TestMutateNonspark(t *testing.T) {
	req := getAdmissionRequest(t, nonsparkConfigMap)
	r, err := MutateConfigMap(&util.FakeStorageProvider{}, log, req)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, nonsparkConfigMap.UID, r.UID)
	assert.Nil(t, r.PatchType)
	assert.Nil(t, r.Patch)
	assert.True(t, r.Allowed)
}
