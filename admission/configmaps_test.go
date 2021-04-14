package admission

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/spotinst/wave-operator/cloudstorage"
	"github.com/spotinst/wave-operator/controllers"
	"github.com/spotinst/wave-operator/internal/util"
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

	nonSparkConfigMap = &corev1.ConfigMap{
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

	badSparkConfigMap = &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			UID:       "96c72b79-506d-4ab3-ae9b-11f1a415e069",
			Name:      "with-bad-data-driver-conf-map",
			Namespace: "spark-jobs",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Controller: &T,
					Kind:       "Pod",
					Name:       "spark-pi-512-driver",
					UID:        "46671815-0caf-4ca7-9bc1-e584c030698d",
				},
			},
		},
		Data: map[string]string{"spark.properties": "key\\u1 = value"},
	}

	sparkConfigMap = &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			UID:       "a2d45608-4ece-4871-8e5d-3c5181dd6705",
			Name:      "spark-pi-512-bfc89a76000e403b-driver-conf-map",
			Namespace: "spark-jobs",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Controller: &T,
					Kind:       "Pod",
					Name:       "spark-pi-512-driver",
					UID:        "46671815-0caf-4ca7-9bc1-e584c030698d",
				},
			},
		},
		Data: map[string]string{"spark.properties": "spark.driver.blockManager.port=7079\nspark.jars=local:///opt/spark/examples/jars/spark-examples_2.12-3.0.0.jar\nspark.kubernetes.executor.label.sparkoperator.k8s.io/submission-id=28c3f712-cf2a-405d-a49e-54eb318c3f4b\nspark.kubernetes.memoryOverheadFactor=0.1\nspark.kubernetes.driver.label.sparkoperator.k8s.io/app-name=spark-pi-512\nspark.kubernetes.driver.label.version=3.0.0\nspark.driver.memory=512m\nspark.app.name=spark-pi-512\nspark.driver.host=spark-pi-512-bfc89a76000e403b-driver-svc.spark-jobs.svc\nspark.kubernetes.executor.label.purpose=test-again\nspark.kubernetes.executor.label.version=3.0.0\nspark.kubernetes.driver.label.sparkoperator.k8s.io/launched-by-spark-operator=true\nspark.executor.instances=2\nspark.submit.pyFiles=\nspark.driver.port=7078\nspark.kubernetes.namespace=spark-jobs\nspark.master=k8s://https://10.100.0.1:443\nspark.executor.memory=512m\nspark.app.id=spark-d645eaf14e484364a48daf153b95f824\nspark.kubernetes.executor.label.sparkoperator.k8s.io/app-name=spark-pi-512\nspark.kubernetes.resource.type=java\nspark.kubernetes.submission.waitAppCompletion=false\nspark.kubernetes.authenticate.driver.serviceAccountName=spark\nspark.kubernetes.driver.pod.name=spark-pi-512-driver\nspark.driver.cores=1\nspark.executor.cores=1\nspark.kubernetes.driver.label.sparkoperator.k8s.io/submission-id=28c3f712-cf2a-405d-a49e-54eb318c3f4b\nspark.kubernetes.executor.label.sparkoperator.k8s.io/launched-by-spark-operator=true\nspark.kubernetes.container.image=public.ecr.aws/l8m2k1n1/netapp/spark\nspark.submit.deployMode=cluster\nspark.kubernetes.driver.label.purpose=test-again\nspark.kubernetes.driver.limit.cores=1200m\nspark.kubernetes.submitInDriver=true\nspark.kubernetes.container.image.pullPolicy=Always\n"},
	}
)

func getDriverPod(name string, namespace string, eventLogSyncEnabled bool) *corev1.Pod {
	pod := getSimplePod()
	pod.Name = name
	pod.Namespace = namespace
	pod.Labels = map[string]string{
		SparkRoleLabel: SparkRoleDriverValue,
	}
	if eventLogSyncEnabled {
		pod.Annotations[controllers.WaveConfigAnnotationSyncEventLogs] = "true"
	}
	return pod
}

// TODO when owner pod not found, when sync off, when sync on

func TestMutateEmptyCM(t *testing.T) {
	clientSet := k8sfake.NewSimpleClientset()
	req := getAdmissionRequest(t, emptyConfigMap)
	r, err := MutateConfigMap(clientSet, &util.FakeStorageProvider{}, log, req)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, emptyConfigMap.UID, r.UID)
	assert.Nil(t, r.PatchType)
	assert.Nil(t, r.Patch)
	assert.True(t, r.Allowed)
}

func TestMutateNonSparkCM(t *testing.T) {
	clientSet := k8sfake.NewSimpleClientset()
	req := getAdmissionRequest(t, nonSparkConfigMap)
	r, err := MutateConfigMap(clientSet, &util.FakeStorageProvider{}, log, req)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, nonSparkConfigMap.UID, r.UID)
	assert.Nil(t, r.PatchType)
	assert.Nil(t, r.Patch)
	assert.True(t, r.Allowed)
}

func TestMutateBadSparkCM(t *testing.T) {
	cm := badSparkConfigMap
	driver := getDriverPod(cm.OwnerReferences[0].Name, cm.Namespace, true)
	clientSet := k8sfake.NewSimpleClientset(driver)
	req := getAdmissionRequest(t, cm)
	r, err := MutateConfigMap(clientSet, &util.FakeStorageProvider{}, log, req)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, cm.UID, r.UID)
	assert.Nil(t, r.PatchType)
	assert.Nil(t, r.Patch)
	assert.True(t, r.Allowed)
}

func TestMutateSparkCM(t *testing.T) {
	cm := sparkConfigMap
	driver := getDriverPod(cm.OwnerReferences[0].Name, cm.Namespace, true)
	clientSet := k8sfake.NewSimpleClientset(driver)
	req := getAdmissionRequest(t, cm)
	r, err := MutateConfigMap(clientSet, &util.FakeStorageProvider{}, log, req)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, cm.UID, r.UID)
	assert.Equal(t, &jsonPathType, r.PatchType)
	assert.NotNil(t, r.Patch)
	assert.True(t, r.Allowed)
	matched := false
	matched, _ = regexp.MatchString(`spark.eventLog.dir ?= ?file:///var/log/spark`, string(r.Patch))
	assert.True(t, matched)
	matched, _ = regexp.MatchString(`spark.eventLog.enabled ?= ?true`, string(r.Patch))
	assert.True(t, matched)
}

func TestMutateSparkBadStorageCM(t *testing.T) {

	testFunc := func(provider cloudstorage.CloudStorageProvider) {
		cm := sparkConfigMap
		driver := getDriverPod(cm.OwnerReferences[0].Name, cm.Namespace, true)
		clientSet := k8sfake.NewSimpleClientset(driver)
		req := getAdmissionRequest(t, cm)
		r, err := MutateConfigMap(clientSet, provider, log, req)
		assert.NoError(t, err)
		assert.NotNil(t, r)
		assert.Equal(t, cm.UID, r.UID)
		assert.Nil(t, r.PatchType)
		assert.Nil(t, r.Patch)
		assert.True(t, r.Allowed)
	}

	t.Run("whenFailedStorageProvider", func(tt *testing.T) {
		testFunc(&util.FailedStorageProvider{})
	})

	t.Run("testNilStorageProvider", func(tt *testing.T) {
		testFunc(&util.NilStorageProvider{})
	})

}
