package controllers

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/golang/mock/gomock"
	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/catalog"
	"github.com/spotinst/wave-operator/cloudstorage"
	"github.com/spotinst/wave-operator/controllers/internal/mock_cloudstorage"
	"github.com/spotinst/wave-operator/controllers/internal/mock_install"
	"github.com/spotinst/wave-operator/install"
	"github.com/spotinst/wave-operator/internal/version"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	_ "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrlrt "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlrt_fake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	testScheme = runtime.NewScheme()

	emptyConfig = &rest.Config{
		Host:            "why are you parsing this",
		APIPath:         "",
		ContentConfig:   rest.ContentConfig{},
		Username:        "",
		Password:        "",
		BearerToken:     "",
		BearerTokenFile: "",
		Impersonate:     rest.ImpersonationConfig{},
		TLSClientConfig: rest.TLSClientConfig{},
	}

	historyStorage = &cloudstorage.StorageInfo{
		Name:    "spark-history-test",
		Region:  "us-east-2",
		Path:    "s3://spark-history-test/",
		Created: time.Date(2020, 12, 21, 10, 02, 0, 0, time.UTC),
	}
)

func init() {
	_ = clientgoscheme.AddToScheme(testScheme)
	_ = v1alpha1.AddToScheme(testScheme)
	_ = apiextensions.AddToScheme(testScheme)

	version.BuildVersion = "v0.0.0-test"
	version.BuildDate = "1970-01-01T00:00:00Z"
}

func withPrefix(name string) string {
	return "wave-" + name
}

func getMinimalTestComponent(chartName v1alpha1.ChartName) (*v1alpha1.WaveComponent, types.NamespacedName) {
	return &v1alpha1.WaveComponent{
			TypeMeta: metav1.TypeMeta{
				Kind:       "WaveComponent",
				APIVersion: "wave.spot.io/v1alpha1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      string(chartName),
				Namespace: catalog.SystemNamespace,
			},
			Spec: v1alpha1.WaveComponentSpec{
				Type:    v1alpha1.HelmComponentType,
				Name:    chartName,
				State:   v1alpha1.PresentComponentState,
				Version: "1.4.0",
			},
		},
		types.NamespacedName{
			Namespace: catalog.SystemNamespace,
			Name:      string(chartName),
		}
}

// func getExpectedObjects(  chartName v1alpha1.ChartName) []runtime.Object {
// 	switch chartName {
// 	case v1alpha1.SparkHistoryChartName:
// 		return []runtime.Object{
func getSparkHistoryObjects(name string) (*appsv1.Deployment, *v1.ConfigMap) {
	return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: catalog.SystemNamespace,
			},
			Status: appsv1.DeploymentStatus{
				Replicas:            2,
				UpdatedReplicas:     2,
				ReadyReplicas:       2,
				AvailableReplicas:   2,
				UnavailableReplicas: 0,
			},
		}, &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: catalog.SystemNamespace,
			},
		}
}

// abbreviated form of CRD installed by the  helm chart
func getSparkAppCRD() client.Object {
	return &apiextensions.CustomResourceDefinition{
		TypeMeta: metav1.TypeMeta{
			Kind:       "CustomResourceDefinition",
			APIVersion: "apiextensions.k8s.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "sparkapplications.sparkoperator.k8s.io",
		},
		Spec: apiextensions.CustomResourceDefinitionSpec{
			Group: "sparkoperator.k8s.io",
			Names: apiextensions.CustomResourceDefinitionNames{
				Plural:   "sparkapplications",
				Singular: "sparkapplication",
				Kind:     "SparkApplication",
				ListKind: "SparkApplicationList",
			},
			Versions: []apiextensions.CustomResourceDefinitionVersion{
				{
					Name: "v1beta2",
				},
			},
		},
	}
}

func getConditionForType(ct v1alpha1.WaveComponentConditionType, status v1alpha1.WaveComponentStatus) *v1alpha1.WaveComponentCondition {
	for _, c := range status.Conditions {
		if c.Type == ct {
			return &c
		}
	}
	return nil
}

func TestBadComponentType(t *testing.T) {

	log := zap.New(zap.UseDevMode(true))
	logf.SetLogger(log)

	component, objectKey := getMinimalTestComponent(v1alpha1.SparkHistoryChartName)
	component.Spec.Type = "kustomize"

	// mock the installer
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mi := mock_install.NewMockInstaller(ctrl)
	mcs := mock_cloudstorage.NewMockCloudStorageProvider(ctrl)

	// getMockInstaller is an instance of type controller.InstallerGetter, and returns a mock
	var getMockInstaller InstallerGetter = func(name string, getter genericclioptions.RESTClientGetter, log logr.Logger) install.Installer {
		return mi
	}

	component.Spec.State = v1alpha1.AbsentComponentState
	controller := NewWaveComponentReconciler(
		ctrlrt_fake.NewFakeClientWithScheme(testScheme, component),
		emptyConfig,
		getMockInstaller,
		mcs,
		log,
		testScheme,
	)

	request := ctrlrt.Request{NamespacedName: objectKey}

	result, err := controller.Reconcile(context.TODO(), request)
	assert.NoError(t, err)
	assert.False(t, result.Requeue)

	updated := v1alpha1.WaveComponent{}
	err = controller.Client.Get(context.TODO(), objectKey, &updated)
	assert.NoError(t, err)
	assert.NotEmpty(t, updated.Status)
	c := getConditionForType(v1alpha1.WaveComponentFailure, updated.Status)
	assert.NotNil(t, c)
	assert.Equal(t, v1.ConditionTrue, c.Status)
	assert.Equal(t, UnsupportedTypeReason, c.Reason)
}

func TestInitialInstall(t *testing.T) {

	log := zap.New(zap.UseDevMode(true))
	logf.SetLogger(log)

	component, objectKey := getMinimalTestComponent(v1alpha1.SparkHistoryChartName)

	// mock the installer
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mi := mock_install.NewMockInstaller(ctrl)
	mi.EXPECT().GetReleaseName(string(v1alpha1.SparkHistoryChartName)).Return(withPrefix(string(v1alpha1.SparkHistoryChartName))).AnyTimes()
	// installation not present
	mi.EXPECT().Get(gomock.Any()).Return(nil, install.ErrReleaseNotFound).Times(2)
	// Install returns no error
	mi.EXPECT().Install(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
	mcs := mock_cloudstorage.NewMockCloudStorageProvider(ctrl)
	mcs.EXPECT().ConfigureHistoryServerStorage().Return(historyStorage, nil).Times(2)

	// getMockInstaller is an instance of type controller.InstallerGetter, and returns a mock
	var getMockInstaller InstallerGetter = func(name string, getter genericclioptions.RESTClientGetter, log logr.Logger) install.Installer {
		return mi
	}

	component.Spec.State = v1alpha1.AbsentComponentState
	controller := NewWaveComponentReconciler(
		ctrlrt_fake.NewFakeClientWithScheme(testScheme, component),
		emptyConfig,
		getMockInstaller,
		mcs,
		log,
		testScheme,
	)

	request := ctrlrt.Request{NamespacedName: objectKey}

	{ // with status = Absent
		result, err := controller.Reconcile(context.TODO(), request)
		assert.NoError(t, err)
		assert.False(t, result.Requeue)

		updated := v1alpha1.WaveComponent{}
		err = controller.Client.Get(context.TODO(), objectKey, &updated)
		assert.NoError(t, err)
		assert.NotEmpty(t, updated.Finalizers)
		assert.True(t, containsString(updated.Finalizers, OperatorFinalizerName))
		assert.NotEmpty(t, updated.Status)
		c := getConditionForType(v1alpha1.WaveComponentAvailable, updated.Status)
		assert.NotNil(t, c)
		assert.Equal(t, v1.ConditionFalse, c.Status)
		assert.Equal(t, UninstalledReason, c.Reason)
		assert.NotEmpty(t, updated.Annotations)
		assert.Equal(t, version.BuildVersion, updated.Annotations[AnnotationWaveVersion])
	}

	component.Spec.State = v1alpha1.PresentComponentState
	controller.Client = ctrlrt_fake.NewFakeClientWithScheme(testScheme, component)
	{ // with status = Present
		result, err := controller.Reconcile(context.TODO(), request)
		assert.NoError(t, err)
		assert.False(t, result.Requeue)

		updated := v1alpha1.WaveComponent{}
		err = controller.Client.Get(context.TODO(), objectKey, &updated)
		assert.NoError(t, err)
		assert.NotEmpty(t, updated.Status)
		c := getConditionForType(v1alpha1.WaveComponentProgressing, updated.Status)
		assert.NotNil(t, c)
		assert.Equal(t, v1.ConditionTrue, c.Status)
		assert.Equal(t, InstallingReason, c.Reason)

		assert.True(t, strings.Contains(updated.Spec.ValuesConfiguration, historyStorage.Path), "values should contain path "+historyStorage.Path)
	}
}

func TestInstallSparkHistory(t *testing.T) {

	log := zap.New(zap.UseDevMode(true))
	logf.SetLogger(log)

	component, objectKey := getMinimalTestComponent(v1alpha1.SparkHistoryChartName)
	deployed := install.Installation{
		Name:   component.Name,
		Status: install.Deployed,
	}

	subTestCount := 3

	// mock the installer
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mi := mock_install.NewMockInstaller(ctrl)
	mi.EXPECT().GetReleaseName(string(v1alpha1.SparkHistoryChartName)).Return(withPrefix(string(v1alpha1.SparkHistoryChartName))).AnyTimes()
	// installation present
	mi.EXPECT().Get(gomock.Any()).Return(&deployed, nil).Times(subTestCount)
	// Install is not called
	mi.EXPECT().IsUpgrade(gomock.Any(), gomock.Any()).Return(false).Times(subTestCount)
	var getMockInstaller InstallerGetter = func(name string, getter genericclioptions.RESTClientGetter, log logr.Logger) install.Installer {
		return mi
	}
	mcs := mock_cloudstorage.NewMockCloudStorageProvider(ctrl)
	mcs.EXPECT().ConfigureHistoryServerStorage().Return(historyStorage, nil).Times(subTestCount)

	fakeClient := ctrlrt_fake.NewFakeClientWithScheme(testScheme, component)
	controller := NewWaveComponentReconciler(
		fakeClient,
		emptyConfig,
		getMockInstaller,
		mcs,
		log,
		testScheme,
	)

	// Deployment is absent
	request := ctrlrt.Request{NamespacedName: objectKey}
	result, err := controller.Reconcile(context.TODO(), request)
	assert.NoError(t, err)
	assert.True(t, result.Requeue)

	updated := v1alpha1.WaveComponent{}
	err = controller.Client.Get(context.TODO(), objectKey, &updated)
	assert.NoError(t, err)
	assert.NotEmpty(t, updated.Status)
	c := getConditionForType(v1alpha1.WaveComponentAvailable, updated.Status)
	assert.NotNil(t, c)
	assert.Equal(t, v1.ConditionFalse, c.Status)
	assert.Equal(t, "DeploymentAbsent", c.Reason)

	// Deployment is not available
	dep, cm := getSparkHistoryObjects(mi.GetReleaseName(string(v1alpha1.SparkHistoryChartName)))
	dep.Status.AvailableReplicas = 0
	controller.Client = ctrlrt_fake.NewFakeClientWithScheme(testScheme, component, dep, cm)
	result, err = controller.Reconcile(context.TODO(), request)
	assert.NoError(t, err)
	assert.True(t, result.Requeue)

	updated = v1alpha1.WaveComponent{}
	err = controller.Client.Get(context.TODO(), objectKey, &updated)
	assert.NoError(t, err)
	assert.NotEmpty(t, updated.Status)
	c = getConditionForType(v1alpha1.WaveComponentAvailable, updated.Status)
	assert.NotNil(t, c)
	assert.Equal(t, v1.ConditionFalse, c.Status)
	assert.Equal(t, "PodsUnavailable", c.Reason)

	// Deployment is available
	dep, cm = getSparkHistoryObjects(mi.GetReleaseName(string(v1alpha1.SparkHistoryChartName)))
	controller.Client = ctrlrt_fake.NewFakeClientWithScheme(testScheme, component, dep, cm)
	result, err = controller.Reconcile(context.TODO(), request)
	assert.NoError(t, err)
	assert.False(t, result.Requeue)

	updated = v1alpha1.WaveComponent{}
	err = controller.Client.Get(context.TODO(), objectKey, &updated)
	assert.NoError(t, err)
	assert.NotEmpty(t, updated.Status)
	c = getConditionForType(v1alpha1.WaveComponentAvailable, updated.Status)
	assert.NotNil(t, c)
	assert.Equal(t, v1.ConditionTrue, c.Status)
	assert.Equal(t, "DeploymentAvailable", c.Reason)
	assert.NotEmpty(t, updated.Status.Properties)
	assert.Equal(t, "2.4.0", updated.Status.Properties["SparkVersion"])
}

// the runtime client only retrieves namespaces objects, so we can't use it to check the existence
// of the sparkoperator CRD, and the mock rest.Config is insufficient for mocking the CRD.
// TODO move this test to the suite_test where it can be mocked/faked properly
func x_doesnt_work_TestInstallSparkOperator(t *testing.T) {

	log := zap.New(zap.UseDevMode(true))
	logf.SetLogger(log)

	component, objectKey := getMinimalTestComponent(v1alpha1.SparkOperatorChartName)
	deployed := install.Installation{
		Name:   component.Name,
		Status: install.Deployed,
	}

	subTestCount := 4

	// mock the installer
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mi := mock_install.NewMockInstaller(ctrl)
	// installation present
	mi.EXPECT().Get(gomock.Any()).Return(&deployed, nil).Times(subTestCount)
	// Install is not called
	mi.EXPECT().IsUpgrade(gomock.Any(), gomock.Any()).Return(false).Times(subTestCount)
	var getMockInstaller InstallerGetter = func(name string, getter genericclioptions.RESTClientGetter, log logr.Logger) install.Installer {
		return mi
	}
	mcs := mock_cloudstorage.NewMockCloudStorageProvider(ctrl)
	mcs.EXPECT().ConfigureHistoryServerStorage().Return(historyStorage, nil).AnyTimes()

	fakeClient := ctrlrt_fake.NewFakeClientWithScheme(testScheme, component)
	controller := NewWaveComponentReconciler(
		fakeClient,
		emptyConfig,
		getMockInstaller,
		mcs,
		log,
		testScheme,
	)

	// CRD absent
	request := ctrlrt.Request{NamespacedName: objectKey}
	result, err := controller.Reconcile(context.TODO(), request)
	assert.NoError(t, err)
	assert.False(t, result.Requeue)

	updated := v1alpha1.WaveComponent{}
	err = controller.Client.Get(context.TODO(), objectKey, &updated)
	assert.NoError(t, err)
	assert.NotEmpty(t, updated.Status)
	c := getConditionForType(v1alpha1.WaveComponentAvailable, updated.Status)
	assert.NotNil(t, c)
	assert.Equal(t, v1.ConditionFalse, c.Status)
	assert.Equal(t, "CRDNotDefined", c.Reason)

	// Deployment is absent
	crd := getSparkAppCRD()
	request = ctrlrt.Request{NamespacedName: objectKey}
	controller.Client = ctrlrt_fake.NewFakeClientWithScheme(testScheme, component, crd)
	result, err = controller.Reconcile(context.TODO(), request)
	assert.NoError(t, err)
	assert.False(t, result.Requeue)

	updated = v1alpha1.WaveComponent{}
	err = controller.Client.Get(context.TODO(), objectKey, &updated)
	assert.NoError(t, err)
	assert.NotEmpty(t, updated.Status)
	c = getConditionForType(v1alpha1.WaveComponentAvailable, updated.Status)
	assert.NotNil(t, c)
	assert.Equal(t, v1.ConditionFalse, c.Status)
	assert.Equal(t, "DeploymentAbsent", c.Reason)

	// Deployment is not available
	dep, cm := getSparkHistoryObjects(mi.GetReleaseName(string(v1alpha1.SparkOperatorChartName)))
	dep.Status.AvailableReplicas = 0
	controller.Client = ctrlrt_fake.NewFakeClientWithScheme(testScheme, component, dep, cm, crd)
	result, err = controller.Reconcile(context.TODO(), request)
	assert.NoError(t, err)
	assert.False(t, result.Requeue)

	updated = v1alpha1.WaveComponent{}
	err = controller.Client.Get(context.TODO(), objectKey, &updated)
	assert.NoError(t, err)
	assert.NotEmpty(t, updated.Status)
	c = getConditionForType(v1alpha1.WaveComponentAvailable, updated.Status)
	assert.NotNil(t, c)
	assert.Equal(t, v1.ConditionFalse, c.Status)
	assert.Equal(t, "PodsUnavailable", c.Reason)

	// Deployment is available
	dep, cm = getSparkHistoryObjects(mi.GetReleaseName(string(v1alpha1.SparkOperatorChartName)))
	controller.Client = ctrlrt_fake.NewFakeClientWithScheme(testScheme, component, dep, cm, crd)
	result, err = controller.Reconcile(context.TODO(), request)
	assert.NoError(t, err)
	assert.False(t, result.Requeue)

	updated = v1alpha1.WaveComponent{}
	err = controller.Client.Get(context.TODO(), objectKey, &updated)
	assert.NoError(t, err)
	assert.NotEmpty(t, updated.Status)
	c = getConditionForType(v1alpha1.WaveComponentAvailable, updated.Status)
	assert.NotNil(t, c)
	assert.Equal(t, v1.ConditionTrue, c.Status)
	assert.Equal(t, "DeploymentAvailable", c.Reason)
}

func TestReinstall(t *testing.T) {

	log := zap.New(zap.UseDevMode(true))
	logf.SetLogger(log)

	component, objectKey := getMinimalTestComponent(v1alpha1.SparkHistoryChartName)
	uninstalled := install.Installation{
		Name:   component.Name,
		Status: install.Uninstalled,
	}

	// mock the installer
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mi := mock_install.NewMockInstaller(ctrl)
	// installation not present
	mi.EXPECT().GetReleaseName(string(v1alpha1.SparkHistoryChartName)).Return(withPrefix(string(v1alpha1.SparkHistoryChartName))).AnyTimes()
	mi.EXPECT().Get(gomock.Any()).Return(&uninstalled, nil).AnyTimes()
	mi.EXPECT().IsUpgrade(gomock.Any(), gomock.Any()).Return(false).AnyTimes()
	mi.EXPECT().Install(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
	mcs := mock_cloudstorage.NewMockCloudStorageProvider(ctrl)
	mcs.EXPECT().ConfigureHistoryServerStorage().Return(historyStorage, nil).AnyTimes()

	// getMockInstaller is an instance of type controller.InstallerGetter, and returns a mock
	var getMockInstaller InstallerGetter = func(name string, getter genericclioptions.RESTClientGetter, log logr.Logger) install.Installer {
		return mi
	}

	component.Spec.State = v1alpha1.AbsentComponentState
	controller := NewWaveComponentReconciler(
		ctrlrt_fake.NewFakeClientWithScheme(testScheme, component),
		emptyConfig,
		getMockInstaller,
		mcs,
		log,
		testScheme,
	)

	request := ctrlrt.Request{NamespacedName: objectKey}

	component.Spec.State = v1alpha1.PresentComponentState
	controller.Client = ctrlrt_fake.NewFakeClientWithScheme(testScheme, component)
	{ // with status = Present
		result, err := controller.Reconcile(context.TODO(), request)
		assert.NoError(t, err)
		assert.False(t, result.Requeue)

		// expect two conditions to be set
		updated := v1alpha1.WaveComponent{}
		err = controller.Client.Get(context.TODO(), objectKey, &updated)
		assert.NoError(t, err)
		assert.NotEmpty(t, updated.Status)
		cp := getConditionForType(v1alpha1.WaveComponentProgressing, updated.Status)
		assert.NotNil(t, cp)
		assert.Equal(t, v1.ConditionTrue, cp.Status)
		assert.Equal(t, InstallingReason, cp.Reason)
		ca := getConditionForType(v1alpha1.WaveComponentAvailable, updated.Status)
		assert.NotNil(t, ca)
		assert.Equal(t, v1.ConditionFalse, ca.Status)
		assert.Equal(t, UninstalledReason, ca.Reason)
	}
}
