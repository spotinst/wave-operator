package controllers

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/golang/mock/gomock"
	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/catalog"
	"github.com/spotinst/wave-operator/controllers/internal/mock_install"
	"github.com/spotinst/wave-operator/install"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	_ "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrlrt "sigs.k8s.io/controller-runtime"
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
)

func init() {
	_ = clientgoscheme.AddToScheme(testScheme)
	_ = v1alpha1.AddToScheme(testScheme)
}

func TestInitialInstallProcess(t *testing.T) {

	log := zap.New(zap.UseDevMode(true))
	logf.SetLogger(log)

	// mock the installer
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock_install.NewMockInstaller(ctrl)
	// installation not present
	m.EXPECT().Get(gomock.Any()).Return(nil, install.ErrReleaseNotFound)
	// Install returns no error
	m.EXPECT().Install(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	var getMockInstaller InstallerGetter = func(getter genericclioptions.RESTClientGetter, log logr.Logger) install.Installer {
		return m
	}

	// add component to fake api client
	component := v1alpha1.WaveComponent{
		TypeMeta: metav1.TypeMeta{
			Kind:       "WaveComponent",
			APIVersion: "wave.spot.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(v1alpha1.SparkHistoryChartName),
			Namespace: catalog.SystemNamespace,
		},
		Spec: v1alpha1.WaveComponentSpec{
			Type: v1alpha1.HelmComponentType,
			Name: v1alpha1.SparkHistoryChartName,
		},
	}
	controller := NewWaveComponentReconciler(
		ctrlrt_fake.NewFakeClientWithScheme(testScheme, &component),
		emptyConfig,
		getMockInstaller,
		log,
		testScheme,
	)

	// test reconcile
	request := ctrlrt.Request{
		NamespacedName: types.NamespacedName{
			Namespace: catalog.SystemNamespace,
			Name:      string(v1alpha1.SparkHistoryChartName),
		},
	}
	_, err := controller.Reconcile(request)
	assert.NoError(t, err)
}
