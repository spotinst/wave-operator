/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/spotinst/wave-operator/catalog"
	"github.com/spotinst/wave-operator/install"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/envtest/printer"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	v1alpha1 "github.com/spotinst/wave-operator/api/v1alpha1"
	// +kubebuilder:scaffold:imports
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

var cfg *rest.Config
var k8sClient client.Client
var testEnv *envtest.Environment

func TestAPIs(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecsWithDefaultAndCustomReporters(t,
		"Controller Suite",
		[]Reporter{printer.NewlineReporter{}})
}

var _ = BeforeSuite(func(done Done) {
	log := zap.New(zap.UseDevMode(true), zap.WriteTo(GinkgoWriter))
	logf.SetLogger(log)

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{filepath.Join("..", "config", "crd", "bases")},
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).ToNot(HaveOccurred())
	Expect(cfg).ToNot(BeNil())

	err = v1alpha1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	// +kubebuilder:scaffold:scheme

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).ToNot(HaveOccurred())
	Expect(k8sClient).ToNot(BeNil())
	k8sManager, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme.Scheme,
	})
	Expect(err).ToNot(HaveOccurred())

	componentList := &v1alpha1.WaveComponentList{}
	err = k8sClient.List(context.Background(), componentList)
	Expect(err).ToNot(HaveOccurred())
	Expect(componentList.Items).To(BeEmpty())

	controller := NewWaveComponentReconciler(
		k8sManager.GetClient(),
		k8sManager.GetConfig(),
		install.GetHelm,
		ctrl.Log.WithName("controllers").WithName("WaveComponent"),
		k8sManager.GetScheme(),
	)
	err = controller.SetupWithManager(k8sManager)
	Expect(err).ToNot(HaveOccurred())

	// there should be no preexisting helm release
	helm := controller.getInstaller(Wave, controller.getClient, log)
	rel, err := helm.Get(helm.GetReleaseName(string(v1alpha1.SparkHistoryChartName)))
	Expect(rel).To(BeNil())
	Expect(err).ToNot(BeNil())

	go func() {
		err = k8sManager.Start(ctrl.SetupSignalHandler())
		Expect(err).ToNot(HaveOccurred())
	}()

	close(done)
}, 60)

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	err := testEnv.Stop()
	Expect(err).ToNot(HaveOccurred())
})

var _ = Describe("WaveComponent controller", func() {

	// Define utility constants for object names and testing timeouts/durations and intervals.
	const (
		timeout  = time.Second * 10
		interval = time.Second
	)

	Context("When reconciling initial WaveComponent", func() {
		It("Should attempt a helm install", func() {
			By("creating a new WaveComponent")
			ctx := context.Background()
			waveComponent := &v1alpha1.WaveComponent{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "wave.spot.io/v1alpha1",
					Kind:       "WaveComponent",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      string(v1alpha1.SparkHistoryChartName),
					Namespace: catalog.SystemNamespace,
				},
				Spec: v1alpha1.WaveComponentSpec{
					Type:    v1alpha1.HelmComponentType,
					Name:    v1alpha1.SparkHistoryChartName,
					State:   v1alpha1.PresentComponentState,
					URL:     "https://kubernetes-charts.storage.googleapis.com",
					Version: "1.4.0",
					ValuesConfiguration: `
nfs:
  enableExampleNFS: false
pvc:
  enablePVC: false
s3:
  enableS3: true
  enableIAM: true
  logDirectory: s3a://spark-hs-natef,
`,
				},
			}
			systemNamespace := &v1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: catalog.SystemNamespace},
			}
			Expect(k8sClient.Create(ctx, systemNamespace)).Should(Succeed())
			Expect(k8sClient.Create(ctx, waveComponent)).Should(Succeed())

			lookupKey := types.NamespacedName{Name: string(v1alpha1.SparkHistoryChartName), Namespace: catalog.SystemNamespace}
			created := &v1alpha1.WaveComponent{}

			Eventually(func() bool {
				err := k8sClient.Get(ctx, lookupKey, created)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(created.Spec.Name).Should(Equal(v1alpha1.SparkHistoryChartName))
			Expect(created.Name).Should(Equal(string(v1alpha1.SparkHistoryChartName)))

			By("By checking that WaveComponent has changed status")
			Eventually(func() (int, error) {
				err := k8sClient.Get(ctx, lookupKey, created)
				if err != nil {
					return 0, err
				}

				return len(created.Status.Conditions), nil
			}, timeout, interval).Should(BeNumerically(">", 0))
			Expect(created.Status.Conditions[0].Type).To(Equal(v1alpha1.WaveComponentProgressing))
			Expect(created.Status.Conditions[0].Reason).To(Equal(InstallingReason))
		})
	})
})
