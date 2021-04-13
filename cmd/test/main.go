package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/spotinst/wave-operator/admission"
	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/catalog"
	"github.com/spotinst/wave-operator/install"
	"github.com/spotinst/wave-operator/internal/util"
	sparkoperator "github.com/spotinst/wave-operator/sparkoperator.k8s.io/v1beta2"
	"helm.sh/helm/v3/pkg/action"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {

	_ = clientgoscheme.AddToScheme(scheme)
	_ = apiextensions.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
	_ = sparkoperator.AddToScheme(scheme)
	// +kubebuilder:scaffold:scheme
}

// RESTClientGetter is an interface that the ConfigFlags describe to provide an easier way to mock for commands
// and eliminate the direct coupling to a struct type.  Users may wish to duplicate this type in their own packages
// as per the golang type overlapping.
// type RESTClientGetter interface {
// 	// ToRESTConfig returns restconfig
// 	ToRESTConfig() (*rest.Config, error)
// 	// ToDiscoveryClient returns discovery client
// 	ToDiscoveryClient() (discovery.CachedDiscoveryInterface, error)
// 	// ToRESTMapper returns a restmapper
// 	ToRESTMapper() (meta.RESTMapper, error)
// 	// ToRawKubeConfigLoader return kubeconfig loader as-is
// 	ToRawKubeConfigLoader() clientcmd.ClientConfig
// }

// https://stackoverflow.com/questions/59782217/run-helm3-client-from-in-cluster
func getActionConfig(namespace string) (*action.Configuration, error) {
	actionConfig := new(action.Configuration)
	var kubeConfig *genericclioptions.ConfigFlags
	// Create the rest config instance with ServiceAccount values loaded in them
	config := ctrl.GetConfigOrDie() //rest.InClusterConfig()

	// Create the ConfigFlags struct instance with initialized values from ServiceAccount
	kubeConfig = genericclioptions.NewConfigFlags(false)
	kubeConfig.APIServer = &config.Host
	kubeConfig.BearerToken = &config.BearerToken
	kubeConfig.CAFile = &config.CAFile
	kubeConfig.Namespace = &namespace
	if err := actionConfig.Init(kubeConfig, namespace, os.Getenv("HELM_DRIVER"), log.Printf); err != nil {
		return nil, err
	}
	return actionConfig, nil
}

func main() {

	logger := zap.New(zap.UseDevMode(true))

	runInstall := false
	checkCatalog := false
	checkCrd := false
	// s3Bucket := false
	webhook := true

	if runInstall {
		installer := install.HelmInstaller{
			Log: logger,
		}
		values := `
serviceAccount:
  create: true
`
		err := installer.Install("redis", "https://charts.bitnami.com/bitnami", "11.2.1", values)
		if err != nil {
			fmt.Println(err)
			panic(err)
		}
	}

	if checkCatalog {
		cat, err := catalog.NewCataloger()
		if err != nil {
			fmt.Println(err)
			panic(err)
		}

		comps, err := cat.List()
		if err != nil {
			fmt.Println(err)
			panic(err)
		}

		for _, c := range comps.Items {
			s, _ := json.MarshalIndent(c, "", "  ")
			fmt.Println(string(s))
		}
	}

	if checkCrd {
		config := ctrl.GetConfigOrDie()
		c, err := client.New(config, client.Options{Scheme: scheme})
		if err != nil {
			panic(err)
		}

		k, err := clientset.NewForConfig(config)
		if err != nil {
			panic(err)
		}

		sparkApp := &apiextensions.CustomResourceDefinition{
			TypeMeta: metav1.TypeMeta{
				Kind:       "CustomResourceDefinition",
				APIVersion: "apiextensions.k8s.io/v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "sparkapplications.sparkoperator.k8s.io",
			},
			Spec:   apiextensions.CustomResourceDefinitionSpec{},
			Status: apiextensions.CustomResourceDefinitionStatus{},
		}
		err = c.Get(context.TODO(), types.NamespacedName{Name: "sparkapplications.sparkoperator.k8s.io"}, sparkApp)
		if err != nil {
			fmt.Println("spark operator crd not found,", err.Error())
		}
		crd, err := k.ApiextensionsV1().CustomResourceDefinitions().Get(
			context.TODO(),
			"sparkapplications.sparkoperator.k8s.io",
			metav1.GetOptions{},
		)
		if err != nil {
			fmt.Println("spark operator crd not found,", err.Error())
		} else {
			fmt.Println(crd.Name)
		}

		waveComp := &apiextensions.CustomResourceDefinition{
			TypeMeta: metav1.TypeMeta{
				Kind:       "CustomResourceDefinition",
				APIVersion: "apiextensions.k8s.io/v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "wavecomponents.wave.spot.io",
			},
			Spec:   apiextensions.CustomResourceDefinitionSpec{},
			Status: apiextensions.CustomResourceDefinitionStatus{},
		}

		err = c.Get(context.TODO(), types.NamespacedName{Name: "wavecomponents.wave.spot.io"}, waveComp)
		if err != nil {
			fmt.Println("wavecomponents crd not found,", err.Error())
		}

		waveEnv := &apiextensions.CustomResourceDefinition{
			TypeMeta: metav1.TypeMeta{
				Kind:       "CustomResourceDefinition",
				APIVersion: "apiextensions.k8s.io/v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "waveenvironments.wave.spot.io",
			},
			Spec:   apiextensions.CustomResourceDefinitionSpec{},
			Status: apiextensions.CustomResourceDefinitionStatus{},
		}

		err = c.Get(context.TODO(), types.NamespacedName{Name: waveEnv.Name}, waveEnv)
		if err != nil {
			fmt.Println("waveenvironments crd not found,", err.Error())
		}
	}

	// if s3Bucket {
	// 	bucketName := "waht-bucket"
	// 	b, err := aws.CreateBucket(bucketName)
	// 	if err != nil {
	// 		fmt.Println("oops on bucket", err.Error())
	// 	}
	// 	contents, err := aws.GetAboutStorageText(b)
	// 	err = aws.WriteFile(bucketName, "about.txt", contents)
	// 	if err != nil {
	// 		fmt.Println("oops on file", err.Error())
	// 	}
	// }
	if webhook {
		ac := admission.NewAdmissionController(&util.FakeStorageProvider{}, logger)
		ctx := ctrl.SetupSignalHandler()
		ac.Start(ctx)
	}

}
