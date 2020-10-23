package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/spotinst/wave-operator/pkg/catalog"
	"github.com/spotinst/wave-operator/pkg/install"
	"helm.sh/helm/v3/pkg/action"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

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

	checkCatalog := true

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

}
