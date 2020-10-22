package install

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/go-logr/logr"
	"gopkg.in/yaml.v3"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	_ "helm.sh/helm/v3/pkg/downloader"
	_ "helm.sh/helm/v3/pkg/getter"
	"helm.sh/helm/v3/pkg/release"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	ctrl "sigs.k8s.io/controller-runtime"
)

type Installer interface {

	// Install applies a helm chart to a cluster.  It's a lightweight wrapper around the helm 3 code
	Install(name string, repository string, version string, values string) error

	// Upgrade applies a helm chart to a cluster.  It's a lightweight wrapper around the helm 3 code
	Upgrade(rlsName string, chart *chart.Chart, vals map[string]string, namespace string, opts ...bool) (*release.Release, error)
}

type HelmInstaller struct {
	Log logr.Logger
}

func (i *HelmInstaller) logForHelm(format string, v ...interface{}) {
	i.Log.Info(fmt.Sprintf(format, v...))
}

// https://stackoverflow.com/questions/59782217/run-helm3-client-from-in-cluster
func (i *HelmInstaller) getActionConfig(namespace string) (*action.Configuration, error) {
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
	if err := actionConfig.Init(kubeConfig, namespace, "secret", i.logForHelm); err != nil {
		return nil, err
	}
	return actionConfig, nil
}

func (i *HelmInstaller) Install(name string, repository string, version string, values string) error {

	var vals map[string]interface{}
	err := yaml.Unmarshal([]byte(values), &vals)
	if err != nil {
		return fmt.Errorf("invalid values configuration, %w", err)
	}

	for k,  v := range(vals){
		i.Log.Info("values", k, v)
	}

	cfg, err := i.getActionConfig("default")
	if err != nil {
		return fmt.Errorf("failed to get action config, %w", err)
	}

	repo := fmt.Sprintf("%s/%s-%s.tgz", repository, name, version)
	act := action.NewInstall(cfg)
	act.ReleaseName = "wave-" + name

	act.Namespace = "spot-system"

	settings := &cli.EnvSettings{}
	cache, err := ioutil.TempDir(os.TempDir(), "wavecache-")
	if err != nil {
		return fmt.Errorf("unable to create cache directory, %s", err)
	}
	defer os.RemoveAll(cache)
	settings.RepositoryCache = os.TempDir()

	cp, err := act.ChartPathOptions.LocateChart(repo, settings)
	if err != nil {
		return fmt.Errorf("failed to locate chart %s, %w", repo, err)
	}

	chrt, err := loader.Load(cp)
	if err != nil {
		return fmt.Errorf("failed to load chart %s, %w", cp, err)

	}

	rel, err := act.Run(chrt, vals)
	if err != nil {
		return fmt.Errorf("installation error, %w", err)

	}
	i.Log.Info("installed", "release", rel.Name)
	return nil
}
