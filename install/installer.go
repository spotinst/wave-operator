package install

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/catalog"
	"gopkg.in/yaml.v3"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	_ "helm.sh/helm/v3/pkg/downloader"
	_ "helm.sh/helm/v3/pkg/getter"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/storage/driver"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	// ErrReleaseNotFound duplicates the helm error from the driver pacakge
	ErrReleaseNotFound = errors.New("release: not found")
)

type Installer interface {

	// Install applies a helm chart to a cluster.  It's a lightweight wrapper around the helm 3 code
	Install(name string, repository string, version string, values string) error

	// Get a release TODO refactor to remove helm references
	Get(name string) (*release.Release, error)

	// Upgrade applies a helm chart to a cluster.  It's a lightweight wrapper around the helm 3 code
	Upgrade(name string, repository string, version string, values string) error
	//Upgrade(rlsName string, chart *chart.Chart, vals map[string]string, namespace string, opts ...bool) (*release.Release, error)

	IsUpgrade(comp *v1alpha1.WaveComponent, rel *release.Release) bool
}

type HelmInstaller struct {
	Log logr.Logger
}

// helm libraries require a logger function of particular form, different from zap's Info()
func (i *HelmInstaller) logForHelm(format string, v ...interface{}) {
	i.Log.Info(fmt.Sprintf(format, v...))
}

func NewHelmInstaller(log logr.Logger) Installer {
	return &HelmInstaller{log}
}

func (i *HelmInstaller) Get(name string) (*release.Release, error) {
	cfg, err := i.getActionConfig(catalog.SystemNamespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get action config, %w", err)
	}
	act := action.NewGet(cfg)
	rel, err := act.Run(name)
	if err != nil && err == driver.ErrReleaseNotFound {
		return nil, ErrReleaseNotFound
	}
	return rel, err
}

// https://stackoverflow.com/questions/59782217/run-helm3-client-from-in-cluster
func (i *HelmInstaller) getActionConfig(namespace string) (*action.Configuration, error) {
	actionConfig := new(action.Configuration)
	var kubeConfig *genericclioptions.ConfigFlags
	// Create the rest config instance with ServiceAccount values loaded in them
	config, err := ctrl.GetConfig() //rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("can't load config, %w", err)
	}

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

func (i *HelmInstaller) Upgrade(chartName string, repository string, version string, values string) error {

	var vals map[string]interface{}
	err := yaml.Unmarshal([]byte(values), &vals)
	if err != nil {
		return fmt.Errorf("invalid values configuration, %w", err)
	}

	for k, v := range vals {
		i.Log.Info("values", k, v)
	}

	cfg, err := i.getActionConfig(catalog.SystemNamespace)
	if err != nil {
		return fmt.Errorf("failed to get action config, %w", err)
	}

	repo := fmt.Sprintf("%s/%s-%s.tgz", repository, chartName, version)

	act := action.NewUpgrade(cfg)

	releaseName := GetReleaseName(chartName)
	act.Namespace = catalog.SystemNamespace

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

	rel, err := act.Run(releaseName, chrt, vals)
	if err != nil {
		return fmt.Errorf("installation error, %w", err)

	}
	i.Log.Info("installed", "release", rel.Name)
	return nil
}

func (i *HelmInstaller) Install(chartName string, repository string, version string, values string) error {

	var vals map[string]interface{}
	err := yaml.Unmarshal([]byte(values), &vals)
	if err != nil {
		return fmt.Errorf("invalid values configuration, %w", err)
	}

	releaseName := GetReleaseName(chartName)

	cfg, err := i.getActionConfig(catalog.SystemNamespace)
	if err != nil {
		return fmt.Errorf("failed to get action config, %w", err)
	}

	getAction := action.NewGet(cfg)
	rel, err := getAction.Run(releaseName)
	// if err != nil {
	// 	i.Log.Info("getting release", "error", err)
	// } else if rel != nil {
	// 	i.Log.Info("getting release", "release", rel.Name)
	// }
	if err != nil && err != driver.ErrReleaseNotFound {
		return fmt.Errorf("existing release check failed, %w", err)
	} else if rel != nil {
		i.Log.Info("release already exists")
		return nil
	}

	repo := fmt.Sprintf("%s/%s-%s.tgz", repository, chartName, version)
	installAction := action.NewInstall(cfg)

	installAction.ReleaseName = releaseName
	installAction.Namespace = catalog.SystemNamespace

	settings := &cli.EnvSettings{}
	cache, err := ioutil.TempDir(os.TempDir(), "wavecache-")
	if err != nil {
		return fmt.Errorf("unable to create cache directory, %s", err)
	}
	defer os.RemoveAll(cache)
	settings.RepositoryCache = os.TempDir()

	cp, err := installAction.ChartPathOptions.LocateChart(repo, settings)
	if err != nil {
		return fmt.Errorf("failed to locate chart %s, %w", repo, err)
	}

	chrt, err := loader.Load(cp)
	if err != nil {
		return fmt.Errorf("failed to load chart %s, %w", cp, err)
	}

	rel, err = installAction.Run(chrt, vals)
	if err != nil {
		return fmt.Errorf("installation error, %w", err)
	}
	i.Log.Info("installed", "release", rel.Name)
	return nil
}

func GetReleaseName(name string) string {
	return "wave-" + name
}

func (i *HelmInstaller) IsUpgrade(comp *v1alpha1.WaveComponent, rel *release.Release) bool {
	if comp.Spec.Version != rel.Chart.Metadata.Version {
		return true
	}
	var vals map[string]interface{}
	err := yaml.Unmarshal([]byte(comp.Spec.ValuesConfiguration), &vals)
	if err != nil {
		return true // fail properly later
	}
	if vals == nil {
		vals = map[string]interface{}{}
	}
	if !reflect.DeepEqual(vals, rel.Config) {
		i.Log.Info("upgrade required", "diff", cmp.Diff(vals, rel.Config))
		return true
	}
	return false
}
