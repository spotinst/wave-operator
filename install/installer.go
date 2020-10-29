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
	ClientGetter genericclioptions.RESTClientGetter
	Log          logr.Logger
}

// helm libraries require a logger function of particular form, different from zap's Info()
func (i *HelmInstaller) logForHelm(format string, v ...interface{}) {
	i.Log.Info(fmt.Sprintf(format, v...))
}

func NewHelmInstaller(getter genericclioptions.RESTClientGetter, log logr.Logger) Installer {
	return &HelmInstaller{getter, log}
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

	if err := actionConfig.Init(i.ClientGetter, namespace, "secret", i.logForHelm); err != nil {
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

	upgradeAction := action.NewUpgrade(cfg)

	releaseName := GetReleaseName(chartName)
	upgradeAction.Namespace = catalog.SystemNamespace
	upgradeAction.ChartPathOptions.RepoURL = repository
	upgradeAction.ChartPathOptions.Version = version

	settings := &cli.EnvSettings{}
	cache, err := ioutil.TempDir(os.TempDir(), "wavecache-")
	if err != nil {
		return fmt.Errorf("unable to create cache directory, %s", err)
	}
	defer os.RemoveAll(cache)
	settings.RepositoryCache = os.TempDir()

	cp, err := upgradeAction.ChartPathOptions.LocateChart(chartName, settings)
	if err != nil {
		return fmt.Errorf("failed to locate chart %s, %w", chartName, err)
	}

	chrt, err := loader.Load(cp)
	if err != nil {
		return fmt.Errorf("failed to load chart %s, %w", cp, err)

	}

	rel, err := upgradeAction.Run(releaseName, chrt, vals)
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

	if err != nil && err != driver.ErrReleaseNotFound {
		return fmt.Errorf("existing release check failed, %w", err)
	} else if rel != nil {
		i.Log.Info("release already exists")
		return nil
	}

	//repo := fmt.Sprintf("%s/%s-%s.tgz", repository, chartName, version)
	installAction := action.NewInstall(cfg)

	installAction.ReleaseName = releaseName
	installAction.Namespace = catalog.SystemNamespace
	installAction.ChartPathOptions.RepoURL = repository
	installAction.ChartPathOptions.Version = version

	settings := &cli.EnvSettings{}
	cache, err := ioutil.TempDir(os.TempDir(), "wavecache-")
	if err != nil {
		return fmt.Errorf("unable to create cache directory, %s", err)
	}
	defer os.RemoveAll(cache)
	settings.RepositoryCache = os.TempDir()

	cp, err := installAction.ChartPathOptions.LocateChart(chartName, settings)
	if err != nil {
		return fmt.Errorf("failed to locate chart %s, %w", chartName, err)
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
	var newVals map[string]interface{}
	err := yaml.Unmarshal([]byte(comp.Spec.ValuesConfiguration), &newVals)
	if err != nil {
		return true // fail properly later
	}
	if newVals == nil {
		newVals = map[string]interface{}{}
	}
	currentVals := rel.Config
	if currentVals == nil {
		currentVals = map[string]interface{}{}
	}
	if !reflect.DeepEqual(newVals, currentVals) {
		i.Log.Info("upgrade required", "diff", cmp.Diff(newVals, currentVals))
		return true
	}
	return false
}
