/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

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
package cmd

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"
	"github.com/spotinst/wave-operator/api/v1alpha1"
	installpkg "github.com/spotinst/wave-operator/install"
	"github.com/spotinst/wave-operator/tide"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	chartNames = []string{
		string(v1alpha1.WaveIngressChartName),
		string(v1alpha1.EnterpriseGatewayChartName),
		string(v1alpha1.SparkHistoryChartName),
		string(v1alpha1.SparkOperatorChartName),
	}

	installCmd = &cobra.Command{
		Use:   "advance",
		Short: "Installs the wave operator",
		Long: `Installs the wave operator and all its dependencies, and configures wave components

  Dependencies:
    cert-manager

  Components:
    ` + strings.Join(chartNames, "\n    "),
		Run: install,
	}

	logger logr.Logger

	k8sClusterCreated   bool
	oceanCreated        bool
	certManagerDeployed bool

	waveChartName       string
	waveChartVersion    string
	waveChartURL        string
	waveChartValuesJSON string

	waveOperatorImage string
	waveComponents    components = map[v1alpha1.ChartName]bool{}
)

func init() {

	logger = zap.New(zap.UseDevMode(true))

	rootCmd.AddCommand(installCmd)

	installCmd.Flags().BoolVar(&k8sClusterCreated, "k8s-cluster-created", false, "indicates the cluster was created specifically for wave")
	installCmd.Flags().BoolVar(&oceanCreated, "ocean-created", false, "indicates that spot ocean was created for this wave installation")

	installCmd.Flags().StringVar(&waveChartName, "wave-chart-name", tide.WaveOperatorChart, "wave-operator chart name")
	installCmd.Flags().StringVar(&waveChartVersion, "wave-chart-version", tide.WaveOperatorVersion, "wave-operator chart version")
	installCmd.Flags().StringVar(&waveChartURL, "wave-chart-url", tide.WaveOperatorRepository, "wave-operator chart repository url")
	installCmd.Flags().StringVar(&waveChartValuesJSON, "wave-chart-values", tide.WaveOperatorValues, "wave-operator chart values (json)")

	installCmd.Flags().StringVar(&waveOperatorImage, "wave-image", "", "full container image specification for the wave operator")

	installCmd.Flags().Var(&waveComponents, "enable", fmt.Sprintf("enable or disable components, allows multiple arguments: <name>=true|false, for name in %s", chartNames))

}

type components map[v1alpha1.ChartName]bool

func (c components) String() string {
	s := make([]string, 0, len(c))
	for n, e := range c {
		s = append(s, fmt.Sprintf("%s=%t", n, e))
	}
	return strings.Join(s, ",")
}

func (c components) Type() string {
	return "WaveComponentSet"
}

func (c components) Set(arg string) error {
	v := strings.Split(arg, ",")
	for _, val := range v {
		input := strings.Split(val, "=")
		name := v1alpha1.ChartName(input[0])

		switch name {
		case
			v1alpha1.WaveIngressChartName,
			v1alpha1.EnterpriseGatewayChartName,
			v1alpha1.SparkHistoryChartName,
			v1alpha1.SparkOperatorChartName:
			enabled, err := strconv.ParseBool(input[1])
			if err != nil {
				return fmt.Errorf("cannot parse enabled flag for %s (%s), %w", name, val, err)
			}
			c[name] = enabled

		default:
			logger.Info("Unknown chart name input, ignoring", "name", name)
		}
	}
	return nil

}

func install(cmd *cobra.Command, args []string) {

	logger.Info("advance: installing wave")

	manager, err := tide.NewManager(logger)
	if err != nil {
		logger.Error(err, "create manager failed")
		os.Exit(1)
	}

	waveSpec := installpkg.InstallSpec{
		Name:       waveChartName,
		Repository: waveChartURL,
		Version:    waveChartVersion,
		Values:     waveChartValuesJSON,
		Enabled:    waveComponents,
	}
	err = manager.SetWaveInstallSpec(waveSpec)
	if err != nil {
		logger.Error(err, "wave chart specification is invalid")
		os.Exit(1)
	}

	waveConfig := map[string]interface{}{
		tide.ConfigIsK8sProvisioned:          k8sClusterCreated,
		tide.ConfigIsOceanClusterProvisioned: oceanCreated,
		tide.ConfigInitialWaveOperatorImage:  waveOperatorImage,
	}

	env, err := manager.SetConfiguration(waveConfig)
	if err != nil {
		logger.Error(err, "configuration failed")
		os.Exit(1)
	}

	err = manager.CreateTideRBAC()
	if err != nil {
		logger.Error(err, "could not create tide rbac objects")
		os.Exit(1)
	}

	err = manager.Create(*env)
	if err != nil {
		logger.Error(err, "creation failed")
		os.Exit(1)
	}

	logger.Info("Wave operator is now managing components")
}
