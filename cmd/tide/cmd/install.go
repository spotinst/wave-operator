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
	"os"

	"github.com/spf13/cobra"
	installpkg "github.com/spotinst/wave-operator/install"
	"github.com/spotinst/wave-operator/tide"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// installCmd represents the install command
var installCmd = &cobra.Command{
	Use:   "advance",
	Short: "installs the wave operator",
	Long: `installs the wave operator and all its dependencies, and configures wave components

  Dependencies:
  - cert-manager

  Components:
  - spark-history-server
  - nginx-ingress
  - spark-operator
  - enterprise-gateway`,

	Run: install,
}

var (
	k8sClusterCreated   bool
	oceanCreated        bool
	certManagerDeployed bool

	waveChartName       string
	waveChartVersion    string
	waveChartURL        string
	waveChartValuesJSON string

	waveOperatorImage string
)

func init() {
	rootCmd.AddCommand(installCmd)

	installCmd.Flags().BoolVar(&k8sClusterCreated, "k8s-cluster-created", false, "indicates the cluster was created specifically for wave")
	installCmd.Flags().BoolVar(&oceanCreated, "ocean-created", false, "indicates that spot ocean was created for this wave installation")

	installCmd.Flags().StringVar(&waveChartName, "wave-chart-name", tide.WaveOperatorChart, "wave-operator chart name")
	installCmd.Flags().StringVar(&waveChartVersion, "wave-chart-version", tide.WaveOperatorVersion, "wave-operator chart version")
	installCmd.Flags().StringVar(&waveChartURL, "wave-chart-url", tide.WaveOperatorRepository, "wave-operator chart repository url")
	installCmd.Flags().StringVar(&waveChartValuesJSON, "wave-chart-values", tide.WaveOperatorValues, "wave-operator chart values (json)")

	installCmd.Flags().StringVar(&waveOperatorImage, "wave-image", "", "full container image specification for the wave operator")
}

func install(cmd *cobra.Command, args []string) {

	logger := zap.New(zap.UseDevMode(true))

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

	err = manager.Create(env)
	if err != nil {
		logger.Error(err, "creation failed")
		os.Exit(1)
	}

	logger.Info("Wave operator is now managing components")
}
