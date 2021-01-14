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
	"github.com/spotinst/wave-operator/internal/tide"
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
	k8sClusterCreated, oceanCreated, certManagerDeployed bool
)

func init() {
	rootCmd.AddCommand(installCmd)

	installCmd.Flags().BoolVar(&k8sClusterCreated, "k8s-cluster-created", false, "indicates the cluster was created specifically for wave")
	installCmd.Flags().BoolVar(&oceanCreated, "ocean-created", false, "indicates that spot ocean was created for this wave installation")
}

func install(cmd *cobra.Command, args []string) {

	logger := zap.New(zap.UseDevMode(true))

	logger.Info("advance: installing wave")

	manager, err := tide.NewManager(logger)
	if err != nil {
		logger.Error(err, "create manager failed")
		os.Exit(1)
	}
	env, err := manager.SetConfiguration(k8sClusterCreated, oceanCreated)
	if err != nil {
		logger.Error(err, "configuration failed")
		os.Exit(1)
	}
	err = manager.Create(env)
	if err != nil {
		logger.Error(err, "creation failed")
		os.Exit(1)
	}
	logger.Info("wave operator is now managing components")
}
