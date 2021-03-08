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
	"github.com/spotinst/wave-operator/tide"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "recede",
	Short: "Deletes the wave operator",
	Long: `Deletes the wave operator and all its dependencies and components

  Dependencies:
  - cert-manager

  Components:
  - spark-history-server
  - nginx-ingress
  - spark-operator
  - enterprise-gateway`,

	Run: delete,
}

var (
	deleteEnvironmentCRD bool
	deleteTideRBAC       bool
)

func init() {
	rootCmd.AddCommand(deleteCmd)

	deleteCmd.Flags().BoolVar(&deleteEnvironmentCRD, "delete-environment-crd", false, "should the Wave Environment CRD be deleted")
	deleteCmd.Flags().BoolVar(&deleteTideRBAC, "delete-tide-rbac", false, "should the Tide RBAC objects be deleted")
}

func delete(cmd *cobra.Command, args []string) {

	logger := zap.New(zap.UseDevMode(true))
	logger.Info("recede: removing wave")
	manager, err := tide.NewManager(logger)
	if err != nil {
		logger.Error(err, "creating manager failed")
		os.Exit(1)
	}

	err = manager.Delete()
	if err != nil {
		logger.Error(err, "deletion failed")
		os.Exit(1)
	}

	err = manager.DeleteConfiguration(deleteEnvironmentCRD)
	if err != nil {
		logger.Error(err, "could not delete wave configuration")
		os.Exit(1)
	}

	if deleteTideRBAC {
		err = manager.DeleteTideRBAC()
		if err != nil {
			// Best effort (deletion job deletes its own service account and cluster role binding)
			logger.Error(err, "could not delete tide rbac objects")
		}
	}

	logger.Info("Wave has been removed")
}
