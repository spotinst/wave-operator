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

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "recede",
	Short: "deletes the wave operator",
	Long: `deletes the wave operator and all its dependencies and components

  Dependencies:
  - cert-manager

  Components:
  - spark-history-server
  - nginx-ingress
  - spark-operator
  - enterprise-gateway`,

	Run: delete,
}

func init() {
	rootCmd.AddCommand(deleteCmd)

	deleteCmd.Flags().BoolP("preserve-cert-manager", "p", false, "leaves cert-manager in place")
}

func delete(cmd *cobra.Command, args []string) {

	logger := zap.New(zap.UseDevMode(true))
	logger.Info("recede called")
	logger.Info("removing wave")
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
}
