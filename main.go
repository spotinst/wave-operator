/*


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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/spotinst/wave-operator/admission"
	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/controllers"
	"github.com/spotinst/wave-operator/install"
	"github.com/spotinst/wave-operator/internal/aws"
	"github.com/spotinst/wave-operator/internal/config/instances"
	"github.com/spotinst/wave-operator/internal/logger"
	"github.com/spotinst/wave-operator/internal/ocean"
	"github.com/spotinst/wave-operator/internal/sparkapi"
	"github.com/spotinst/wave-operator/internal/spot/client"
	spotconfig "github.com/spotinst/wave-operator/internal/spot/client/config"
	"github.com/spotinst/wave-operator/internal/version"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = apiextensions.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
	// +kubebuilder:scaffold:scheme
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	flag.StringVar(&metricsAddr, "metrics-addr", "0.0.0.0:8080", "The address the metric endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "enable-leader-election", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.Parse()

	log := logger.New()
	ctrl.SetLogger(log)

	config := ctrl.GetConfigOrDie()
	mgr, err := ctrl.NewManager(config, ctrl.Options{
		Scheme:             scheme,
		MetricsBindAddress: metricsAddr,
		Port:               9443,
		LeaderElection:     enableLeaderElection,
		LeaderElectionID:   "9c5d2999.wave.spot.io",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		setupLog.Error(err, "unable to get client set")
		os.Exit(1)
	}

	oceanConfigMap, err := ocean.GetOceanConfigMap(context.TODO(), clientSet)
	if err != nil {
		setupLog.Error(err, "unable to get ocean config map")
		os.Exit(1)
	}

	clusterIdentifier, err := spotconfig.GetClusterIdentifier(oceanConfigMap, log)
	if err != nil || clusterIdentifier == "" {
		if err == nil {
			err = fmt.Errorf("cluster identifier missing")
		}
		setupLog.Error(err, "unable to get cluster identifier")
		os.Exit(1)
	}

	storageProvider := aws.NewS3Provider(clusterIdentifier)
	controller := controllers.NewWaveComponentReconciler(
		mgr.GetClient(),
		mgr.GetConfig(),
		install.GetHelm,
		storageProvider,
		ctrl.Log.WithName("controllers").WithName("WaveComponent"),
		mgr.GetScheme(),
	)
	if err = controller.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "WaveComponent")
		os.Exit(1)
	}

	sparkPodController := controllers.NewSparkPodReconciler(
		mgr.GetClient(),
		clientSet,
		sparkapi.GetManager,
		ctrl.Log.WithName("controllers").WithName("SparkPod"),
		mgr.GetScheme())

	if err = sparkPodController.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "SparkPod")
		os.Exit(1)
	}

	spotClient, err := client.NewClient(clientSet, log.WithName("spotClient"))
	if err != nil {
		setupLog.Error(err, "could not create spot client")
		os.Exit(1)
	}

	instanceTypeManager := instances.NewInstanceTypeManager(spotClient, clusterIdentifier, log.WithName("instanceTypeManager"))
	if err := instanceTypeManager.Start(); err != nil {
		setupLog.Error(err, "could not start instance type manager")
		os.Exit(1)
	}

	ac := admission.NewAdmissionController(clientSet, storageProvider, instanceTypeManager, log.WithName("admission"))
	err = mgr.Add(ac)
	if err != nil {
		setupLog.Error(err, "unable to add admission controller")
		os.Exit(1)
	}

	// +kubebuilder:scaffold:builder
	setupLog.Info("starting manager", "buildVersion", version.BuildVersion, "buildDate", version.BuildDate)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
