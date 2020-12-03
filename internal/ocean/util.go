package ocean

import (
	"context"
	"fmt"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

const (
	SpotinstClusterIdentifier = "spotinst.cluster-identifier"
	SpotinstOceanConfigmap    = "spotinst-kubernetes-cluster-controller-config"
)

func GetClusterIdentifier() (string, error) {
	conf, err := config.GetConfig()
	if err != nil {
		return "", fmt.Errorf("cannot get cluster configuration, %w", err)
	}

	ctx := context.TODO()
	kc, err := kubernetes.NewForConfig(conf)
	if err != nil {
		return "", fmt.Errorf("cannot connect to cluster, %w", err)
	}
	cm, err := kc.CoreV1().ConfigMaps(v1.NamespaceSystem).Get(ctx, SpotinstOceanConfigmap, v1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("error in ocean configuration, %w", err)
	}

	id := cm.Data[SpotinstClusterIdentifier]
	if id == "" {
		return "", fmt.Errorf("cluster identifier not found")
	}
	return id, nil
}
