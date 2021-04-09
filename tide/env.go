package tide

import (
	"context"

	ctrlrt "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/catalog"
)

type Environment interface {
	EnvironmentGetter
	EnvironmentSaver
}

type EnvironmentGetter interface {
	GetConfiguration() (*v1alpha1.WaveEnvironment, error)
}

type EnvironmentSaver interface {
	SaveConfiguration(env *v1alpha1.WaveEnvironment) error
}

type KubernetesEnvironment struct {
	client    ctrlrt.Client
	clusterId string
}

func NewKubernetesEnvironment(c ctrlrt.Client, clusterId string) *KubernetesEnvironment {
	return &KubernetesEnvironment{
		clusterId: clusterId,
		client:    c,
	}
}

func (k *KubernetesEnvironment) GetConfiguration() (*v1alpha1.WaveEnvironment, error) {
	env := &v1alpha1.WaveEnvironment{}
	ctx := context.TODO()
	key := ctrlrt.ObjectKey{Name: k.clusterId, Namespace: catalog.SystemNamespace}
	err := k.client.Get(ctx, key, env)
	if err != nil {
		return nil, err
	}
	return env, nil
}

func (k *KubernetesEnvironment) SaveConfiguration(env *v1alpha1.WaveEnvironment) error {
	ctx := context.TODO()
	err := k.client.Update(ctx, env)
	if err != nil {
		return err
	}
	return nil
}
