package tide

import (
	"context"
	"embed"
	"fmt"
	"path"
	"reflect"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/spotinst/wave-operator/api/v1alpha1"
	"github.com/spotinst/wave-operator/catalog"
	"github.com/spotinst/wave-operator/install"
	tideconfig "github.com/spotinst/wave-operator/tide/config"
	goyaml "gopkg.in/yaml.v3"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrlrt "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

const (
	WaveOperatorChart      = "wave-operator"
	WaveOperatorRepository = "https://charts.spot.io"
	WaveOperatorVersion    = "" // empty string indicates latest chart version
	WaveOperatorValues     = ""

	CertManagerChart      = "cert-manager"
	CertManagerRepository = "https://charts.jetstack.io"
	CertManagerVersion    = "v1.1.0"
	CertManagerValues     = "installCRDs: true"

	spotConfigMapNamespace        = metav1.NamespaceSystem
	spotConfigMapName             = "spotinst-kubernetes-cluster-controller-config"
	clusterIdentifierConfigMapKey = "spotinst.cluster-identifier"

	ConfigIsOceanClusterProvisioned = "isOceanClusterProvisioned"
	ConfigIsK8sProvisioned          = "isK8sProvisioned"
	ConfigInitialWaveOperatorImage  = "initialWaveOperatorImage"

	AnnotationPrefix = "tide.wave.spot.io"
)

var (
	scheme = runtime.NewScheme()
)

//go:embed components/*
var components embed.FS
var componentDirName string = "components"

//go:embed crds/*
var crds embed.FS
var crdDirName = "crds"

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = apiextensions.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
}

type Manager interface {
	SetWaveInstallSpec(spec install.InstallSpec) error

	SetConfiguration(config map[string]interface{}) (*v1alpha1.WaveEnvironment, error)
	DeleteConfiguration(deleteEnvironmentCRD bool) error
	GetConfiguration() (*v1alpha1.WaveEnvironment, error)

	Create(env *v1alpha1.WaveEnvironment) error
	Delete() error

	CreateTideRBAC() error
	DeleteTideRBAC() error
}

type manager struct {
	spec              install.InstallSpec
	clusterIdentifier string
	log               logr.Logger
	kubeClientGetter  genericclioptions.RESTClientGetter
}

func NewManager(log logr.Logger) (Manager, error) {

	ctx := context.TODO()

	conf, err := config.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("cannot get cluster configuration, %w", err)
	}

	kc, err := kubernetes.NewForConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to cluster, %w", err)
	}

	cm, err := kc.CoreV1().ConfigMaps(spotConfigMapNamespace).Get(ctx, spotConfigMapName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("error in ocean configuration, %w", err)
	}

	clusterIdentifier := cm.Data[clusterIdentifierConfigMapKey]
	if clusterIdentifier == "" {
		return nil, fmt.Errorf("ocean configuration has no cluster identifier")
	}
	log.Info("Reading ocean configuration", "clusterIdentifier", clusterIdentifier)

	kubeConfig := genericclioptions.NewConfigFlags(false)
	kubeConfig.APIServer = &conf.Host
	kubeConfig.BearerToken = &conf.BearerToken
	kubeConfig.CAFile = &conf.CAFile
	ns := catalog.SystemNamespace
	kubeConfig.Namespace = &ns

	return &manager{
		clusterIdentifier: clusterIdentifier,
		log:               log,
		kubeClientGetter:  kubeConfig,
		spec: install.InstallSpec{
			Name:       WaveOperatorChart,
			Repository: WaveOperatorRepository,
			Version:    WaveOperatorVersion,
			Values:     WaveOperatorValues,
		},
	}, nil
}

func (m *manager) SetWaveInstallSpec(spec install.InstallSpec) error {
	if spec.Name != "" {
		m.spec.Name = spec.Name
	}
	if spec.Repository != "" {
		m.spec.Repository = spec.Repository
	}
	if spec.Version != "" {
		m.spec.Version = spec.Version
	}
	if spec.Values != "" {
		m.spec.Values = spec.Values
	}
	return nil
}

type validatedConfig struct {
	isOceanClusterProvisioned bool
	isK8sProvisioned          bool
	initialWaveOperatorImage  string
}

func validateConfig(input map[string]interface{}) (*validatedConfig, error) {
	config := &validatedConfig{}
	cp, ok := input[ConfigIsOceanClusterProvisioned].(bool)
	if !ok {
		return nil, fmt.Errorf("invalid configuration field for %s <<%v>>", ConfigIsOceanClusterProvisioned, input[ConfigIsOceanClusterProvisioned])
	}
	config.isOceanClusterProvisioned = cp

	k, ok := input[ConfigIsK8sProvisioned].(bool)
	if !ok {
		return nil, fmt.Errorf("invalid configuration field for %s <<%v>>", ConfigIsK8sProvisioned, input[ConfigIsK8sProvisioned])
	}
	config.isK8sProvisioned = k

	i, ok := input[ConfigInitialWaveOperatorImage].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration field for %s <<%v>>", ConfigInitialWaveOperatorImage, input[ConfigInitialWaveOperatorImage])
	}
	config.initialWaveOperatorImage = i

	return config, nil
}

func (m *manager) getKubernetesClient() (kubernetes.Interface, error) {
	conf, err := m.kubeClientGetter.ToRESTConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(conf)
}

func (m *manager) getControllerRuntimeClient() (ctrlrt.Client, error) {
	conf, err := m.kubeClientGetter.ToRESTConfig()
	if err != nil {
		return nil, err
	}

	opts := ctrlrt.Options{
		Scheme: scheme,
		Mapper: nil,
	}

	rc, err := ctrlrt.New(conf, opts)
	if err != nil {
		return nil, err
	}
	return rc, nil
}

func (m *manager) loadCRD(name string) (*apiextensions.CustomResourceDefinition, error) {

	crd := &apiextensions.CustomResourceDefinition{}
	data, err := crds.ReadFile(path.Join(crdDirName, name))
	if err != nil {
		return nil, fmt.Errorf("crd %s not readable, %w", name, err)
	}

	serializer := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	_, _, err = serializer.Decode(data, &schema.GroupVersionKind{
		Group:   "apiextensions.k8s.io",
		Version: runtime.APIVersionInternal,
		Kind:    "CustomResourceDefinition",
	}, crd)
	if err != nil {
		return nil, fmt.Errorf("cannot load crd, %w", err)
	}

	return crd, nil
}

func (m *manager) loadWaveComponents() ([]*v1alpha1.WaveComponent, error) {

	dd, err := components.ReadDir(componentDirName)
	if err != nil {
		return nil, fmt.Errorf("components in %s can't be listed, %w", componentDirName, err)
	}
	manifests := make([]string, 0, len(dd))
	for _, d := range dd {
		manifests = append(manifests, path.Join(componentDirName, d.Name()))
	}

	if len(manifests) == 0 {
		return nil, fmt.Errorf("No wave component manifests found")
	}
	waveComponents := make([]*v1alpha1.WaveComponent, 0, len(manifests))

	for _, mm := range manifests {
		comp := &v1alpha1.WaveComponent{}
		data, err := components.ReadFile(mm)
		if err != nil {
			return nil, fmt.Errorf("cannot read file %s, %w", mm, err)
		}
		serializer := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
		_, _, err = serializer.Decode(data, &schema.GroupVersionKind{
			Group:   "wave.spot.io",
			Version: "v1alpha1",
			Kind:    "WaveComponent",
		}, comp)
		if err != nil {
			return nil, fmt.Errorf("cannot load wave component %s, %w", mm, err)
		}
		waveComponents = append(waveComponents, comp)
	}
	return waveComponents, nil
}

func (m *manager) SetConfiguration(input map[string]interface{}) (*v1alpha1.WaveEnvironment, error) {
	ctx := context.TODO()

	m.log.Info("Configuring Wave")
	config, err := validateConfig(input)
	if err != nil {
		return nil, fmt.Errorf("invalid input, %w", err)
	}

	kc, err := m.getKubernetesClient()
	if err != nil {
		return nil, err
	}
	ns := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: catalog.SystemNamespace,
		},
	}
	_, err = kc.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return nil, err
	}

	certManagerExists, err := m.checkCertManagerPreinstallation()
	if err != nil {
		return nil, fmt.Errorf("can't determine state of certificate manager before installation, %w", err)
	}

	crd, err := m.loadCRD("/wave.spot.io_waveenvironments.yaml")
	if err != nil {
		return nil, err
	}
	ucrd := &unstructured.Unstructured{}
	gv := schema.GroupVersion{
		Group:   "apiextensions.k8s.io",
		Version: runtime.APIVersionInternal,
	}
	if err := scheme.Convert(crd, ucrd, gv); err != nil {
		return nil, fmt.Errorf("failed to convert, %w", err)
	}
	rc, err := m.getControllerRuntimeClient()
	if err != nil {
		return nil, err
	}

	err = rc.Create(ctx, crd, &ctrlrt.CreateOptions{})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("failed to create crd, %w", err)
	}

	env := &v1alpha1.WaveEnvironment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.clusterIdentifier,
			Namespace: catalog.SystemNamespace,
			Annotations: map[string]string{
				AnnotationPrefix + "/" + ConfigInitialWaveOperatorImage: config.initialWaveOperatorImage,
			},
		},
		Spec: v1alpha1.WaveEnvironmentSpec{
			EnvironmentNamespace:    catalog.SystemNamespace,
			OperatorVersion:         m.spec.Version,
			CertManagerDeployed:     !certManagerExists,
			K8sClusterProvisioned:   config.isK8sProvisioned,
			OceanClusterProvisioned: config.isOceanClusterProvisioned,
		},
	}

	uenv := &unstructured.Unstructured{}
	if err := scheme.Convert(env, uenv, nil); err != nil {
		return nil, err
	}

	err = rc.Create(ctx, uenv)
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			m.log.Info("WaveEnvironment CR already exists", "message", err.Error())
		} else {
			return nil, fmt.Errorf("failed to create wave environment cr, %w", err)
		}
	}

	return env, nil
}

func (m *manager) GetConfiguration() (*v1alpha1.WaveEnvironment, error) {
	client, err := m.getControllerRuntimeClient()
	if err != nil {
		return nil, err
	}
	env := &v1alpha1.WaveEnvironment{}
	ctx := context.TODO()
	key := ctrlrt.ObjectKey{Name: m.clusterIdentifier, Namespace: catalog.SystemNamespace}
	err = client.Get(ctx, key, env)
	if err != nil {
		return nil, err
	}
	return env, nil
}

func (m *manager) Create(env *v1alpha1.WaveEnvironment) error {
	ctx := context.TODO()

	m.log.Info("Installing Wave")

	waveComponents, err := m.loadWaveComponents()
	if err != nil {
		return err
	}

	if env.Spec.CertManagerDeployed {
		err = m.installCertManager(ctx)
		if err != nil {
			return err
		}
	}

	err = m.installWaveOperator(ctx, env.ObjectMeta.Annotations[AnnotationPrefix+"/"+ConfigInitialWaveOperatorImage])
	if err != nil {
		return err
	}

	rc, err := m.getControllerRuntimeClient()
	if err != nil {
		return fmt.Errorf("kubernetes config error, %w", err)
	}

	for _, wc := range waveComponents {
		m.log.Info("installing wave component", "name", wc.Name)
		wc.Namespace = catalog.SystemNamespace
		err = rc.Create(ctx, wc)
		if err != nil {
			if k8serrors.IsAlreadyExists(err) {
				m.log.Info("wave component already exists", "name", wc.Name)
			} else {
				return fmt.Errorf("cannot install component %s, %w", wc.Name, err)
			}
		}
	}

	return nil
}

func (m *manager) Delete() error {

	ctx := context.TODO()

	m.log.Info("Deleting Wave")

	rc, err := m.getControllerRuntimeClient()
	if err != nil {
		return fmt.Errorf("kubernetes config error, %w", err)
	}

	components := &v1alpha1.WaveComponentList{}
	err = rc.List(ctx, components)
	if err != nil {
		crdGone, ok := err.(*apimeta.NoKindMatchError)
		if ok {
			m.log.Info("WaveComponent CRD is not present", "message", crdGone.Error())
		} else {
			return err
		}
	} else {
		for _, wc := range components.Items {
			if err := rc.Delete(ctx, &wc); err != nil {
				m.log.Error(err, "could not delete wave component", wc.Name)
			}
		}
	}

	err = wait.Poll(5*time.Second, 300*time.Second, func() (bool, error) {
		for _, wc := range components.Items {
			obj := &v1alpha1.WaveComponent{}
			key := types.NamespacedName{
				Namespace: wc.Namespace,
				Name:      wc.Name,
			}
			// wait for IsNotFound on all wavecomponents
			err := rc.Get(ctx, key, obj)
			if err == nil {
				return false, nil
			} else if !k8serrors.IsNotFound(err) {
				return false, err
			}
		}
		return true, nil
	})
	if err != nil {
		return err
	}

	err = m.deleteWaveOperator(ctx)
	if err != nil {
		return err
	}

	env, err := m.GetConfiguration()
	if err != nil {
		crdGone, ok := err.(*apimeta.NoKindMatchError)
		if ok {
			m.log.Info("WaveEnvironment CRD is not present", "message", crdGone.Error())
		} else {
			if k8serrors.IsNotFound(err) {
				m.log.Info("WaveEnvironment CR not found", "message", err.Error())
			} else {
				return fmt.Errorf("unable to read wave environment, %w", err)
			}
		}
	} else {
		if env.Spec.CertManagerDeployed {
			err = m.deleteCertManager(ctx)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *manager) DeleteConfiguration(deleteEnvironmentCRD bool) error {

	m.log.Info("Deleting configuration", "deleteEnvironmentCRD", deleteEnvironmentCRD)

	ctx := context.TODO()

	crdPresent := true
	crPresent := true

	environment, err := m.GetConfiguration()
	if err != nil {
		crdGone, ok := err.(*apimeta.NoKindMatchError)
		if ok {
			m.log.Info("WaveEnvironment CRD is not present", "message", crdGone.Error())
			crdPresent = false
		} else {
			if k8serrors.IsNotFound(err) {
				m.log.Info("WaveEnvironment CR not found", "message", err.Error())
				crPresent = false
			} else {
				return fmt.Errorf("unable to read wave environment, %w", err)
			}
		}
	}

	if !crdPresent {
		return nil
	}

	rc, err := m.getControllerRuntimeClient()
	if err != nil {
		return fmt.Errorf("could not get controller runtime client, %w", err)
	}

	if crPresent {
		err = rc.Delete(ctx, environment)
		if err != nil {
			return fmt.Errorf("could not delete wave environment cr, %w", err)
		}
	}

	if crdPresent && deleteEnvironmentCRD {
		crd, err := m.loadCRD("/wave.spot.io_waveenvironments.yaml")
		if err != nil {
			return fmt.Errorf("could not load crd, %w", err)
		}

		err = rc.Delete(ctx, crd)
		if err != nil {
			return fmt.Errorf("could not delete crd, %w", err)
		}
	}

	return nil
}

func (m *manager) installCertManager(ctx context.Context) error {
	kc, err := m.getKubernetesClient()
	if err != nil {
		return err
	}
	certNS := CertManagerChart // chart name == namespace
	_, _ = kc.CoreV1().Namespaces().Create(
		ctx,
		&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: certNS}},
		metav1.CreateOptions{},
	)
	installer := install.GetHelm("", m.kubeClientGetter, m.log)
	installer.SetNamespace(certNS)
	err = installer.Install(CertManagerChart, CertManagerRepository, CertManagerVersion, CertManagerValues)
	if err != nil {
		return fmt.Errorf("cannot install cert manager, %w", err)
	}

	// webhook must have cert and endpoint before we can proceed
	// Exited with error: cannot install wave operator, installation error, Internal error occurred: failed calling webhook "webhook.cert-manager.io": Post https://cert-manager-webhook.cert-manager.svc:443/mutate?timeout=10s: no endpoints available for service "cert-manager-webhook"

	err = wait.Poll(5*time.Second, 300*time.Second, func() (bool, error) {
		wh, err := kc.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, "cert-manager-webhook", metav1.GetOptions{})
		if err != nil || wh.Webhooks[0].ClientConfig.CABundle == nil {
			return false, nil
		}
		ep, err := kc.CoreV1().Endpoints(certNS).Get(ctx, "cert-manager-webhook", metav1.GetOptions{})
		if err != nil || len(ep.Subsets) == 0 || len(ep.Subsets[0].Addresses) == 0 {
			return false, nil
		}
		m.log.Info("polled", "webhook", "cert-manager-webhook", "name", wh.Webhooks[0].Name)

		return true, nil
	})
	return err
}

func (m *manager) installWaveOperator(ctx context.Context, waveOperatorImage string) error {
	kc, err := m.getKubernetesClient()
	if err != nil {
		return err
	}

	_, _ = kc.CoreV1().Namespaces().Create(
		ctx,
		&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: catalog.SystemNamespace}},
		metav1.CreateOptions{},
	)

	values, err := setImageInValues(m.spec.Values, waveOperatorImage)
	if err != nil {
		return fmt.Errorf("unable to set image %s, %w", waveOperatorImage, err)
	}

	installer := install.GetHelm("", m.kubeClientGetter, m.log)
	err = installer.Install(m.spec.Name, m.spec.Repository, m.spec.Version, values)
	if err != nil {
		return fmt.Errorf("cannot install wave operator, %w", err)
	}

	err = wait.Poll(5*time.Second, 300*time.Second, func() (bool, error) {
		dep, err := kc.AppsV1().Deployments(catalog.SystemNamespace).Get(ctx, "wave-operator", metav1.GetOptions{})
		if err != nil || dep.Status.AvailableReplicas == 0 {
			return false, nil
		}
		m.log.Info("polled", "deployment", "wave-operator", "replicas", dep.Status.AvailableReplicas)

		return true, nil
	})
	return err
}

func setImageInValues(valuesString string, image string) (string, error) {
	if image == "" {
		return valuesString, nil
	}
	// image:
	//   repository: public.ecr.aws/l8m2k1n1/netapp/wave-operator
	//   pullPolicy: IfNotPresent
	//   # Overrides the image tag whose default is the chart appVersion.
	//   tag: "0.2.0-a8e1a364"

	vals := map[string]interface{}{}
	err := goyaml.Unmarshal([]byte(valuesString), &vals)
	if err != nil {
		return "", err
	}
	spec := strings.Split(image, ":")
	if len(spec) > 2 {
		return "", fmt.Errorf("invalid image specification, %s", image)
	}
	tag := "latest"
	repo := spec[0]
	if len(spec) > 1 {
		tag = spec[1]
	}
	if repo == "" || tag == "" {
		return "", fmt.Errorf("bad image spec %s", image)
	}

	imageSpec := map[string]interface{}{}
	var ok bool
	i := vals["image"]
	if i != nil {
		imageSpec, ok = i.(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("invalid yaml image spec, %s, %v", reflect.TypeOf(i), i)
		}
	}
	imageSpec["repository"] = repo
	imageSpec["tag"] = tag
	imageSpec["pullPolicy"] = "IfNotPresent"

	vals["image"] = imageSpec

	b, err := goyaml.Marshal(vals)
	return string(b), err
}

func (m *manager) deleteWaveOperator(ctx context.Context) error {
	kc, err := m.getKubernetesClient()
	if err != nil {
		return err
	}

	installer := install.GetHelm("", m.kubeClientGetter, m.log)
	err = installer.Delete(m.spec.Name, m.spec.Repository, m.spec.Version, "")
	if err != nil {
		return fmt.Errorf("cannot delete wave operator, %w", err)
	}

	err = wait.Poll(5*time.Second, 300*time.Second, func() (bool, error) {
		_, err := kc.AppsV1().Deployments(catalog.SystemNamespace).Get(ctx, "spotctl-wave-operator", metav1.GetOptions{})
		if err == nil {
			return false, nil
		} else if !k8serrors.IsNotFound(err) {
			return false, err
		}
		return true, nil
	})
	return err
}

func (m *manager) deleteCertManager(ctx context.Context) error {
	kc, err := m.getKubernetesClient()
	if err != nil {
		return err
	}
	certNS := CertManagerChart // chart name == namespace

	installer := install.GetHelm("", m.kubeClientGetter, m.log)
	installer.SetNamespace(certNS)
	err = installer.Delete(CertManagerChart, CertManagerRepository, CertManagerVersion, CertManagerValues)
	if err != nil {
		return fmt.Errorf("cannot delete wave operator, %w", err)
	}

	err = wait.Poll(5*time.Second, 300*time.Second, func() (bool, error) {
		_, err := kc.AppsV1().Deployments(certNS).Get(ctx, "cert-manager", metav1.GetOptions{})
		if err == nil {
			return false, nil
		} else if !k8serrors.IsNotFound(err) {
			return false, err
		}
		return true, nil
	})
	return err
}

func (m *manager) CreateTideRBAC() error {

	ctx := context.TODO()
	namespace := catalog.SystemNamespace

	kubeClient, err := m.getKubernetesClient()
	if err != nil {
		return fmt.Errorf("could not create kubernetes client, %w", err)
	}

	sa, crb, err := loadTideRBAC(namespace)
	if err != nil {
		return fmt.Errorf("could not load tide RBAC objects, %w", err)
	}

	m.log.Info("Creating tide RBAC objects")

	_, err = kubeClient.CoreV1().ServiceAccounts(namespace).Create(ctx, sa, metav1.CreateOptions{})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return fmt.Errorf("could not create tide service account, %w", err)
	}

	_, err = kubeClient.RbacV1().ClusterRoleBindings().Create(ctx, crb, metav1.CreateOptions{})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return fmt.Errorf("could not create tide cluster role binding, %w", err)
	}

	return nil
}

func (m *manager) DeleteTideRBAC() error {

	ctx := context.TODO()
	namespace := catalog.SystemNamespace

	kubeClient, err := m.getKubernetesClient()
	if err != nil {
		return fmt.Errorf("could not create kubernetes client, %w", err)
	}

	m.log.Info("Deleting tide RBAC objects")

	err = kubeClient.CoreV1().ServiceAccounts(namespace).Delete(ctx, tideconfig.ServiceAccountName, metav1.DeleteOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("could not delete tide service account, %w", err)
	}

	err = kubeClient.RbacV1().ClusterRoleBindings().Delete(ctx, tideconfig.RoleBindingName, metav1.DeleteOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("could not delete tide cluster role binding, %w", err)
	}

	return nil
}

func loadTideRBAC(namespace string) (*v1.ServiceAccount, *rbacv1.ClusterRoleBinding, error) {

	manifests, err := tideconfig.GetRBACManifests(namespace)
	if err != nil {
		return nil, nil, fmt.Errorf("could not get manifests, %w", err)
	}

	sa := &v1.ServiceAccount{}
	err = yamlutil.NewYAMLOrJSONDecoder(strings.NewReader(manifests.ServiceAccount), len(manifests.ServiceAccount)).Decode(sa)
	if err != nil {
		return nil, nil, fmt.Errorf("could not decode service account yaml, %w", err)
	}

	crb := &rbacv1.ClusterRoleBinding{}
	err = yamlutil.NewYAMLOrJSONDecoder(strings.NewReader(manifests.ClusterRoleBinding), len(manifests.ClusterRoleBinding)).Decode(crb)
	if err != nil {
		return nil, nil, fmt.Errorf("could not decode cluster role binding yaml, %w", err)
	}

	return sa, crb, nil
}
