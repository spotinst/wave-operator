module github.com/spotinst/wave-operator

go 1.16

require (
	github.com/aws/aws-sdk-go v1.34.30
	github.com/evanphx/json-patch/v5 v5.1.0
	github.com/go-logr/logr v0.3.0
	github.com/go-logr/zapr v0.3.0 // indirect
	github.com/golang/mock v1.4.4
	github.com/google/go-cmp v0.5.2
	github.com/jetstack/cert-manager v1.1.0
	github.com/magiconair/properties v1.8.1
	github.com/mattbaird/jsonpatch v0.0.0-20200820163806-098863c1fc24
	github.com/mitchellh/go-homedir v1.1.0
	github.com/onsi/ginkgo v1.14.2
	github.com/onsi/gomega v1.10.2
	github.com/spf13/cobra v1.1.1
	github.com/spf13/viper v1.7.0
	github.com/stretchr/testify v1.6.1
	gopkg.in/yaml.v3 v3.0.0-20200615113413-eeeca48fe776
	helm.sh/helm/v3 v3.4.1
	k8s.io/api v0.19.3
	k8s.io/apiextensions-apiserver v0.19.3
	k8s.io/apimachinery v0.19.3
	k8s.io/cli-runtime v0.19.3
	k8s.io/client-go v0.19.3
	rsc.io/letsencrypt v0.0.3 // indirect
	sigs.k8s.io/controller-runtime v0.7.0-alpha.6
)
