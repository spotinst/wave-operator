module github.com/spotinst/wave-operator

go 1.16

require (
	github.com/aws/aws-sdk-go v1.38.35
	github.com/evanphx/json-patch/v5 v5.1.0
	github.com/go-logr/logr v0.4.0
	github.com/go-logr/zapr v0.3.0 // indirect
	github.com/golang/mock v1.4.4
	github.com/google/go-cmp v0.5.4
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/jetstack/cert-manager v1.3.1
	github.com/magiconair/properties v1.8.1
	github.com/mattbaird/jsonpatch v0.0.0-20200820163806-098863c1fc24
	github.com/mitchellh/go-homedir v1.1.0
	github.com/onsi/ginkgo v1.14.2
	github.com/onsi/gomega v1.10.2
	github.com/prometheus/client_golang v1.10.0
	github.com/spf13/cobra v1.1.3
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.7.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	helm.sh/helm/v3 v3.5.4
	k8s.io/api v0.20.4
	k8s.io/apiextensions-apiserver v0.20.4
	k8s.io/apimachinery v0.20.4
	k8s.io/cli-runtime v0.20.4
	k8s.io/client-go v0.20.4
	sigs.k8s.io/controller-runtime v0.8.3
)

replace (
	// https://github.com/helm/helm/issues/9354
	github.com/docker/distribution => github.com/docker/distribution v0.0.0-20191216044856-a8371794149d
	github.com/docker/docker => github.com/moby/moby v17.12.0-ce-rc1.0.20200618181300-9dc6525e6118+incompatible
)
