.PHONY: help
help: ## Display this help
	$(Q) awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

VERSION = 0.1
REGISTRY = public.ecr.aws/l8m2k1n1/netapp/jupyter-kernelspecs
IMG ?= ${REGISTRY}:${VERSION}
LATEST = ${REGISTRY}:latest

docker-build: ## Build the docker image
	docker build . -t ${IMG}
	docker tag ${IMG} ${LATEST}

docker-push:  ## Push the docker image
	docker push ${IMG}
	docker push ${LATEST}
