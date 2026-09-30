# Image URL to use for all building/pushing image targets.
IMG ?= ghcr.io/backblaze-b2-samples/b2-kubernetes-operator:dev
# Kubernetes version of the envtest control plane binaries.
ENVTEST_K8S_VERSION ?= 1.34.x

GOBIN ?= $(shell go env GOPATH)/bin
LOCALBIN ?= $(shell pwd)/bin
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
SETUP_ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT ?= $(shell command -v golangci-lint 2>/dev/null || echo $(LOCALBIN)/golangci-lint)

CONTROLLER_TOOLS_VERSION ?= v0.22.0
ENVTEST_VERSION ?= release-0.25
GOLANGCI_LINT_VERSION ?= v2.12.2

CHART_DIR := charts/b2-kubernetes-operator
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate CRDs and RBAC, and copy CRDs into the Helm chart.
	$(CONTROLLER_GEN) rbac:roleName=b2-operator-manager crd paths="./..." output:crd:artifacts:config=config/crd/bases output:rbac:artifacts:config=config/rbac
	$(CONTROLLER_GEN) crd paths="./..." output:crd:artifacts:config=$(CHART_DIR)/crds
	hack/sync-chart-rbac.sh

.PHONY: generate
generate: controller-gen ## Generate DeepCopy methods.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: fmt
fmt: ## Run go fmt.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet.
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint.
	$(GOLANGCI_LINT) run

.PHONY: verify
verify: generate manifests ## Fail if generated files are out of date.
	@git diff --exit-code -- api config $(CHART_DIR)/crds $(CHART_DIR)/templates/clusterrole-manager.yaml || \
		(echo "Generated files are out of date: run 'make generate manifests' and commit." && exit 1)

.PHONY: test
test: generate fmt vet setup-envtest ## Run unit and integration (envtest) tests.
	KUBEBUILDER_ASSETS="$$($(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" \
		go test -race -coverpkg=./internal/... -coverprofile cover.out ./...

KIND_CLUSTER ?= b2-operator-e2e

.PHONY: e2e-setup
e2e-setup: ## Create a kind cluster and install the operator and the fake B2 API into it.
	kind get clusters | grep -qx $(KIND_CLUSTER) || kind create cluster --name $(KIND_CLUSTER) --wait 120s
	docker build -t b2-kubernetes-operator:e2e .
	docker build --target b2fake -t b2fake:e2e .
	kind load docker-image --name $(KIND_CLUSTER) b2-kubernetes-operator:e2e b2fake:e2e
	kubectl --context kind-$(KIND_CLUSTER) create namespace b2-operator-system --dry-run=client -o yaml | kubectl --context kind-$(KIND_CLUSTER) apply -f -
	kubectl --context kind-$(KIND_CLUSTER) apply -f test/e2e/testdata/b2fake.yaml
	helm upgrade --install b2-operator $(CHART_DIR) --kube-context kind-$(KIND_CLUSTER) \
		--namespace b2-operator-system --wait --timeout 3m \
		--set image.repository=b2-kubernetes-operator,image.tag=e2e,image.pullPolicy=Never \
		--set operator.logEncoding=console,operator.defaultGracePeriod=5s \
		--set 'operator.extraArgs={--allow-insecure-api-url}'
	# The image tag does not change between runs, so force pods onto the fresh images.
	kubectl --context kind-$(KIND_CLUSTER) -n b2-operator-system rollout restart deploy/b2-operator deploy/b2fake
	kubectl --context kind-$(KIND_CLUSTER) -n b2-operator-system rollout status deploy/b2-operator --timeout 2m
	kubectl --context kind-$(KIND_CLUSTER) -n b2-operator-system rollout status deploy/b2fake --timeout 2m

.PHONY: test-e2e
test-e2e: e2e-setup ## Run end-to-end tests against a kind cluster.
	KUBECONTEXT=kind-$(KIND_CLUSTER) go test -tags e2e -timeout 20m -count=1 -v ./test/e2e/...

.PHONY: e2e-teardown
e2e-teardown: ## Delete the e2e kind cluster.
	kind delete cluster --name $(KIND_CLUSTER)

.PHONY: helm-lint
helm-lint: ## Lint the Helm chart.
	helm lint $(CHART_DIR) --strict
	helm template b2-operator $(CHART_DIR) --namespace b2-operator-system > /dev/null

##@ Build

.PHONY: build
build: generate fmt vet ## Build the manager binary.
	go build -trimpath -o bin/manager ./cmd/manager

.PHONY: run
run: manifests generate ## Run the operator against the current kubeconfig.
	go run ./cmd/manager --leader-elect=false

.PHONY: docker-build
docker-build: ## Build the operator image.
	docker build -t $(IMG) .

.PHONY: docker-buildx
docker-buildx: ## Build and push a multi-arch image.
	docker buildx build --platform linux/amd64,linux/arm64 --push -t $(IMG) .

##@ Tools

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN)
$(CONTROLLER_GEN):
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

.PHONY: setup-envtest
setup-envtest: $(SETUP_ENVTEST)
$(SETUP_ENVTEST):
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(ENVTEST_VERSION)
