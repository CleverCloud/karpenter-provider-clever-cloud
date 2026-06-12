CONTROLLER_GEN_VERSION ?= v0.20.1
# Keep in sync with the version pinned in .github/workflows/ci-lint.yaml
GOLANGCI_LINT_VERSION ?= v2.12.2
IMAGE ?= ghcr.io/clevercloud/karpenter
TAG ?= dev
# Helm chart version derived from TAG (v0.2.0 -> 0.2.0)
CHART_VERSION ?= $(TAG:v%=%)

.PHONY: all
all: generate build test

.PHONY: build
build: ## Build the controller binary
	go build -o bin/karpenter-clevercloud ./cmd/controller

.PHONY: run
run: build ## Run the controller locally against the current kubeconfig
	DISABLE_LEADER_ELECTION=true ./bin/karpenter-clevercloud

.PHONY: test
test: ## Run unit tests
	go test ./pkg/... ./cmd/...

.PHONY: generate
generate: ## Generate deepcopy functions and the CleverNodeClass CRD
	go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION) object paths="./pkg/apis/..."
	go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION) crd paths="./pkg/apis/v1alpha1/..." output:crd:artifacts:config=deploy/crds
	$(MAKE) sync-chart-crds

.PHONY: sync-chart-crds
sync-chart-crds: ## Sync deploy/crds into both Helm charts (crds/ dir + templated CRD chart)
	cp deploy/crds/*.yaml charts/karpenter/crds/
	for f in deploy/crds/*.yaml; do \
		awk '{print} /^  annotations:$$/{ \
			print "    {{- with .Values.additionalAnnotations }}"; \
			print "    {{- toYaml . | nindent 4 }}"; \
			print "    {{- end }}"}' $$f \
			> charts/karpenter-crd/templates/$$(basename $$f); \
	done

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint (built from source on first run, then cached)
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run

.PHONY: image
image: ## Build the container image
	docker build -t $(IMAGE):$(TAG) .

.PHONY: chart-lint
chart-lint: ## Lint and render both Helm charts
	helm lint charts/karpenter-crd charts/karpenter
	helm template karpenter-crd charts/karpenter-crd > /dev/null
	helm template karpenter charts/karpenter > /dev/null

.PHONY: chart-package
chart-package: ## Package both Helm charts into dist/ (use TAG=v<semver>, e.g. TAG=v0.2.0)
	mkdir -p dist
	helm package charts/karpenter-crd charts/karpenter \
		--version $(CHART_VERSION) --app-version $(TAG) --destination dist

.PHONY: apply-crds
apply-crds: ## Install all CRDs (karpenter.sh + CleverNodeClass) into the cluster
	kubectl apply -f deploy/crds/

.PHONY: deploy
deploy: apply-crds ## Deploy karpenter in-cluster (expects the image to be available)
	kubectl apply -f deploy/karpenter.yaml

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "%-14s %s\n", $$1, $$2}'
