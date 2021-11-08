AWS_ACCOUNT_ID ?= 771188043543
AWS_REGION ?= us-east-1

AZURE_RG_NAME ?= dev
AZURE_ACR_NAME ?= crc570d91c95c6aac0ea80afb1019a0c6f

GITHUB_OWNER ?= neuro-inc

TAG ?= latest

IMAGE_REPO_gke    = $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)
IMAGE_REPO_aws    = $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com
IMAGE_REPO_azure  = $(AZURE_ACR_NAME).azurecr.io
IMAGE_REPO_github = ghcr.io/$(GITHUB_OWNER)

IMAGE_REGISTRY ?= aws

IMAGE_NAME      = platformdiskapi
IMAGE_REPO_BASE = $(IMAGE_REPO_$(IMAGE_REGISTRY))
IMAGE_REPO      = $(IMAGE_REPO_BASE)/$(IMAGE_NAME)

HELM_ENV           ?= dev
HELM_CHART          = platform-disks
HELM_CHART_VERSION ?= 1.0.0
HELM_APP_VERSION   ?= 1.0.0

include k8s.mk

setup:
	pip install -U pip
	pip install -e .[dev]
	pre-commit install

lint: format
	mypy platform_disk_api tests

format:
ifdef CI_LINT_RUN
	pre-commit run --all-files --show-diff-on-failure
else
	pre-commit run --all-files
endif

test_unit:
	pytest -vv --cov=platform_disk_api --cov-report xml:.coverage-unit.xml tests/unit

test_integration:
	pytest -vv --maxfail=3 --cov=platform_disk_api --cov-report xml:.coverage-integration.xml tests/integration

docker_build:
	rm -rf build dist
	pip install -U build
	python -m build
	docker build \
		--build-arg PYTHON_BASE=slim-buster \
		-t $(IMAGE_NAME):latest .

docker_push: docker_build
	docker tag $(IMAGE_NAME):latest $(IMAGE_REPO):$(TAG)
	docker push $(IMAGE_REPO):$(TAG)

	docker tag $(IMAGE_NAME):latest $(IMAGE_REPO):latest
	docker push $(IMAGE_REPO):latest

docker_pull_test_images:
ifeq ($(MINIKUBE_DRIVER),none)
	make _docker_pull_test_images
else
	@eval $$(minikube docker-env); \
	make _docker_pull_test_images
endif

_docker_pull_test_images:
	docker pull $(IMAGE_REPO_BASE)/platformauthapi:latest; \
	docker tag $(IMAGE_REPO_BASE)/platformauthapi:latest platformauthapi:latest

aws_k8s_login:
	aws eks --region $(AWS_REGION) update-kubeconfig --name $(CLUSTER_NAME)

azure_k8s_login:
	az aks get-credentials --resource-group $(AZURE_RG_NAME) --name $(CLUSTER_NAME)

helm_create_chart:
	export IMAGE_REPO=$(IMAGE_REPO); \
	export IMAGE_TAG=$(TAG); \
	export CHART_VERSION=$(HELM_CHART_VERSION); \
	export APP_VERSION=$(HELM_APP_VERSION); \
	VALUES=$$(cat charts/$(HELM_CHART)/values.yaml | envsubst); \
	echo "$$VALUES" > charts/$(HELM_CHART)/values.yaml; \
	CHART=$$(cat charts/$(HELM_CHART)/Chart.yaml | envsubst); \
	echo "$$CHART" > charts/$(HELM_CHART)/Chart.yaml

helm_deploy: helm_create_chart
	helm upgrade $(HELM_CHART) temp_deploy/$(HELM_CHART) \
		-f deploy/$(HELM_CHART)/values-$(HELM_ENV).yaml \
		--namespace platform --install --wait --timeout 600s
