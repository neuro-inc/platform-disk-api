IMAGE_NAME ?= platformdiskapi
IMAGE_TAG ?= latest
ARTIFACTORY_TAG ?= $(shell echo "$(GITHUB_REF)" | awk -F/ '{print $$NF}')
IMAGE ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME)
#IMAGE_AWS ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(IMAGE_NAME)

CLOUD_IMAGE_gke   ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME)
CLOUD_IMAGE_aws   ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(IMAGE_NAME)
CLOUD_IMAGE_azure ?= $(AZURE_DEV_ACR_NAME).azurecr.io/$(IMAGE_NAME)

CLOUD_IMAGE  = ${CLOUD_IMAGE_${CLOUD_PROVIDER}}

PLATFORMAUTHAPI_TAG=disk-support

export PIP_EXTRA_INDEX_URL ?= $(shell python pip_extra_index_url.py)

include k8s.mk

setup:
	@echo "Using extra pip index: $(PIP_EXTRA_INDEX_URL)"
	pip install -r requirements/test.txt

lint:
	isort --check-only --diff platform_disk_api tests setup.py
	black --check platform_disk_api tests setup.py
	flake8 platform_disk_api tests setup.py
	mypy platform_disk_api tests setup.py

format:
	isort platform_disk_api tests setup.py
	black platform_disk_api tests setup.py

test_unit:
	pytest -vv --cov=platform_disk_api --cov-report xml:.coverage-unit.xml tests/unit

test_integration:
	pytest -vv --maxfail=3 --cov=platform_disk_api --cov-report xml:.coverage-integration.xml tests/integration

build:
	docker build -f Dockerfile -t $(IMAGE_NAME):$(IMAGE_TAG) --build-arg PIP_EXTRA_INDEX_URL .

docker_pull_test_images:
	@eval $$(minikube docker-env); \
	    docker pull $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/platformauthapi:$(PLATFORMAUTHAPI_TAG); \
	    docker tag $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/platformauthapi:$(PLATFORMAUTHAPI_TAG) platformauthapi:latest

eks_login:
	aws eks --region $(AWS_REGION) update-kubeconfig --name $(CLUSTER_NAME)

aks_login:
	az aks get-credentials --resource-group $(AZURE_DEV_RG_NAME) --name $(CLUSTER_NAME)

ecr_login:
	$$(aws ecr get-login --no-include-email --region $(AWS_REGION))

docker_push: build
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(CLOUD_IMAGE):latest
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(CLOUD_IMAGE):$(GITHUB_SHA)
	docker push $(CLOUD_IMAGE):latest
	docker push $(CLOUD_IMAGE):$(GITHUB_SHA)

_helm:
	curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get | bash -s -- -v $(HELM_VERSION)
	helm init --client-only

helm_deploy: _helm
	helm -f deploy/platformdiskapi/values-$(HELM_ENV)-$(CLOUD_PROVIDER).yaml --set "IMAGE=$(CLOUD_IMAGE):$(GITHUB_SHA)" upgrade --install platformdiskapi deploy/platformdiskapi/ --namespace platform --wait --timeout 600

artifactory_docker_push: build
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(ARTIFACTORY_DOCKER_REPO)/$(IMAGE_NAME):$(ARTIFACTORY_TAG)
	docker login $(ARTIFACTORY_DOCKER_REPO) --username=$(ARTIFACTORY_USERNAME) --password=$(ARTIFACTORY_PASSWORD)
	docker push $(ARTIFACTORY_DOCKER_REPO)/$(IMAGE_NAME):$(ARTIFACTORY_TAG)

artifactory_helm_push: _helm
	mkdir -p temp_deploy/platformdiskapi
	cp -Rf deploy/platformdiskapi/. temp_deploy/platformdiskapi
	cp temp_deploy/platformdiskapi/values-template.yaml temp_deploy/platformdiskapi/values.yaml
	sed -i "s/IMAGE_TAG/$(ARTIFACTORY_TAG)/g" temp_deploy/platformdiskapi/values.yaml
	find temp_deploy/platformdiskapi -type f -name 'values-*' -delete
	helm init --client-only
	helm package --app-version=$(ARTIFACTORY_TAG) --version=$(ARTIFACTORY_TAG) temp_deploy/platformdiskapi/
	helm plugin install https://github.com/belitre/helm-push-artifactory-plugin
	helm push-artifactory $(IMAGE_NAME)-$(ARTIFACTORY_TAG).tgz $(ARTIFACTORY_HELM_REPO) --username $(ARTIFACTORY_USERNAME) --password $(ARTIFACTORY_PASSWORD)
