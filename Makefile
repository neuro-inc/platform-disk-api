IMAGE_NAME ?= platformdiskapi
IMAGE_TAG ?= latest
ARTIFACTORY_TAG ?= $(shell echo $${GITHUB_REF\#refs/tags/v})

CLOUD_REPO_gke   ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)
CLOUD_REPO_aws   ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com
CLOUD_REPO_azure ?= $(AZURE_ACR_NAME).azurecr.io

CLOUD_REPO  = $(CLOUD_REPO_$(CLOUD_PROVIDER))
CLOUD_IMAGE = $(CLOUD_REPO)/$(IMAGE_NAME)

export PIP_EXTRA_INDEX_URL ?= $(shell python pip_extra_index_url.py)

include k8s.mk

setup:
	@echo "Using extra pip index: $(PIP_EXTRA_INDEX_URL)"
	pip install -r requirements/test.txt
	pre-commit install

lint: format
	mypy platform_disk_api tests setup.py

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

build:
	python setup.py sdist
	docker build -f Dockerfile -t $(IMAGE_NAME):$(IMAGE_TAG) \
	--build-arg PIP_EXTRA_INDEX_URL \
	--build-arg DIST_FILENAME=`python setup.py --fullname`.tar.gz .

docker_pull_test_images:
	@eval $$(minikube docker-env); \
	    docker pull $(CLOUD_REPO)/platformauthapi:latest; \
	    docker tag $(CLOUD_REPO)/platformauthapi:latest platformauthapi:latest

aws_k8s_login:
	aws eks --region $(AWS_REGION) update-kubeconfig --name $(CLUSTER_NAME)

azure_k8s_login:
	az aks get-credentials --resource-group $(AZURE_RG_NAME) --name $(CLUSTER_NAME)

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
