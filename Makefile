include k8s.mk

setup:
	pip install -U pip
	pip install -e .[dev]
	pre-commit install

lint: format
	mypy platform_disk_api tests

format:
ifdef CI
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
	docker build -t $(IMAGE_NAME):latest .

docker_pull_test_images:
ifeq ($(MINIKUBE_DRIVER),none)
	make _docker_pull_test_images
else
	@eval $$(minikube docker-env); \
	make _docker_pull_test_images
endif

_docker_pull_test_images:
	docker pull ghcr.io/neuro-inc/platformauthapi:latest; \
	docker tag ghcr.io/neuro-inc/platformauthapi:latest platformauthapi:latest
