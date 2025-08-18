.PHONY: all test clean
all test clean:

include k8s.mk

.PHONY: venv
venv:
	poetry lock
	poetry install --with dev;

.PHONY: setup
setup: venv
	poetry run pre-commit install;


.PHONY: lint
lint: format
	poetry run mypy platform_disk_api tests

format:
ifdef CI
	poetry run pre-commit run --all-files --show-diff-on-failure
else
	poetry run pre-commit run --all-files
endif

.PHONY: test_unit
test_unit:
	poetry run pytest -vv --cov-config=pyproject.toml --cov-report xml:.coverage-unit.xml tests/unit

.PHONY: test_integration
test_integration:
	poetry run pytest -vv --maxfail=3 --cov-config=pyproject.toml --cov-report xml:.coverage-integration.xml tests/integration

.PHONY: clean-dist
clean-dist:
	rm -rf dist

IMAGE_NAME = platformdiskapi

.PHONY: docker_build
docker_build: dist
	docker build \
		--build-arg PY_VERSION=$$(cat .python-version) \
		-t $(IMAGE_NAME):latest .

.python-version:
	@echo "Error: .python-version file is missing!" && exit 1

.PHONY: dist
dist: venv
	rm -rf build dist; \
	poetry export -f requirements.txt --without-hashes -o requirements.txt; \
	poetry build -f wheel;

.PHONY: docker_pull_test_images
docker_pull_test_images:
ifeq ($(MINIKUBE_DRIVER),none)
	make _docker_pull_test_images
else
	@eval $$(minikube docker-env); \
	make _docker_pull_test_images
endif

_docker_pull_test_images:
	docker pull ghcr.io/neuro-inc/platformadmin:latest \
