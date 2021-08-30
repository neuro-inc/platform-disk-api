# Platform Disk Api

## Local Development

1. Install minikube (https://github.com/kubernetes/minikube#installation);
2. Authenticate local docker:
```shell
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 771188043543.dkr.ecr.us-east-1.amazonaws.com
```
(If values is outdated, ask someone for recent on slack and then update this file)
3. Launch minikube:
```shell
make start_k8s
```
4. Make sure the kubectl tool uses the minikube k8s cluster:
```shell
minikube status
kubectl config use-context minikube
```
5. Load images into minikube's virtual machine:
```shell
make docker_pull_test_images
```
6. Apply minikube configuration and some k8s fixture services:
```shell
make apply_configuration_k8s
```
5. Create a new virtual environment with Python 3.8:
```shell
python -m venv venv
source venv/bin/activate
```
6. Install testing dependencies:
```shell
make setup
```
7. Run the unit test suite:
```shell
make test_unit
```
8. Run the integration test suite:
```shell
make test_integration
```
9. Shutdown minikube:
```shell
minikube stop
```

## How to release

Push new tag of form `vXX.XX.XX` where `XX.XX.XX` is semver version
(please just use the date, like 20.12.31 for 31 December 2020).
You can do this by using github "Create release" UI.
