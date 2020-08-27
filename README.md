# Platform Disk Api

## Local Development

1. Install minikube (https://github.com/kubernetes/minikube#installation);
2. Authenticate local docker:
```shell
export AWS_REGION=us-east-1 # replace with proper region
make ecr_login # aws cli version 1 required
```
3. Launch minikube:
```shell
./minikube.sh start
```
4. Make sure the kubectl tool uses the minikube k8s cluster:
```shell
minikube status
kubectl config use-context minikube
```
5. Load images into minikube's virtual machine:
```shell
export AWS_ACCOUNT_ID=771188043543 # replace with proper id.
export AWS_REGION=us-east-1 # replace with proper region.
./minikube.sh load-images
```
6. Apply minikube configuration and some k8s fixture services:
```shell
./minikube.sh apply
```
5. Create a new virtual environment with Python 3.7:
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
9. Cleanup+shutdown minikube:
```shell
./minikube.sh clean
./minikube.sh stop
```
