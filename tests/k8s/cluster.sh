#!/usr/bin/env bash
set -o errexit

# based on
# https://github.com/kubernetes/minikube#linux-continuous-integration-without-vm-support

function k8s::install_minikube {
    local minikube_version="v1.25.2"
    sudo apt-get update
    sudo apt-get install -y conntrack
    curl -Lo minikube https://storage.googleapis.com/minikube/releases/${minikube_version}/minikube-linux-amd64
    chmod +x minikube
    sudo mv minikube /usr/local/bin/
}

function k8s::start {
 # ----------------------------------------------------------------------------
    # Bring up a local Minikube cluster with the “none” driver.
    # Preconditions:
    #   * minikube binary already installed (see k8s::install_minikube)
    #   * Docker (or containerd) present on the host
    # ----------------------------------------------------------------------------

    # ----- Kubeconfig -----------------------------------------------------------
    export KUBECONFIG="$HOME/.kube/config"
    mkdir -p "$(dirname "$KUBECONFIG")"
    touch "$KUBECONFIG"

    # ----- Minikube env vars ----------------------------------------------------
    export MINIKUBE_DISABLE_PROMPT=1          # suppress interactive prompts
    export MINIKUBE_DISABLE_WARNING=1         # suppress non-driver warning
    export MINIKUBE_HOME="$HOME"
    export CHANGE_MINIKUBE_NONE_USER=true     # allow non-root kubectl usage

    # ----- Kernel prerequisites for the none driver ----------------------------
    echo "• Enabling br_netfilter and required sysctl flags …"
    sudo modprobe br_netfilter
    sudo sysctl -w \
        net.bridge.bridge-nf-call-iptables=1 \
        net.bridge.bridge-nf-call-ip6tables=1 \
        net.ipv4.ip_forward=1

    # ----- Disable swap (kubeadm requirement) -----------------------------------
    echo "• Disabling swap …"
    sudo swapoff -a

    # ----- Optional utilities required by kubeadm pre-flight --------------------
    if ! command -v socat >/dev/null 2>&1; then
        echo "• Installing socat (kubeadm pre-flight dependency) …"
        sudo apt-get update -qq
        sudo apt-get install -y -qq socat
    fi

    # ----- Start Minikube -------------------------------------------------------
    echo "• Starting Minikube (driver=none) …"
    sudo -E minikube start \
        --driver=none \
        --wait=all \
        --wait-timeout=5m
}

function k8s::apply_all_configurations {
    echo "Applying configurations..."
    kubectl config use-context minikube
    kubectl apply -f tests/k8s/rb.default.gke.yml
    kubectl apply -f tests/k8s/platformapi.yml
    kubectl apply -f tests/k8s/storageclass.yml
    kubectl apply -f charts/platform-disks/templates/crd-disknaming.yaml
}


function k8s::stop {
    sudo -E minikube stop || :
    sudo -E minikube delete || :
    sudo -E rm -rf ~/.minikube
    sudo rm -rf /root/.minikube
}


function k8s::test {
    kubectl delete jobs testjob1 2>/dev/null || :
    kubectl create -f tests/k8s/pod.yml
    for i in {1..300}; do
        if [ "$(kubectl get job testjob1 --template {{.status.succeeded}})" == "1" ]; then
            exit 0
        fi
        if [ "$(kubectl get job testjob1 --template {{.status.failed}})" == "1" ]; then
            exit 1
        fi
        sleep 1
    done
    echo "Could not complete test job"
    kubectl describe job testjob1
    exit 1
}

case "${1:-}" in
    install)
        k8s::install_minikube
        ;;
    start)
        k8s::start
        ;;
    apply)
        k8s::apply_all_configurations
        ;;
    stop)
        k8s::stop
        ;;
    test)
        k8s::test
        ;;
esac
