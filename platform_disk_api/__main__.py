import os

from platform_disk_api.api import main

if __name__ == "__main__":
    values = {
        "NP_CLUSTER_NAME": "minikube",
        "NP_DISK_API_ENABLE_DOCS": "true",
        "NP_DISK_API_PORT": "13000",
        "NP_DISK_API_PLATFORM_AUTH_URL": "-",
        "NP_DISK_API_PLATFORM_AUTH_TOKEN": "-",
        "NP_DISK_API_K8S_API_URL": "http://localhost:8080",
        "NP_DISK_API_STORAGE_LIMIT_PER_USER": "1",
    }
    for key, value in values.items():
        os.environ[key] = value
    main()
