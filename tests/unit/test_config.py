from pathlib import Path
from typing import Any

import pytest
from apolo_kube_client.config import KubeClientAuthType, KubeConfig
from yarl import URL

from platform_disk_api.config import (
    AuthConfig,
    Config,
    CORSConfig,
    DiskConfig,
    DiskUsageWatcherConfig,
    ServerConfig,
)
from platform_disk_api.config_factory import EnvironConfigFactory

CA_DATA_PEM = "this-is-certificate-authority-public-key"
TOKEN = "this-is-token"


@pytest.fixture()
def cert_authority_path(tmp_path: Path) -> str:
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text(CA_DATA_PEM)
    return str(ca_path)


@pytest.fixture()
def token_path(tmp_path: Path) -> str:
    token_path = tmp_path / "token"
    token_path.write_text(TOKEN)
    return str(token_path)


def test_create_default() -> None:
    environ: dict[str, Any] = {
        "NP_DISK_API_PLATFORM_AUTH_URL": "-",
        "NP_DISK_API_PLATFORM_AUTH_TOKEN": "platform-auth-token",
        "NP_DISK_API_K8S_API_URL": "https://localhost:8443",
        "NP_DISK_API_STORAGE_LIMIT_PER_PROJECT": "444",
        "NP_CLUSTER_NAME": "default",
    }
    config = EnvironConfigFactory(environ).create()
    assert config == Config(
        server=ServerConfig(),
        platform_auth=AuthConfig(url=None, token="platform-auth-token"),
        kube=KubeConfig(endpoint_url="https://localhost:8443"),
        disk=DiskConfig(storage_limit_per_project=444),
        cluster_name="default",
        cors=CORSConfig(),
    )


def test_create_custom(cert_authority_path: str, token_path: str) -> None:
    environ: dict[str, Any] = {
        "NP_DISK_API_HOST": "0.0.0.0",
        "NP_DISK_API_PORT": 8080,
        "NP_DISK_API_PLATFORM_AUTH_URL": "http://platformauthapi/api/v1",
        "NP_DISK_API_PLATFORM_AUTH_TOKEN": "platform-auth-token",
        "NP_DISK_API_K8S_API_URL": "https://localhost:8443",
        "NP_DISK_API_K8S_AUTH_TYPE": "token",
        "NP_DISK_API_K8S_CA_PATH": cert_authority_path,
        "NP_DISK_API_K8S_TOKEN_PATH": token_path,
        "NP_DISK_API_K8S_AUTH_CERT_PATH": "/cert_path",
        "NP_DISK_API_K8S_AUTH_CERT_KEY_PATH": "/cert_key_path",
        "NP_DISK_API_K8S_NS": "other-namespace",
        "NP_DISK_API_K8S_CLIENT_CONN_TIMEOUT": "111",
        "NP_DISK_API_K8S_CLIENT_READ_TIMEOUT": "222",
        "NP_DISK_API_K8S_CLIENT_WATCH_TIMEOUT": "555",
        "NP_DISK_API_K8S_CLIENT_CONN_POOL_SIZE": "333",
        "NP_DISK_API_K8S_STORAGE_CLASS": "some-class",
        "NP_DISK_API_ENABLE_DOCS": "true",
        "NP_DISK_API_STORAGE_LIMIT_PER_PROJECT": "444",
        "NP_CLUSTER_NAME": "default",
        "NP_CORS_ORIGINS": "https://domain1.com,http://do.main",
        "SENTRY_DSN": "https://sentry",
        "SENTRY_CLUSTER_NAME": "test",
    }
    config = EnvironConfigFactory(environ).create()
    assert config == Config(
        server=ServerConfig(host="0.0.0.0", port=8080),
        platform_auth=AuthConfig(
            url=URL("http://platformauthapi/api/v1"), token="platform-auth-token"
        ),
        kube=KubeConfig(
            endpoint_url="https://localhost:8443",
            cert_authority_data_pem=CA_DATA_PEM,
            auth_type=KubeClientAuthType.TOKEN,
            token=TOKEN,
            token_path=token_path,
            auth_cert_path="/cert_path",
            auth_cert_key_path="/cert_key_path",
            namespace="other-namespace",
            client_conn_timeout_s=111,
            client_read_timeout_s=222,
            client_watch_timeout_s=555,
            client_conn_pool_size=333,
        ),
        disk=DiskConfig(k8s_storage_class="some-class", storage_limit_per_project=444),
        cluster_name="default",
        cors=CORSConfig(["https://domain1.com", "http://do.main"]),
        enable_docs=True,
    )


def test_create_disk_usage_watcher() -> None:
    environ: dict[str, Any] = {
        "NP_DISK_API_HOST": "127.0.0.1",
        "NP_DISK_API_PORT": 8081,
        "NP_DISK_API_K8S_API_URL": "https://localhost:8443",
        "SENTRY_DSN": "https://sentry",
        "SENTRY_CLUSTER_NAME": "test",
    }
    config = EnvironConfigFactory(environ).create_disk_usage_watcher()
    assert config == DiskUsageWatcherConfig(
        server=ServerConfig(host="127.0.0.1", port=8081),
        kube=KubeConfig(endpoint_url="https://localhost:8443"),
    )
