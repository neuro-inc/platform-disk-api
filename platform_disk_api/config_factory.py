import logging
import os
from collections.abc import Sequence
from pathlib import Path

from apolo_events_client import EventsClientConfig
from apolo_kube_client.config import KubeClientAuthType, KubeConfig
from yarl import URL

from .config import (
    AuthConfig,
    Config,
    CORSConfig,
    DiskConfig,
    DiskUsageWatcherConfig,
    JobMigrateProjectNamespaceConfig,
    ServerConfig,
)


logger = logging.getLogger(__name__)


class EnvironConfigFactory:
    def __init__(self, environ: dict[str, str] | None = None) -> None:
        self._environ = environ or os.environ

    def _get_url(self, name: str) -> URL | None:
        value = self._environ[name]
        if value == "-":
            return None
        return URL(value)

    def create(self) -> Config:
        cluster_name = self._environ["NP_CLUSTER_NAME"]
        enable_docs = self._environ.get("NP_DISK_API_ENABLE_DOCS", "false") == "true"
        return Config(
            server=self._create_server(),
            platform_auth=self._create_platform_auth(),
            kube=self.create_kube(),
            cluster_name=cluster_name,
            cors=self.create_cors(),
            disk=self.create_disk(),
            enable_docs=enable_docs,
            events=self.create_events(),
        )

    def create_events(self) -> EventsClientConfig | None:
        if "NP_REGISTRY_EVENTS_URL" in self._environ:
            url = URL(self._environ["NP_REGISTRY_EVENTS_URL"])
            token = self._environ["NP_REGISTRY_EVENTS_TOKEN"]
            return EventsClientConfig(url=url, token=token, name="platform-disk")
        return None

    def create_disk_usage_watcher(self) -> DiskUsageWatcherConfig:
        return DiskUsageWatcherConfig(
            server=self._create_server(),
            kube=self.create_kube(),
        )

    def create_job_migrate_project(self) -> JobMigrateProjectNamespaceConfig:
        return JobMigrateProjectNamespaceConfig(kube=self.create_kube())

    def _create_server(self) -> ServerConfig:
        host = self._environ.get("NP_DISK_API_HOST", ServerConfig.host)
        port = int(self._environ.get("NP_DISK_API_PORT", ServerConfig.port))
        tls_cert_path = self._environ.get(
            "NP_DISK_API_TLS_CERT_PATH", ServerConfig.tls_cert_path
        )
        tls_key_path = self._environ.get(
            "NP_DISK_API_TLS_KEY_PATH", ServerConfig.tls_key_path
        )
        return ServerConfig(
            host=host,
            port=port,
            tls_cert_path=tls_cert_path,
            tls_key_path=tls_key_path,
        )

    def _create_platform_auth(self) -> AuthConfig:
        url = self._get_url("NP_DISK_API_PLATFORM_AUTH_URL")
        token = self._environ["NP_DISK_API_PLATFORM_AUTH_TOKEN"]
        return AuthConfig(url=url, token=token)

    def create_kube(self) -> KubeConfig:
        endpoint_url = self._environ["NP_DISK_API_K8S_API_URL"]
        auth_type = KubeClientAuthType(
            self._environ.get("NP_DISK_API_K8S_AUTH_TYPE", KubeConfig.auth_type.value)
        )
        ca_path = self._environ.get("NP_DISK_API_K8S_CA_PATH")
        ca_data = Path(ca_path).read_text() if ca_path else None

        token_path = self._environ.get("NP_DISK_API_K8S_TOKEN_PATH")
        token = Path(token_path).read_text() if token_path else None

        return KubeConfig(
            endpoint_url=endpoint_url,
            cert_authority_data_pem=ca_data,
            auth_type=auth_type,
            auth_cert_path=self._environ.get("NP_DISK_API_K8S_AUTH_CERT_PATH"),
            auth_cert_key_path=self._environ.get("NP_DISK_API_K8S_AUTH_CERT_KEY_PATH"),
            token=token,
            token_path=token_path,
            namespace=self._environ.get("NP_DISK_API_K8S_NS", KubeConfig.namespace),
            client_conn_timeout_s=int(
                self._environ.get("NP_DISK_API_K8S_CLIENT_CONN_TIMEOUT")
                or KubeConfig.client_conn_timeout_s
            ),
            client_read_timeout_s=int(
                self._environ.get("NP_DISK_API_K8S_CLIENT_READ_TIMEOUT")
                or KubeConfig.client_read_timeout_s
            ),
            client_watch_timeout_s=int(
                self._environ.get("NP_DISK_API_K8S_CLIENT_WATCH_TIMEOUT")
                or KubeConfig.client_watch_timeout_s
            ),
            client_conn_pool_size=int(
                self._environ.get("NP_DISK_API_K8S_CLIENT_CONN_POOL_SIZE")
                or KubeConfig.client_conn_pool_size
            ),
        )

    def create_disk(self) -> DiskConfig:
        return DiskConfig(
            k8s_storage_class=self._environ.get(
                "NP_DISK_API_K8S_STORAGE_CLASS", DiskConfig.k8s_storage_class
            ),
            storage_limit_per_project=int(
                self._environ["NP_DISK_API_STORAGE_LIMIT_PER_PROJECT"]
            ),
        )

    def create_cors(self) -> CORSConfig:
        origins: Sequence[str] = CORSConfig.allowed_origins
        origins_str = self._environ.get("NP_CORS_ORIGINS", "").strip()
        if origins_str:
            origins = origins_str.split(",")
        return CORSConfig(allowed_origins=origins)
