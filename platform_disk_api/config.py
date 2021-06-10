import enum
from dataclasses import dataclass, field
from typing import Optional, Sequence

from yarl import URL


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080


@dataclass(frozen=True)
class AuthConfig:
    url: URL
    token: str = field(repr=False)


class KubeClientAuthType(str, enum.Enum):
    NONE = "none"
    TOKEN = "token"
    CERTIFICATE = "certificate"


@dataclass(frozen=True)
class CORSConfig:
    allowed_origins: Sequence[str] = ()


@dataclass(frozen=True)
class KubeConfig:
    endpoint_url: str
    cert_authority_data_pem: Optional[str] = field(repr=False, default=None)
    cert_authority_path: Optional[str] = None
    auth_type: KubeClientAuthType = KubeClientAuthType.CERTIFICATE
    auth_cert_path: Optional[str] = None
    auth_cert_key_path: Optional[str] = None
    token: Optional[str] = field(repr=False, default=None)
    namespace: str = "default"
    client_conn_timeout_s: int = 300
    client_read_timeout_s: int = 300
    client_watch_timeout_s: int = 1800
    client_conn_pool_size: int = 100


@dataclass(frozen=True)
class DiskConfig:
    k8s_storage_class: str
    storage_limit_per_user: int


@dataclass(frozen=True)
class ZipkinConfig:
    url: URL
    app_name: str
    sample_rate: float = 0


@dataclass(frozen=True)
class SentryConfig:
    dsn: URL
    cluster_name: str
    app_name: str
    sample_rate: float = 0


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    platform_auth: AuthConfig
    kube: KubeConfig
    cors: CORSConfig
    disk: DiskConfig
    cluster_name: str
    enable_docs: bool = False

    zipkin: Optional[ZipkinConfig] = None
    sentry: Optional[SentryConfig] = None


@dataclass(frozen=True)
class DiskUsageWatcherConfig:
    server: ServerConfig
    kube: KubeConfig
    zipkin: Optional[ZipkinConfig] = None
    sentry: Optional[SentryConfig] = None
