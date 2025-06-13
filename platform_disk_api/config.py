from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Optional

from apolo_kube_client.config import KubeConfig
from yarl import URL


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080


@dataclass(frozen=True)
class AuthConfig:
    url: Optional[URL]
    token: str = field(repr=False)


@dataclass(frozen=True)
class CORSConfig:
    allowed_origins: Sequence[str] = ()


@dataclass(frozen=True)
class DiskConfig:
    storage_limit_per_project: int
    k8s_storage_class: str = ""  # default k8s storage class


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    platform_auth: AuthConfig
    kube: KubeConfig
    cors: CORSConfig
    disk: DiskConfig
    cluster_name: str
    enable_docs: bool = False


@dataclass(frozen=True)
class DiskUsageWatcherConfig:
    server: ServerConfig
    kube: KubeConfig


@dataclass(frozen=True)
class JobMigrateProjectNamespaceConfig:
    kube: KubeConfig
