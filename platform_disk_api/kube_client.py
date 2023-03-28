import asyncio
import json
import logging
import ssl
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlsplit

import aiohttp
from aiohttp import ClientTimeout
from yarl import URL

from .config import KubeClientAuthType

logger = logging.getLogger(__name__)


class KubeClientException(Exception):
    pass


class ResourceNotFound(KubeClientException):
    pass


class ResourceInvalid(KubeClientException):
    pass


class ResourceExists(KubeClientException):
    pass


class ResourceBadRequest(KubeClientException):
    pass


class ResourceGone(KubeClientException):
    pass


class KubeClientUnauthorized(Exception):
    pass


def _storage_str_to_int(storage: str) -> int:
    # More about this format:
    # https://github.com/kubernetes/kubernetes/blob/6b963ed9c841619d511d2830719b6100d6ab1431/staging/src/k8s.io/apimachinery/pkg/api/resource/quantity.go#L30
    suffix_to_factor = {
        "E": 10**18,
        "P": 10**15,
        "T": 10**12,
        "G": 10**9,
        "M": 10**6,
        "k": 10**3,
        "Ei": 1024**6,
        "Pi": 1024**5,
        "Ti": 1024**4,
        "Gi": 1024**3,
        "Mi": 1024**2,
        "Ki": 1024,
    }
    try:
        return int(float(storage))
    except ValueError:
        for suffix, factor in suffix_to_factor.items():
            if storage.endswith(suffix):
                return factor * int(storage[: -len(suffix)])
        raise


@dataclass(frozen=True)
class PersistentVolumeClaimWrite:
    name: str
    storage: int  # In bytes
    storage_class_name: str = ""
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)

    def to_primitive(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "kind": "PersistentVolumeClaim",
            "apiVersion": "v1",
            "metadata": {"name": self.name},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "volumeMode": "Filesystem",
                "resources": {"requests": {"storage": self.storage}},
            },
        }
        if self.storage_class_name:
            result["spec"]["storageClassName"] = self.storage_class_name
        if self.labels:
            result["metadata"]["labels"] = self.labels
        if self.annotations:
            result["metadata"]["annotations"] = self.annotations
        return result


class MergeDiff:
    _diff: dict[str, Any]

    def __init__(self, diff: dict[str, Any]) -> None:
        self._diff = diff

    def serialize(self) -> str:
        return json.dumps(self._diff)

    @classmethod
    def make_add_label_diff(cls, label_key: str, value: str) -> "MergeDiff":
        return cls({"metadata": {"labels": {label_key: value}}})

    @classmethod
    def make_add_annotations_diff(cls, annotation_key: str, value: str) -> "MergeDiff":
        return cls({"metadata": {"annotations": {annotation_key: value}}})


@dataclass(frozen=True)
class PersistentVolumeClaimRead:
    name: str
    storage_class_name: str
    phase: "PersistentVolumeClaimRead.Phase"
    storage_requested: int
    storage_real: Optional[int]
    labels: dict[str, str]
    annotations: dict[str, str]

    class Phase(str, Enum):
        """Possible values for phase of PVC.

        Check k8s source code:
        https://github.com/kubernetes/kubernetes/blob/b7d44329f3514a65af9048224329a4897cf4d31d/pkg/apis/core/types.go#L540-L549
        """

        PENDING = "Pending"
        BOUND = "Bound"
        LOST = "Lost"

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "PersistentVolumeClaimRead":
        try:
            storage_real: Optional[int] = _storage_str_to_int(
                payload["status"]["capacity"]["storage"]
            )
        except KeyError:
            storage_real = None
        return cls(
            name=payload["metadata"]["name"],
            storage_class_name=payload["spec"]["storageClassName"],
            phase=cls.Phase(payload["status"]["phase"]),
            storage_requested=_storage_str_to_int(
                payload["spec"]["resources"]["requests"]["storage"]
            ),
            storage_real=storage_real,
            labels=payload["metadata"].get("labels", dict()),
            annotations=payload["metadata"].get("annotations", dict()),
        )


@dataclass(frozen=True)
class PodRead:
    pvc_in_use: list[str]

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "PodRead":
        pvc_names = []
        for volume in payload["spec"]["volumes"]:
            pvc_data = volume.get("persistentVolumeClaim")
            if pvc_data:
                pvc_names.append(pvc_data["claimName"])
        return PodRead(pvc_in_use=pvc_names)


@dataclass(frozen=True)
class PodListResult:
    resource_version: str
    pods: list[PodRead]

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "PodListResult":
        return PodListResult(
            resource_version=payload["metadata"]["resourceVersion"],
            pods=[PodRead.from_primitive(item) for item in payload["items"]],
        )


@dataclass(frozen=True)
class PodWatchEvent:
    type: "PodWatchEvent.Type"
    pod: PodRead
    resource_version: Optional[str] = None

    class Type(str, Enum):
        """Possible values for phase of PVC.

        Check k8s source code:
        https://github.com/kubernetes/kubernetes/blob/b7d44329f3514a65af9048224329a4897cf4d31d/pkg/apis/core/types.go#L540-L549
        """

        ADDED = "ADDED"
        MODIFIED = "MODIFIED"
        DELETED = "DELETED"
        ERROR = "ERROR"
        BOOKMARK = "BOOKMARK"

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "PodWatchEvent":
        event_type = next(
            event_type for event_type in cls.Type if event_type == payload["type"]
        )
        if event_type == cls.Type.BOOKMARK:
            return PodWatchEvent(
                type=event_type,
                resource_version=payload["object"]["metadata"]["resourceVersion"],
                pod=PodRead([]),
            )
        return PodWatchEvent(
            type=event_type, pod=PodRead.from_primitive(payload["object"])
        )

    @classmethod
    def is_error(cls, payload: dict[str, Any]) -> bool:
        return cls.Type.ERROR == payload["type"].upper()


@dataclass(frozen=True)
class PVCVolumeMetrics:
    pvc_name: str
    used_bytes: int


@dataclass(frozen=True)
class DiskNaming:
    name: str
    disk_id: str

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "DiskNaming":
        return DiskNaming(
            name=payload["metadata"]["name"],
            disk_id=payload["spec"]["disk_id"],
        )

    def to_primitive(self) -> dict[str, Any]:
        return {
            "kind": "DiskNaming",
            "apiVersion": "neuromation.io/v1",
            "metadata": {
                "name": self.name,
            },
            "spec": {
                "disk_id": self.disk_id,
            },
        }


class KubeClient:
    def __init__(
        self,
        *,
        base_url: str,
        namespace: str,
        cert_authority_path: Optional[str] = None,
        cert_authority_data_pem: Optional[str] = None,
        auth_type: KubeClientAuthType = KubeClientAuthType.CERTIFICATE,
        auth_cert_path: Optional[str] = None,
        auth_cert_key_path: Optional[str] = None,
        token: Optional[str] = None,
        token_path: Optional[str] = None,
        conn_timeout_s: int = 300,
        read_timeout_s: int = 100,
        watch_timeout_s: int = 1800,
        conn_pool_size: int = 100,
        trace_configs: Optional[list[aiohttp.TraceConfig]] = None,
    ) -> None:
        self._base_url = base_url
        self._namespace = namespace

        self._cert_authority_data_pem = cert_authority_data_pem
        self._cert_authority_path = cert_authority_path

        self._auth_type = auth_type
        self._auth_cert_path = auth_cert_path
        self._auth_cert_key_path = auth_cert_key_path
        self._token = token
        self._token_path = token_path

        self._conn_timeout_s = conn_timeout_s
        self._read_timeout_s = read_timeout_s
        self._watch_timeout_s = watch_timeout_s
        self._conn_pool_size = conn_pool_size
        self._trace_configs = trace_configs

        self._client: Optional[aiohttp.ClientSession] = None

    @property
    def _is_ssl(self) -> bool:
        return urlsplit(self._base_url).scheme == "https"

    def _create_ssl_context(self) -> Optional[ssl.SSLContext]:
        if not self._is_ssl:
            return None
        ssl_context = ssl.create_default_context(
            cafile=self._cert_authority_path, cadata=self._cert_authority_data_pem
        )
        if self._auth_type == KubeClientAuthType.CERTIFICATE:
            ssl_context.load_cert_chain(
                self._auth_cert_path,  # type: ignore
                self._auth_cert_key_path,
            )
        return ssl_context

    async def init(self) -> None:
        self._client = await self.create_http_client()

    async def create_http_client(self) -> aiohttp.ClientSession:
        connector = aiohttp.TCPConnector(
            limit=self._conn_pool_size, ssl=self._create_ssl_context()
        )
        if self._auth_type == KubeClientAuthType.TOKEN:
            token = self._token
            if not token:
                assert self._token_path is not None
                token = Path(self._token_path).read_text()
            headers = {"Authorization": "Bearer " + token}
        else:
            headers = {}
        timeout = aiohttp.ClientTimeout(
            connect=self._conn_timeout_s, total=self._read_timeout_s
        )
        return aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=headers,
            trace_configs=self._trace_configs,
        )

    async def _reload_http_client(self) -> None:
        await self.close()
        self._token = None
        await self.init()

    @property
    def namespace(self) -> str:
        return self._namespace

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None

    async def __aenter__(self) -> "KubeClient":
        await self.init()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    @property
    def _api_v1_url(self) -> str:
        return f"{self._base_url}/api/v1"

    def _generate_namespace_url(self, namespace_name: Optional[str] = None) -> str:
        namespace_name = namespace_name or self._namespace
        return f"{self._api_v1_url}/namespaces/{namespace_name}"

    @property
    def _namespace_url(self) -> str:
        return self._generate_namespace_url(self._namespace)

    @property
    def _pvc_url(self) -> str:
        return f"{self._namespace_url}/persistentvolumeclaims"

    def _generate_pvc_url(self, pvc_name: str) -> str:
        return f"{self._pvc_url}/{pvc_name}"

    @property
    def _disk_naming_url(self) -> str:
        return (
            f"{self._base_url}/apis/neuromation.io/v1/"
            f"namespaces/{self._namespace}/disknamings"
        )

    def _generate_disk_naming_url(self, name: str) -> str:
        return f"{self._disk_naming_url}/{name}"

    @property
    def _pod_url(self) -> str:
        return f"{self._namespace_url}/pods"

    async def _request(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        assert self._client, "client is not initialized"
        doing_retry = kwargs.pop("doing_retry", False)

        async with self._client.request(*args, **kwargs) as response:
            payload = await response.json()
        try:
            self._raise_for_status(payload)
            return payload
        except KubeClientUnauthorized:
            if doing_retry:
                raise
            # K8s SA's token might be stale, need to refresh it and retry
            await self._reload_http_client()
            kwargs["doing_retry"] = True
            return await self._request(*args, **kwargs)

    def _raise_for_status(self, payload: dict[str, Any]) -> None:
        kind = payload["kind"]
        if kind == "Status":
            if payload.get("status") == "Success":
                return
            code = payload.get("code")
            if code == 400:
                raise ResourceBadRequest(payload)
            if code == 401:
                raise KubeClientUnauthorized(payload)
            if code == 404:
                raise ResourceNotFound(payload)
            if code == 409:
                raise ResourceExists(payload)
            if code == 410:
                raise ResourceGone(payload)
            if code == 422:
                raise ResourceInvalid(payload["message"])
            raise KubeClientException(payload["message"])

    async def create_pvc(
        self, pvc: PersistentVolumeClaimWrite
    ) -> PersistentVolumeClaimRead:
        url = self._pvc_url
        payload = await self._request(method="POST", url=url, json=pvc.to_primitive())
        return PersistentVolumeClaimRead.from_primitive(payload)

    async def list_pvc(
        self, label_selector: Optional[str] = None
    ) -> list[PersistentVolumeClaimRead]:
        url = URL(self._pvc_url)
        if label_selector:
            url = url.with_query(labelSelector=label_selector)
        payload = await self._request(method="GET", url=url)
        return [
            PersistentVolumeClaimRead.from_primitive(item)
            for item in payload.get("items", [])
        ]

    async def get_pvc(self, pvc_name: str) -> PersistentVolumeClaimRead:
        url = self._generate_pvc_url(pvc_name)
        payload = await self._request(method="GET", url=url)
        return PersistentVolumeClaimRead.from_primitive(payload)

    async def update_pvc(
        self, pvc_name: str, json_diff: MergeDiff
    ) -> PersistentVolumeClaimRead:
        url = self._generate_pvc_url(pvc_name)
        payload = await self._request(
            method="PATCH",
            url=url,
            data=json_diff.serialize(),
            headers={"Content-Type": "application/merge-patch+json"},
        )
        return PersistentVolumeClaimRead.from_primitive(payload)

    async def remove_pvc(self, pvc_name: str) -> None:
        url = self._generate_pvc_url(pvc_name)
        await self._request(method="DELETE", url=url)

    async def list_pods(self) -> PodListResult:
        url = self._pod_url
        payload = await self._request(method="GET", url=url)
        return PodListResult.from_primitive(payload)

    async def watch_pods(
        self, resource_version: Optional[str] = None
    ) -> AsyncIterator[PodWatchEvent]:
        params = dict(watch="true", allowWatchBookmarks="true")
        if resource_version:
            params["resourceVersion"] = resource_version
        url = self._pod_url
        assert self._client, "client is not initialized"
        timeout = ClientTimeout(
            connect=self._conn_timeout_s,
            total=self._watch_timeout_s,
        )
        async with self._client.request(
            method="GET", url=url, params=params, timeout=timeout
        ) as response:
            if response.status == 410:
                raise ResourceGone
            try:
                async for line in response.content:
                    payload = json.loads(line)

                    if PodWatchEvent.is_error(payload):
                        self._raise_for_status(payload["object"])

                    yield PodWatchEvent.from_primitive(payload)
            except asyncio.TimeoutError:
                pass

    async def get_pvc_volumes_metrics(self) -> AsyncIterator[PVCVolumeMetrics]:
        assert self._client, "client is not initialized"
        # Get list of all nodes
        nodes_url = f"{self._api_v1_url}/nodes"
        async with self._client.request(method="GET", url=nodes_url) as response:
            payload = await response.json()
        nodes_list = payload.get("items", [])
        for node in nodes_list:
            # Check stats for each node
            node_name = node["metadata"]["name"]
            node_summary_url = f"{nodes_url}/{node_name}/proxy/stats/summary"
            try:
                payload = await self._request(method="GET", url=node_summary_url)
            except aiohttp.ContentTypeError as exc:
                logger.exception(
                    "Failed to parse node stats. "
                    "Response status: %s. Response headers: %s",
                    exc.status,
                    exc.headers,
                )
                continue
            for pod in payload.get("pods", []):
                for volume in pod.get("volume", []):
                    try:
                        yield PVCVolumeMetrics(
                            pvc_name=volume["pvcRef"]["name"],
                            used_bytes=volume["usedBytes"],
                        )
                    except KeyError:
                        pass

    async def create_disk_naming(self, disk_naming: DiskNaming) -> None:
        url = self._disk_naming_url
        await self._request(method="POST", url=url, json=disk_naming.to_primitive())

    async def list_disk_namings(self) -> list[DiskNaming]:
        url = self._disk_naming_url
        payload = await self._request(method="GET", url=url)
        return [DiskNaming.from_primitive(item) for item in payload.get("items", [])]

    async def get_disk_naming(self, name: str) -> DiskNaming:
        url = self._generate_disk_naming_url(name)
        payload = await self._request(method="GET", url=url)
        return DiskNaming.from_primitive(payload)

    async def remove_disk_naming(self, name: str) -> None:
        url = self._generate_disk_naming_url(name)
        await self._request(method="DELETE", url=url)
