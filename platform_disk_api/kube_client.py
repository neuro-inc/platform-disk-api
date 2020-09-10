import json
import logging
import ssl
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

import aiohttp

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


def _storage_str_to_int(storage: str) -> int:
    # More about this format:
    # https://github.com/kubernetes/kubernetes/blob/6b963ed9c841619d511d2830719b6100d6ab1431/staging/src/k8s.io/apimachinery/pkg/api/resource/quantity.go#L30
    suffix_to_factor = {
        "E": 10 ** 18,
        "P": 10 ** 15,
        "T": 10 ** 12,
        "G": 10 ** 9,
        "M": 10 ** 6,
        "k": 10 ** 3,
        "Ei": 1024 ** 6,
        "Pi": 1024 ** 5,
        "Ti": 1024 ** 4,
        "Gi": 1024 ** 3,
        "Mi": 1024 ** 2,
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
    storage_class_name: str
    storage: int  # In bytes
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)

    def to_primitive(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "kind": "PersistentVolumeClaim",
            "apiVersion": "v1",
            "metadata": {"name": self.name},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "volumeMode": "Filesystem",
                "resources": {"requests": {"storage": self.storage}},
                "storageClassName": self.storage_class_name,
            },
        }
        if self.labels:
            result["metadata"]["labels"] = self.labels
        if self.annotations:
            result["metadata"]["annotations"] = self.annotations
        return result


class MergeDiff:
    _diff: Dict[str, Any]

    def __init__(self, diff: Dict[str, Any]) -> None:
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
    labels: Dict[str, str]
    annotations: Dict[str, str]

    class Phase(str, Enum):
        """Possible values for phase of PVC.

        Check k8s source code:
        https://github.com/kubernetes/kubernetes/blob/b7d44329f3514a65af9048224329a4897cf4d31d/pkg/apis/core/types.go#L540-L549
        """

        PENDING = "Pending"
        BOUND = "Bound"
        LOST = "Lost"

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "PersistentVolumeClaimRead":
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
        conn_pool_size: int = 100,
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
        self._conn_pool_size = conn_pool_size
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
            connector=connector, timeout=timeout, headers=headers
        )

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

    async def _request(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        assert self._client, "client is not initialized"
        async with self._client.request(*args, **kwargs) as response:
            # TODO (A Danshyn 05/21/18): check status code etc
            payload = await response.json()
            return payload

    def _raise_for_status(self, payload: Dict[str, Any]) -> None:
        kind = payload["kind"]
        if kind == "Status":
            code = payload["code"]
            if code == 400:
                raise ResourceBadRequest(payload)
            if code == 404:
                raise ResourceNotFound(payload)
            if code == 409:
                raise ResourceExists(payload)
            if code == 422:
                raise ResourceInvalid(payload["message"])
            raise KubeClientException(payload["message"])

    async def create_pvc(
        self, pvc: PersistentVolumeClaimWrite
    ) -> PersistentVolumeClaimRead:
        url = self._pvc_url
        payload = await self._request(method="POST", url=url, json=pvc.to_primitive())
        self._raise_for_status(payload)
        return PersistentVolumeClaimRead.from_primitive(payload)

    async def list_pvc(self) -> List[PersistentVolumeClaimRead]:
        url = self._pvc_url
        payload = await self._request(method="GET", url=url)
        return [
            PersistentVolumeClaimRead.from_primitive(item)
            for item in payload.get("items", [])
        ]

    async def get_pvc(self, pvc_name: str) -> PersistentVolumeClaimRead:
        url = self._generate_pvc_url(pvc_name)
        payload = await self._request(method="GET", url=url)
        self._raise_for_status(payload)
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
        self._raise_for_status(payload)
        return PersistentVolumeClaimRead.from_primitive(payload)

    async def remove_pvc(self, pvc_name: str) -> None:
        url = self._generate_pvc_url(pvc_name)
        payload = await self._request(method="DELETE", url=url)
        self._raise_for_status(payload)
