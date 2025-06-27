import asyncio
import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

import aiohttp
from aiohttp import ClientTimeout
from apolo_kube_client.client import KubeClient as ApoloKubeClient
from apolo_kube_client.config import KubeClientAuthType
from apolo_kube_client.errors import ResourceGone
from yarl import URL

logger = logging.getLogger(__name__)


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
    def make_add_label_diff(cls, labels: dict[str, str]) -> "MergeDiff":
        return cls({"metadata": {"labels": labels}})

    @classmethod
    def make_add_annotations_diff(cls, annotations: dict[str, str]) -> "MergeDiff":
        return cls({"metadata": {"annotations": annotations}})


@dataclass(frozen=True)
class PersistentVolumeClaimRead:
    namespace: str
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
            namespace=payload["metadata"]["namespace"],
            name=payload["metadata"]["name"],
            storage_class_name=payload["spec"]["storageClassName"],
            phase=cls.Phase(payload["status"]["phase"]),
            storage_requested=_storage_str_to_int(
                payload["spec"]["resources"]["requests"]["storage"]
            ),
            storage_real=storage_real,
            labels=payload["metadata"].get("labels", {}),
            annotations=payload["metadata"].get("annotations", {}),
        )


@dataclass(frozen=True)
class PodRead:
    namespace: str
    pvc_in_use: list[str]

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "PodRead":
        pvc_names = []
        for volume in payload["spec"].get("volumes", []):
            pvc_data = volume.get("persistentVolumeClaim")
            if pvc_data:
                pvc_names.append(pvc_data["claimName"])
        return PodRead(namespace=payload["metadata"]["namespace"], pvc_in_use=pvc_names)


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
                pod=PodRead(namespace="", pvc_in_use=[]),
            )
        return PodWatchEvent(
            type=event_type, pod=PodRead.from_primitive(payload["object"])
        )

    @classmethod
    def is_error(cls, payload: dict[str, Any]) -> bool:
        return cls.Type.ERROR == payload["type"].upper()


@dataclass(frozen=True)
class PVCVolumeMetrics:
    namespace: str
    pvc_name: str
    used_bytes: int


@dataclass(frozen=True)
class DiskNaming:
    namespace: str
    name: str
    disk_id: str

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "DiskNaming":
        return DiskNaming(
            namespace=payload["metadata"]["namespace"],
            name=payload["metadata"]["name"],
            disk_id=payload["spec"]["disk_id"],
        )

    def to_primitive(self) -> dict[str, Any]:
        return {
            "kind": "DiskNaming",
            "apiVersion": "neuromation.io/v1",
            "metadata": {
                "name": self.name,
                "namespace": self.namespace,
            },
            "spec": {
                "disk_id": self.disk_id,
            },
        }


class KubeClient(ApoloKubeClient):
    @property
    def _all_pods_url(self) -> str:
        return f"{self.api_v1_url}/pods"

    @property
    def _all_pvc_url(self) -> str:
        return f"{self.api_v1_url}/persistentvolumeclaims"

    def _generate_pvc_url(self, namespace: str, pvc_name: Optional[str] = None) -> str:
        url = self.generate_namespace_url(namespace)
        url = f"{url}/persistentvolumeclaims"
        if pvc_name:
            url = f"{url}/{pvc_name}"
        return url

    def _generate_disk_naming_url(
        self, namespace: Optional[str] = None, name: Optional[str] = None
    ) -> str:
        url = f"{self._base_url}/apis/neuromation.io/v1"
        if namespace:
            url = f"{url}/namespaces/{namespace}"
        url = f"{url}/disknamings"
        if name:
            url = f"{url}/{name}"
        return url

    @property
    def _all_storage_classes_url(self) -> str:
        return f"{self._base_url}/apis/storage.k8s.io/v1/storageclasses"

    def _generate_pods_url(self, namespace: str) -> str:
        url = self.generate_namespace_url(namespace)
        return f"{url}/pods"

    def _generate_statefulsets_url(self, namespace: str) -> str:
        return f"{self._base_url}/apis/apps/v1/namespaces/{namespace}/statefulsets"

    def _create_headers(
        self, headers: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        headers = dict(headers) if headers else {}
        if self._auth_type == KubeClientAuthType.TOKEN and self._token:
            headers["Authorization"] = "Bearer " + self._token
        return headers

    async def create_pvc(
        self, namespace: str, pvc: PersistentVolumeClaimWrite
    ) -> PersistentVolumeClaimRead:
        url = self._generate_pvc_url(namespace)
        payload = await self.post(url=url, json=pvc.to_primitive())
        return PersistentVolumeClaimRead.from_primitive(payload)

    async def list_pvc(
        self, namespace: Optional[str] = None, label_selector: Optional[str] = None
    ) -> list[PersistentVolumeClaimRead]:
        if namespace:
            url = URL(self._generate_pvc_url(namespace))
        else:
            url = URL(self._all_pvc_url)
        if label_selector:
            url = url.with_query(labelSelector=label_selector)
        payload = await self.get(url=url)
        return [
            PersistentVolumeClaimRead.from_primitive(item)
            for item in payload.get("items", [])
        ]

    async def get_pvc(self, namespace: str, pvc_name: str) -> PersistentVolumeClaimRead:
        url = self._generate_pvc_url(namespace, pvc_name)
        payload = await self.get(url=url)
        return PersistentVolumeClaimRead.from_primitive(payload)

    async def update_pvc(
        self, namespace: str, pvc_name: str, json_diff: MergeDiff
    ) -> PersistentVolumeClaimRead:
        url = self._generate_pvc_url(namespace, pvc_name)
        payload = await self.patch(
            url=url,
            data=json_diff.serialize(),
            headers={"Content-Type": "application/merge-patch+json"},
        )
        return PersistentVolumeClaimRead.from_primitive(payload)

    async def remove_pvc(self, namespace: str, pvc_name: str) -> None:
        url = self._generate_pvc_url(namespace, pvc_name)
        await self.delete(url=url)

    async def get_default_storage_class_name(self) -> str | None:
        response = await self.get(self._all_storage_classes_url)
        for storage_class in response["items"]:
            if (
                storage_class.get("metadata", {})
                .get("annotations", {})
                .get("storageclass.kubernetes.io/is-default-class")
            ) == "true":
                return storage_class["metadata"]["name"]
        return None

    async def list_pods(self) -> PodListResult:
        url = self._all_pods_url
        payload = await self.get(url=url)
        return PodListResult.from_primitive(payload)

    async def watch_pods(
        self, resource_version: Optional[str] = None
    ) -> AsyncIterator[PodWatchEvent]:
        params = {"watch": "true", "allowWatchBookmarks": "true"}
        if resource_version:
            params["resourceVersion"] = resource_version
        assert self._client, "client is not initialized"
        timeout = ClientTimeout(
            connect=self._conn_timeout_s,
            total=self._watch_timeout_s,
        )
        async with self._client.request(
            method="GET",
            url=self._all_pods_url,
            params=params,
            headers=self._create_headers(),
            timeout=timeout,
        ) as response:
            if response.status == 410:
                raise ResourceGone
            try:
                async for line in response.content:
                    payload = json.loads(line)

                    self._raise_for_status(payload)
                    if PodWatchEvent.is_error(payload):
                        self._raise_for_status(payload["object"])

                    yield PodWatchEvent.from_primitive(payload)
            except asyncio.TimeoutError:
                pass

    async def get_pvc_volumes_metrics(self) -> AsyncIterator[PVCVolumeMetrics]:
        # Get list of all nodes
        nodes_url = f"{self.api_v1_url}/nodes"
        payload = await self.get(url=nodes_url)
        nodes_list = payload.get("items", [])
        for node in nodes_list:
            # Check stats for each node
            node_name = node["metadata"]["name"]
            node_summary_url = f"{nodes_url}/{node_name}/proxy/stats/summary"
            try:
                # not self._request since response has a different structure
                # (does not contain `status` field)
                assert self._client
                async with self._client.request(
                    method="GET", url=node_summary_url, headers=self._create_headers()
                ) as resp:
                    payload = await resp.json()
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
                            namespace=pod["podRef"]["namespace"],
                            pvc_name=volume["pvcRef"]["name"],
                            used_bytes=volume["usedBytes"],
                        )
                    except KeyError:
                        pass

    async def create_disk_naming(self, disk_naming: DiskNaming) -> None:
        url = self._generate_disk_naming_url(disk_naming.namespace)
        await self.post(url=url, json=disk_naming.to_primitive())

    async def list_disk_namings(self) -> list[DiskNaming]:
        url = self._generate_disk_naming_url()
        try:
            payload = await self.get(url=url)
        except aiohttp.client.ClientError:
            return []
        return [DiskNaming.from_primitive(item) for item in payload.get("items", [])]

    async def get_disk_naming(self, namespace: str, name: str) -> DiskNaming:
        url = self._generate_disk_naming_url(namespace=namespace, name=name)
        payload = await self.get(url=url)
        return DiskNaming.from_primitive(payload)

    async def remove_disk_naming(self, namespace: str, name: str) -> None:
        url = self._generate_disk_naming_url(namespace, name)
        await self.delete(url=url)
