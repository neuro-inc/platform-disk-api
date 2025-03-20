import hashlib
import logging
import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, TypeVar
from uuid import uuid4

from .kube_client import (
    DiskNaming,
    KubeClient,
    MergeDiff,
    PersistentVolumeClaimRead,
    PersistentVolumeClaimWrite,
    ResourceExists,
    ResourceNotFound,
)
from .utils import datetime_dump, datetime_load, timedelta_dump, timedelta_load, utc_now


class DiskNotFound(Exception):
    pass


class DiskNameUsed(Exception):
    pass


logger = logging.getLogger()

ORG_LABEL = "platform.apolo.us/org"
USER_LABEL = "platform.neuromation.io/user"
PROJECT_LABEL = "platform.neuromation.io/project"
DISK_API_MARK_LABEL = "platform.neuromation.io/disk-api-pvc"
DISK_API_DELETED_LABEL = "platform.neuromation.io/disk-api-pvc-deleted"
DISK_API_ORG_LABEL = "platform.neuromation.io/disk-api-org-name"
DISK_API_NAME_ANNOTATION = "platform.neuromation.io/disk-api-pvc-name"
DISK_API_CREATED_AT_ANNOTATION = "platform.neuromation.io/disk-api-pvc-created-at"
DISK_API_LAST_USAGE_ANNOTATION = "platform.neuromation.io/disk-api-pvc-last-usage"
DISK_API_LIFE_SPAN_ANNOTATION = "platform.neuromation.io/disk-api-pvc-life-span"
DISK_API_USED_BYTES_ANNOTATION = "platform.neuromation.io/disk-api-used-bytes"

NO_ORG = "NO_ORG"
NO_ORG_NORMALIZED = "no-org"

KUBE_NAME_LENGTH_MAX = 63
KUBE_NAMESPACE_SEP = "--"
KUBE_NAMESPACE_PREFIX = "platform"
KUBE_NAMESPACE_HASH_LENGTH = 24


def generate_namespace_name(org_name: str, project_name: str) -> str:
    """
    returns a Kubernetes resource name in the format
    `platform--<org_name>--<project_name>--<hash>`,
    ensuring that the total length does not exceed `KUBE_NAME_LENGTH_MAX` characters.

    - `platform--` prefix is never truncated
    - `<hash>` (a sha256 truncated to 24 chars), is also never truncated
    - if the names are long, we truncate them evenly,
      so at least some parts of both org and proj names will remain
    """
    if org_name == NO_ORG:
        org_name = NO_ORG_NORMALIZED

    hashable = f"{org_name}{KUBE_NAMESPACE_SEP}{project_name}"
    name_hash = (
        hashlib
        .sha256(hashable.encode("utf-8"))
        .hexdigest()
        [:KUBE_NAMESPACE_HASH_LENGTH]
    )

    len_reserved = (
        len(KUBE_NAMESPACE_PREFIX)
        + (len(KUBE_NAMESPACE_SEP) * 2)
        + KUBE_NAMESPACE_HASH_LENGTH
    )
    len_free = KUBE_NAME_LENGTH_MAX - len_reserved
    if len(hashable) <= len_free:
        return (
            f"{KUBE_NAMESPACE_PREFIX}"
            f"{KUBE_NAMESPACE_SEP}"
            f"{hashable}"
            f"{KUBE_NAMESPACE_SEP}"
            f"{name_hash}"
        )

    # org and project names do not fit into a full length.
    # let's figure out the full length of org and proj, and calculate a ratio
    # between org and project, so that we'll truncate more chars from the
    # string which actually has more chars
    len_org, len_proj = len(org_name), len(project_name)
    len_org_proj = len_org + len_proj + len(KUBE_NAMESPACE_SEP)
    exceeds = len_org_proj - len_free

    # ratio calculation. for proj can be derived via an org ratio
    remove_from_org = math.ceil((len_org / len_org_proj) * exceeds)
    remove_from_proj = exceeds - remove_from_org

    new_org_name = org_name[: max(1, len_org - remove_from_org)]
    new_project_name = project_name[: max(1, len_proj - remove_from_proj)]

    return (
        f"{KUBE_NAMESPACE_PREFIX}"
        f"{KUBE_NAMESPACE_SEP}"
        f"{new_org_name}"
        f"{KUBE_NAMESPACE_SEP}"
        f"{new_project_name}"
        f"{KUBE_NAMESPACE_SEP}"
        f"{name_hash}"
    )


@dataclass(frozen=True)
class DiskRequest:
    storage: int  # In bytes
    org_name: str
    project_name: str
    life_span: Optional[timedelta] = None
    name: Optional[str] = None


@dataclass(frozen=True)
class Disk:
    id: str
    storage: int  # In bytes
    owner: str
    project_name: str
    name: Optional[str]
    org_name: str
    status: "Disk.Status"
    created_at: datetime
    last_usage: Optional[datetime]
    life_span: Optional[timedelta]
    used_bytes: Optional[int]

    class Status(str, Enum):
        PENDING = "Pending"
        READY = "Ready"
        BROKEN = "Broken"

        def __str__(self) -> str:
            return str(self.value)

    @property
    def namespace(self) -> str:
        return generate_namespace_name(self.org_name, self.project_name)


class Service:
    def __init__(self, kube_client: KubeClient, storage_class_name: str) -> None:
        self._kube_client = kube_client
        self._storage_class_name = storage_class_name

    @staticmethod
    def _get_disk_naming_name(
        name: str,
        org_name: str,
        project_name: str,
    ) -> str:
        """Get kubernetes resource name for a disk naming object.
        """
        return f"{name}--{org_name}--{project_name}"

    def _request_to_pvc(
        self, request: DiskRequest, username: str
    ) -> PersistentVolumeClaimWrite:
        annotations = {
            DISK_API_CREATED_AT_ANNOTATION: datetime_dump(utc_now()),
        }
        if request.life_span:
            annotations[DISK_API_LIFE_SPAN_ANNOTATION] = timedelta_dump(
                request.life_span
            )
        if request.name:
            annotations[DISK_API_NAME_ANNOTATION] = request.name
        labels = {
            USER_LABEL: username.replace("/", "--"),
            DISK_API_MARK_LABEL: "true",
            DISK_API_ORG_LABEL: request.org_name,
        }
        if request.project_name != username:
            labels[PROJECT_LABEL] = request.project_name

        return PersistentVolumeClaimWrite(
            name=f"disk-{uuid4()}",
            storage=request.storage,
            storage_class_name=self._storage_class_name,
            labels=labels,
            annotations=annotations,
        )

    async def _pvc_to_disk(self, pvc: PersistentVolumeClaimRead) -> Disk:
        status_map = {
            PersistentVolumeClaimRead.Phase.PENDING: Disk.Status.PENDING,
            PersistentVolumeClaimRead.Phase.BOUND: Disk.Status.READY,
            PersistentVolumeClaimRead.Phase.LOST: Disk.Status.BROKEN,
        }
        if DISK_API_CREATED_AT_ANNOTATION not in pvc.annotations:
            # This is old pvc, created before we added created_at field.
            diff = MergeDiff.make_add_annotations_diff(
                DISK_API_CREATED_AT_ANNOTATION, datetime_dump(utc_now())
            )
            pvc = await self._kube_client.update_pvc(pvc.namespace, pvc.name, diff)

        _T = TypeVar("_T")

        def _get_if_present(
            annotation: str, mapper: Callable[[str], _T]
        ) -> Optional[_T]:
            if annotation in pvc.annotations:
                return mapper(pvc.annotations[annotation])
            return None

        username = pvc.labels[USER_LABEL].replace("--", "/")
        last_usage = _get_if_present(DISK_API_LAST_USAGE_ANNOTATION, datetime_load)
        life_span = _get_if_present(DISK_API_LIFE_SPAN_ANNOTATION, timedelta_load)
        used_bytes = _get_if_present(DISK_API_USED_BYTES_ANNOTATION, int)

        return Disk(
            id=pvc.name,
            storage=(
                pvc.storage_real
                if pvc.storage_real is not None
                else pvc.storage_requested
            ),
            status=status_map[pvc.phase],
            owner=username,
            project_name=pvc.labels.get(PROJECT_LABEL, username),
            name=pvc.annotations.get(DISK_API_NAME_ANNOTATION),
            org_name=pvc.labels[DISK_API_ORG_LABEL],
            created_at=datetime_load(pvc.annotations[DISK_API_CREATED_AT_ANNOTATION]),
            last_usage=last_usage,
            life_span=life_span,
            used_bytes=used_bytes,
        )

    def _get_org_project_labels(
        self,
        org_name: str,
        project_name: str
    ) -> dict[str, str]:
        return {
            ORG_LABEL: org_name,
            PROJECT_LABEL: project_name,
        }

    async def get_or_create_namespace(self, org_name: str, project_name: str) -> str:
        namespace = generate_namespace_name(org_name, project_name)
        labels = self._get_org_project_labels(org_name, project_name)
        try:
            await self._kube_client.create_namespace(namespace, labels=labels)
        except ResourceExists:
            pass
        return namespace

    async def create_disk(self, request: DiskRequest, username: str) -> Disk:
        namespace = await self.get_or_create_namespace(
            request.org_name, request.project_name
        )
        pvc_write = self._request_to_pvc(request, username)
        disk_name: Optional[str] = None

        if request.name:
            disk_name = self._get_disk_naming_name(
                request.name,
                org_name=request.org_name,
                project_name=request.project_name,
            )
            disk_naming = DiskNaming(name=disk_name, disk_id=pvc_write.name)
            try:
                await self._kube_client.create_disk_naming(namespace, disk_naming)
            except ResourceExists:
                raise DiskNameUsed(
                    f"Disk with name {request.name} already"
                    f"exists for user {username}"
                )
        try:
            pvc_read = await self._kube_client.create_pvc(namespace, pvc_write)
        except Exception:
            if disk_name:
                await self._kube_client.remove_disk_naming(namespace, disk_name)
            raise
        return await self._pvc_to_disk(pvc_read)

    async def get_disk(self, namespace: str, disk_id: str) -> Disk:
        try:
            pvc = await self._kube_client.get_pvc(namespace, disk_id)
        except ResourceNotFound:
            raise DiskNotFound
        return await self._pvc_to_disk(pvc)

    async def get_disk_by_name(
        self,
        namespace: str,
        name: str,
        org_name: str,
        project_name: str
    ) -> Disk:
        try:
            disk_naming_name = self._get_disk_naming_name(
                name,
                org_name=org_name,
                project_name=project_name,
            )
            disk_naming = await self._kube_client.get_disk_naming(disk_naming_name)
            pvc = await self._kube_client.get_pvc(namespace, disk_naming.disk_id)
            return await self._pvc_to_disk(pvc)
        except ResourceNotFound:
            pass
        raise DiskNotFound

    async def get_all_disks(
        self,
        org_name: Optional[str] = None,
        project_name: Optional[str] = None,
    ) -> list[Disk]:
        namespace = None
        if org_name and project_name:
            namespace = await self.get_or_create_namespace(org_name, project_name)

        label_selector = f"{DISK_API_ORG_LABEL}={org_name}"
        disks = []
        for pvc in await self._kube_client.list_pvc(namespace, label_selector):
            if not pvc.labels.get(DISK_API_MARK_LABEL, False) or pvc.labels.get(
                DISK_API_DELETED_LABEL, False
            ):
                continue
            if project_name:
                disk_project_name = pvc.labels.get(PROJECT_LABEL) or pvc.labels[
                    USER_LABEL
                ].replace("--", "/")
                if project_name != disk_project_name:
                    continue
            disks.append(await self._pvc_to_disk(pvc))
        return disks

    async def remove_disk(self, disk: Disk) -> None:
        namespace = disk.namespace
        try:
            if disk.name:
                disk_naming_name = self._get_disk_naming_name(
                    disk.name,
                    org_name=disk.org_name,
                    project_name=disk.project_name,
                )
                try:
                    await self._kube_client.remove_disk_naming(
                        namespace, disk_naming_name)
                except ResourceNotFound:
                    pass  # already removed
            diff = MergeDiff.make_add_label_diff(DISK_API_DELETED_LABEL, "true")
            await self._kube_client.update_pvc(namespace, disk.id, diff)
            await self._kube_client.remove_pvc(namespace, disk.id)
        except ResourceNotFound:
            raise DiskNotFound

    async def mark_disk_usage(
        self,
        namespace: str,
        disk_id: str,
        time: datetime
    ) -> None:
        diff = MergeDiff.make_add_annotations_diff(
            DISK_API_LAST_USAGE_ANNOTATION, datetime_dump(time)
        )
        try:
            await self._kube_client.update_pvc(namespace, disk_id, diff)
        except ResourceNotFound:
            raise DiskNotFound

    async def update_disk_used_bytes(
        self,
        namespace: str,
        disk_id: str,
        used_bytes: int
    ) -> None:
        diff = MergeDiff.make_add_annotations_diff(
            DISK_API_USED_BYTES_ANNOTATION, str(used_bytes)
        )
        try:
            await self._kube_client.update_pvc(namespace, disk_id, diff)
        except ResourceNotFound:
            raise DiskNotFound
