import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, List, Optional, TypeVar
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


USER_LABEL = "platform.neuromation.io/user"
DISK_API_MARK_LABEL = "platform.neuromation.io/disk-api-pvc"
DISK_API_DELETED_LABEL = "platform.neuromation.io/disk-api-pvc-deleted"
DISK_API_NAME_ANNOTATION = "platform.neuromation.io/disk-api-pvc-name"
DISK_API_CREATED_AT_ANNOTATION = "platform.neuromation.io/disk-api-pvc-created-at"
DISK_API_LAST_USAGE_ANNOTATION = "platform.neuromation.io/disk-api-pvc-last-usage"
DISK_API_LIFE_SPAN_ANNOTATION = "platform.neuromation.io/disk-api-pvc-life-span"
DISK_API_USED_BYTES_ANNOTATION = "platform.neuromation.io/disk-api-used-bytes"


@dataclass(frozen=True)
class DiskRequest:
    storage: int  # In bytes
    life_span: Optional[timedelta] = None
    name: Optional[str] = None


@dataclass(frozen=True)
class Disk:
    id: str
    storage: int  # In bytes
    owner: str
    name: Optional[str]
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


class Service:
    def __init__(self, kube_client: KubeClient, storage_class_name: str) -> None:
        self._kube_client = kube_client
        self._storage_class_name = storage_class_name

    def _get_disk_naming_name(self, name: str, owner: str) -> str:
        return f"{name}--{owner.replace('/', '--')}"

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

        return PersistentVolumeClaimWrite(
            name=f"disk-{uuid4()}",
            storage=request.storage,
            storage_class_name=self._storage_class_name,
            labels={
                USER_LABEL: username.replace("/", "--"),
                DISK_API_MARK_LABEL: "true",
            },
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
            pvc = await self._kube_client.update_pvc(pvc.name, diff)

        _T = TypeVar("_T")

        def _get_if_present(
            annotation: str, mapper: Callable[[str], _T]
        ) -> Optional[_T]:
            if annotation in pvc.annotations:
                return mapper(pvc.annotations[annotation])
            return None

        last_usage = _get_if_present(DISK_API_LAST_USAGE_ANNOTATION, datetime_load)
        life_span = _get_if_present(DISK_API_LIFE_SPAN_ANNOTATION, timedelta_load)
        used_bytes = _get_if_present(DISK_API_USED_BYTES_ANNOTATION, int)

        return Disk(
            id=pvc.name,
            storage=pvc.storage_real
            if pvc.storage_real is not None
            else pvc.storage_requested,
            status=status_map[pvc.phase],
            owner=pvc.labels[USER_LABEL].replace("--", "/"),
            name=pvc.annotations.get(DISK_API_NAME_ANNOTATION),
            created_at=datetime_load(pvc.annotations[DISK_API_CREATED_AT_ANNOTATION]),
            last_usage=last_usage,
            life_span=life_span,
            used_bytes=used_bytes,
        )

    async def create_disk(self, request: DiskRequest, username: str) -> Disk:
        pvc_write = self._request_to_pvc(request, username)
        if request.name:
            disk_naming = DiskNaming(
                name=self._get_disk_naming_name(request.name, username),
                disk_id=pvc_write.name,
            )
            try:
                await self._kube_client.create_disk_naming(disk_naming)
            except ResourceExists:
                raise DiskNameUsed(
                    f"Disk with name {request.name} already"
                    f"exists for user {username}"
                )
        try:
            pvc_read = await self._kube_client.create_pvc(pvc_write)
        except Exception:
            if request.name:
                await self._kube_client.remove_disk_naming(
                    self._get_disk_naming_name(request.name, username)
                )
            raise
        return await self._pvc_to_disk(pvc_read)

    async def get_disk(self, disk_id: str) -> Disk:
        try:
            pvc = await self._kube_client.get_pvc(disk_id)
        except ResourceNotFound:
            raise DiskNotFound
        return await self._pvc_to_disk(pvc)

    async def get_disk_by_name(self, name: str, owner: str) -> Disk:
        try:
            disk_naming_name = self._get_disk_naming_name(name, owner)
            disk_naming = await self._kube_client.get_disk_naming(disk_naming_name)
            pvc = await self._kube_client.get_pvc(disk_naming.disk_id)
        except ResourceNotFound:
            raise DiskNotFound
        return await self._pvc_to_disk(pvc)

    async def get_all_disks(self) -> List[Disk]:
        return [
            await self._pvc_to_disk(pvc)
            for pvc in await self._kube_client.list_pvc()
            if pvc.labels.get(DISK_API_MARK_LABEL, False)
            and not pvc.labels.get(DISK_API_DELETED_LABEL, False)
        ]

    async def remove_disk(self, disk_id: str) -> None:
        try:
            disk = await self.get_disk(disk_id)
            if disk.name:
                disk_naming_name = self._get_disk_naming_name(disk.name, disk.owner)
                await self._kube_client.remove_disk_naming(disk_naming_name)
            diff = MergeDiff.make_add_label_diff(DISK_API_DELETED_LABEL, "true")
            await self._kube_client.update_pvc(disk_id, diff)
            await self._kube_client.remove_pvc(disk_id)
        except ResourceNotFound:
            raise DiskNotFound

    async def mark_disk_usage(self, disk_id: str, time: datetime) -> None:
        diff = MergeDiff.make_add_annotations_diff(
            DISK_API_LAST_USAGE_ANNOTATION, datetime_dump(time)
        )
        try:
            await self._kube_client.update_pvc(disk_id, diff)
        except ResourceNotFound:
            raise DiskNotFound

    async def update_disk_used_bytes(self, disk_id: str, used_bytes: int) -> None:
        diff = MergeDiff.make_add_annotations_diff(
            DISK_API_USED_BYTES_ANNOTATION, str(used_bytes)
        )
        try:
            await self._kube_client.update_pvc(disk_id, diff)
        except ResourceNotFound:
            raise DiskNotFound
