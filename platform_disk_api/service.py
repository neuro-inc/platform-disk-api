import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import List, Optional
from uuid import uuid4

from .kube_client import (
    KubeClient,
    MergeDiff,
    PersistentVolumeClaimRead,
    PersistentVolumeClaimWrite,
    ResourceNotFound,
)
from .utils import datetime_dump, datetime_load, timedelta_dump, timedelta_load, utc_now


class DiskNotFound(Exception):
    pass


logger = logging.getLogger()


USER_LABEL = "platform.neuromation.io/user"
DISK_API_MARK_LABEL = "platform.neuromation.io/disk-api-pvc"
DISK_API_DELETED_LABEL = "platform.neuromation.io/disk-api-pvc-deleted"
DISK_API_CREATED_AT_ANNOTATION = "platform.neuromation.io/disk-api-pvc-created-at"
DISK_API_LAST_USAGE_ANNOTATION = "platform.neuromation.io/disk-api-pvc-last-usage"
DISK_API_LIFESPAN_ANNOTATION = "platform.neuromation.io/disk-api-pvc-lifespan"


@dataclass(frozen=True)
class DiskRequest:
    storage: int  # In bytes
    lifespan: Optional[timedelta] = None


@dataclass(frozen=True)
class Disk:
    id: str
    storage: int  # In bytes
    owner: str
    status: "Disk.Status"
    created_at: datetime
    last_usage: Optional[datetime]
    lifespan: timedelta

    class Status(str, Enum):
        PENDING = "Pending"
        READY = "Ready"
        BROKEN = "Broken"

        def __str__(self) -> str:
            return str(self.value)


class Service:
    def __init__(
        self,
        kube_client: KubeClient,
        storage_class_name: str,
        default_lifespan: timedelta,
    ) -> None:
        self._kube_client = kube_client
        self._storage_class_name = storage_class_name
        self._default_lifespan = default_lifespan

    def _request_to_pvc(
        self, request: DiskRequest, username: str
    ) -> PersistentVolumeClaimWrite:
        return PersistentVolumeClaimWrite(
            name=f"disk-{uuid4()}",
            storage=request.storage,
            storage_class_name=self._storage_class_name,
            labels={USER_LABEL: username, DISK_API_MARK_LABEL: "true"},
            annotations={
                DISK_API_CREATED_AT_ANNOTATION: datetime_dump(utc_now()),
                DISK_API_LIFESPAN_ANNOTATION: timedelta_dump(
                    request.lifespan or self._default_lifespan
                ),
            },
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
        if DISK_API_LIFESPAN_ANNOTATION not in pvc.annotations:
            # This is old pvc, created before we added timedelta field.
            diff = MergeDiff.make_add_annotations_diff(
                DISK_API_LIFESPAN_ANNOTATION, timedelta_dump(self._default_lifespan)
            )
            pvc = await self._kube_client.update_pvc(pvc.name, diff)

        last_usage_raw = pvc.annotations.get(DISK_API_LAST_USAGE_ANNOTATION)
        if last_usage_raw is not None:
            last_usage: Optional[datetime] = datetime_load(last_usage_raw)
        else:
            last_usage = None
        return Disk(
            id=pvc.name,
            storage=pvc.storage_real
            if pvc.storage_real is not None
            else pvc.storage_requested,
            status=status_map[pvc.phase],
            owner=pvc.labels[USER_LABEL],
            created_at=datetime_load(pvc.annotations[DISK_API_CREATED_AT_ANNOTATION]),
            last_usage=last_usage,
            lifespan=timedelta_load(pvc.annotations[DISK_API_LIFESPAN_ANNOTATION]),
        )

    async def create_disk(self, request: DiskRequest, username: str) -> Disk:
        pvc_write = self._request_to_pvc(request, username)
        pvc_read = await self._kube_client.create_pvc(pvc_write)
        return await self._pvc_to_disk(pvc_read)

    async def get_disk(self, disk_id: str) -> Disk:
        try:
            pvc = await self._kube_client.get_pvc(disk_id)
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
            diff = MergeDiff.make_add_label_diff(DISK_API_DELETED_LABEL, "true")
            await self._kube_client.update_pvc(disk_id, diff)
            await self._kube_client.remove_pvc(disk_id)
        except ResourceNotFound:
            raise DiskNotFound
