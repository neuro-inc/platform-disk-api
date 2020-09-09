import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List
from uuid import uuid4

from .kube_client import (
    KubeClient,
    MergeDiff,
    PersistentVolumeClaimRead,
    PersistentVolumeClaimWrite,
    ResourceNotFound,
)


class DiskNotFound(Exception):
    pass


logger = logging.getLogger()


USER_LABEL = "platform.neuromation.io/user"
DISK_API_MARK_LABEL = "platform.neuromation.io/disk-api-pvc"
DISK_API_DELETED_LABEL = "platform.neuromation.io/disk-api-pvc-deleted"


@dataclass(frozen=True)
class DiskRequest:
    storage: int  # In bytes


@dataclass(frozen=True)
class Disk:
    id: str
    storage: int  # In bytes
    owner: str
    status: "Disk.Status"

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

    def _request_to_pvc(
        self, request: DiskRequest, labels: Dict[str, str]
    ) -> PersistentVolumeClaimWrite:
        return PersistentVolumeClaimWrite(
            name=f"disk-{uuid4()}",
            storage=request.storage,
            storage_class_name=self._storage_class_name,
            labels=labels,
        )

    def _pvc_to_disk(self, pvc: PersistentVolumeClaimRead) -> Disk:
        status_map = {
            PersistentVolumeClaimRead.Phase.PENDING: Disk.Status.PENDING,
            PersistentVolumeClaimRead.Phase.BOUND: Disk.Status.READY,
            PersistentVolumeClaimRead.Phase.LOST: Disk.Status.BROKEN,
        }
        return Disk(
            id=pvc.name,
            storage=pvc.storage_real
            if pvc.storage_real is not None
            else pvc.storage_requested,
            status=status_map[pvc.phase],
            owner=pvc.labels[USER_LABEL],
        )

    async def create_disk(self, request: DiskRequest, username: str) -> Disk:
        pvc_write = self._request_to_pvc(
            request, labels={USER_LABEL: username, DISK_API_MARK_LABEL: "true"}
        )
        pvc_read = await self._kube_client.create_pvc(pvc_write)
        return self._pvc_to_disk(pvc_read)

    async def get_disk(self, disk_id: str) -> Disk:
        try:
            pvc = await self._kube_client.get_pvc(disk_id)
        except ResourceNotFound:
            raise DiskNotFound
        return self._pvc_to_disk(pvc)

    async def get_all_disks(self) -> List[Disk]:
        return [
            self._pvc_to_disk(pvc)
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
