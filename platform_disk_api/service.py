import logging
from dataclasses import dataclass
from enum import Enum
from typing import List

from .kube_client import (
    KubeClient,
    PersistentVolumeClaimRead,
    PersistentVolumeClaimWrite,
    ResourceExists,
)


logger = logging.getLogger()


@dataclass(frozen=True)
class DiskRequest:
    name: str
    storage: int  # In bytes


@dataclass(frozen=True)
class Disk:
    name: str
    storage: int  # In bytes
    status: "Disk.Status"

    class Status(Enum):
        PENDING = "Pending"
        READY = "Ready"
        BROKEN = "Broken"


class Service:
    def __init__(self, kube_client: KubeClient, storage_class_name: str) -> None:
        self._kube_client = kube_client
        self._storage_class_name = storage_class_name

    def _request_to_pvc(self, request: DiskRequest) -> PersistentVolumeClaimWrite:
        return PersistentVolumeClaimWrite(
            name=request.name,
            storage=request.storage,
            storage_class_name=self._storage_class_name,
        )

    def _pvc_to_disk(self, pvc: PersistentVolumeClaimRead) -> Disk:
        status_map = {
            PersistentVolumeClaimRead.Phase.PENDING: Disk.Status.PENDING,
            PersistentVolumeClaimRead.Phase.BOUND: Disk.Status.READY,
            PersistentVolumeClaimRead.Phase.LOST: Disk.Status.BROKEN,
        }
        return Disk(
            name=pvc.name,
            storage=pvc.storage_real
            if pvc.storage_real is not None
            else pvc.storage_requested,
            status=status_map[pvc.phase],
        )

    async def create_disk(self, request: DiskRequest) -> Disk:
        pvc_write = self._request_to_pvc(request)
        try:
            pvc_read = await self._kube_client.create_pvc(pvc_write)
        except ResourceExists:
            raise ValueError(f'disk with name "{request.name}" already exists')
        return self._pvc_to_disk(pvc_read)

    async def get_all_disks(self) -> List[Disk]:
        return [self._pvc_to_disk(pvc) for pvc in await self._kube_client.list_pvc()]

    async def remove_disk(self, name: str) -> None:
        await self._kube_client.remove_pvc(name)
