import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, TypeVar
from uuid import uuid4

from apolo_kube_client.apolo import create_namespace, generate_namespace_name, NO_ORG, \
    normalize_name
from apolo_kube_client.errors import ResourceExists, ResourceNotFound

from .kube_client import (
    DiskNaming,
    KubeClient,
    MergeDiff,
    PersistentVolumeClaimRead,
    PersistentVolumeClaimWrite,
)
from .utils import datetime_dump, datetime_load, timedelta_dump, timedelta_load, utc_now


class DiskNotFound(Exception):
    pass


class DiskNameUsed(Exception):
    pass


logger = logging.getLogger()

DISK_API_ORG_LABEL = "platform.neuromation.io/disk-api-org-name"
APOLO_ORG_LABEL = "platform.apolo.us/org"
DISK_API_PROJECT_LABEL = "platform.neuromation.io/project"
APOLO_PROJECT_LABEL = "platform.apolo.us/project"
USER_LABEL = "platform.neuromation.io/user"
APOLO_USER_LABEL = "platform.apolo.us/user"
DISK_API_MARK_LABEL = "platform.neuromation.io/disk-api-pvc"
APOLO_DISK_API_MARK_LABEL = "platform.apolo.us/disk-api-pvc"
DISK_API_DELETED_LABEL = "platform.neuromation.io/disk-api-pvc-deleted"
APOLO_DISK_API_DELETED_LABEL = "platform.apolo.us/disk-api-pvc-deleted"
DISK_API_NAME_ANNOTATION = "platform.neuromation.io/disk-api-pvc-name"
APOLO_DISK_API_NAME_ANNOTATION = "platform.apolo.us/disk-api-pvc-name"
DISK_API_CREATED_AT_ANNOTATION = "platform.neuromation.io/disk-api-pvc-created-at"
APOLO_DISK_API_CREATED_AT_ANNOTATION = "platform.apolo.us/disk-api-pvc-created-at"
DISK_API_LAST_USAGE_ANNOTATION = "platform.neuromation.io/disk-api-pvc-last-usage"
APOLO_DISK_API_LAST_USAGE_ANNOTATION = "platform.apolo.us/disk-api-pvc-last-usage"
DISK_API_LIFE_SPAN_ANNOTATION = "platform.neuromation.io/disk-api-pvc-life-span"
APOLO_DISK_API_LIFE_SPAN_ANNOTATION = "platform.apolo.us/disk-api-pvc-life-span"
DISK_API_USED_BYTES_ANNOTATION = "platform.neuromation.io/disk-api-used-bytes"
APOLO_DISK_API_USED_BYTES_ANNOTATION = "platform.apolo.us/disk-api-used-bytes"


def is_no_org(org_name: Optional[str]) -> bool:
    return (
        org_name is None
        or org_name == NO_ORG
        or org_name == normalize_name(NO_ORG)
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

    @property
    def has_org(self) -> bool:
        return not is_no_org(self.org_name)


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
        self,
        request: DiskRequest,
        username: str
    ) -> PersistentVolumeClaimWrite:
        now = datetime_dump(utc_now())
        annotations = {
            DISK_API_CREATED_AT_ANNOTATION: now,
            APOLO_DISK_API_CREATED_AT_ANNOTATION: now,
        }
        if request.life_span:
            lifespan = timedelta_dump(request.life_span)
            annotations[DISK_API_LIFE_SPAN_ANNOTATION] = lifespan
            annotations[APOLO_DISK_API_LIFE_SPAN_ANNOTATION] = lifespan
        if request.name:
            annotations[DISK_API_NAME_ANNOTATION] = request.name
            annotations[APOLO_DISK_API_NAME_ANNOTATION] = request.name

        kube_valid_username = username.replace("/", "--")
        labels = {
            USER_LABEL: kube_valid_username,
            APOLO_USER_LABEL: kube_valid_username,
            DISK_API_MARK_LABEL: "true",
            APOLO_DISK_API_MARK_LABEL: "true",
            DISK_API_ORG_LABEL: request.org_name,
            APOLO_ORG_LABEL: request.org_name,
            DISK_API_PROJECT_LABEL: request.project_name,
            APOLO_PROJECT_LABEL: request.project_name,
        }

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
            now = datetime_dump(utc_now())
            # This is old pvc, created before we added created_at field.
            diff = MergeDiff.make_add_annotations_diff({
                DISK_API_CREATED_AT_ANNOTATION: now,
                APOLO_DISK_API_CREATED_AT_ANNOTATION: now,
            })
            pvc = await self._kube_client.update_pvc(pvc.namespace, pvc.name, diff)

        _T = TypeVar("_T")

        def _get_if_present(
            new_annotation: str,
            old_annotation: str,
            mapper: Callable[[str], _T]
        ) -> Optional[_T]:
            if new_annotation in pvc.annotations:
                return mapper(pvc.annotations[new_annotation])
            if old_annotation in pvc.annotations:
                return mapper(pvc.annotations[old_annotation])
            return None

        username = pvc.labels.get(
            APOLO_USER_LABEL,
            pvc.labels[USER_LABEL]
        ).replace("--", "/")
        last_usage = _get_if_present(
            APOLO_DISK_API_LAST_USAGE_ANNOTATION,
            DISK_API_LAST_USAGE_ANNOTATION,
            datetime_load
        )
        life_span = _get_if_present(
            APOLO_DISK_API_LIFE_SPAN_ANNOTATION,
            DISK_API_LIFE_SPAN_ANNOTATION,
            timedelta_load
        )
        used_bytes = _get_if_present(
            APOLO_DISK_API_USED_BYTES_ANNOTATION,
            DISK_API_USED_BYTES_ANNOTATION,
            int
        )

        org_name = pvc.labels.get(APOLO_ORG_LABEL, pvc.labels[DISK_API_ORG_LABEL])
        project_name = pvc.labels.get(
            APOLO_PROJECT_LABEL,
            pvc.labels.get(DISK_API_PROJECT_LABEL, username)
        )
        disk_name = pvc.annotations.get(
            APOLO_DISK_API_NAME_ANNOTATION,
            pvc.annotations.get(DISK_API_NAME_ANNOTATION)
        )
        created_at = datetime_load(
            pvc.annotations.get(
                APOLO_DISK_API_CREATED_AT_ANNOTATION,
                pvc.annotations[DISK_API_CREATED_AT_ANNOTATION],
            )
        )
        return Disk(
            id=pvc.name,
            storage=(
                pvc.storage_real
                if pvc.storage_real is not None
                else pvc.storage_requested
            ),
            status=status_map[pvc.phase],
            owner=username,
            project_name=project_name,
            name=disk_name,
            org_name=org_name,
            created_at=created_at,
            last_usage=last_usage,
            life_span=life_span,
            used_bytes=used_bytes,
        )

    async def create_disk(
        self,
        request: DiskRequest,
        username: str,
    ) -> Disk:
        namespace = await create_namespace(
            self._kube_client,
            request.org_name,
            request.project_name,
        )
        pvc_write = self._request_to_pvc(request, username)
        disk_name: Optional[str] = None

        if request.name:
            disk_name = self._get_disk_naming_name(
                request.name,
                org_name=request.org_name,
                project_name=request.project_name,
            )
            disk_naming = DiskNaming(
                namespace.name, name=disk_name, disk_id=pvc_write.name)
            try:
                await self._kube_client.create_disk_naming(disk_naming)
            except ResourceExists:
                raise DiskNameUsed(
                    f"Disk with name {request.name} already"
                    f"exists for user {username}"
                )
        try:
            pvc_read = await self._kube_client.create_pvc(namespace.name, pvc_write)
        except Exception:
            if disk_name:
                await self._kube_client.remove_disk_naming(namespace.name, disk_name)
            raise
        return await self._pvc_to_disk(pvc_read)

    async def get_disk(self, org_name: str, project_name: str, disk_id: str) -> Disk:
        namespace = generate_namespace_name(org_name, project_name)
        try:
            pvc = await self._kube_client.get_pvc(namespace, disk_id)
        except ResourceNotFound:
            raise DiskNotFound
        return await self._pvc_to_disk(pvc)

    async def get_disk_by_name(
        self,
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
            namespace = generate_namespace_name(org_name, project_name)
            disk_naming = await self._kube_client.get_disk_naming(
                namespace=namespace,
                name=disk_naming_name,
            )
            return await self.get_disk(org_name, project_name, disk_naming.disk_id)
        except ResourceNotFound:
            logger.exception("get_disk_by_name: unhandled error")
            raise DiskNotFound

    async def get_all_disks(
        self,
        org_name: Optional[str] = None,
        project_name: Optional[str] = None,
    ) -> list[Disk]:
        namespace = None
        label_selectors = [
            f"{DISK_API_MARK_LABEL}=true",  # is apolo disk
            f"!{DISK_API_DELETED_LABEL}",  # not deleted
        ]
        if project_name:
            # request is in the scope of org/project.
            # let's figure out the org and enrich with labels and namespace
            org_name = org_name or normalize_name(NO_ORG)
            namespace = generate_namespace_name(org_name, project_name)

        label_selector = ",".join(label_selectors)

        disks = []
        for pvc in await self._kube_client.list_pvc(namespace, label_selector):
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
            diff = MergeDiff.make_add_label_diff({
                DISK_API_DELETED_LABEL: "true", APOLO_DISK_API_DELETED_LABEL: "true"
            })
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
        time_dump = datetime_dump(time)
        diff = MergeDiff.make_add_annotations_diff({
            DISK_API_LAST_USAGE_ANNOTATION: time_dump,
            APOLO_DISK_API_LAST_USAGE_ANNOTATION: time_dump,
        })
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
        used_bytes_dump = str(used_bytes)

        diff = MergeDiff.make_add_annotations_diff({
            DISK_API_USED_BYTES_ANNOTATION: used_bytes_dump,
            APOLO_DISK_API_USED_BYTES_ANNOTATION: used_bytes_dump,
        })
        try:
            await self._kube_client.update_pvc(namespace, disk_id, diff)
        except ResourceNotFound:
            raise DiskNotFound
