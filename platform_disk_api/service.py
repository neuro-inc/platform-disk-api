import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import TypeVar
from uuid import uuid4

from apolo_kube_client import (
    KubeClientException,
    KubeClientSelector,
    PatchAdd,
    ResourceNotFound,
    V1ObjectMeta,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1VolumeResourceRequirements,
    escape_json_pointer,
)
from apolo_kube_client.apolo import (
    generate_namespace_name,
)

from .utils import (
    _storage_str_to_int,
    datetime_dump,
    datetime_load,
    timedelta_dump,
    timedelta_load,
    utc_now,
)


class DiskServiceError(Exception):
    pass


class DiskNotFound(DiskServiceError):
    pass


class DiskConflict(DiskServiceError):
    pass


class DiskNameUsed(DiskConflict):
    pass


class DiskAlreadyInUse(DiskConflict):
    pass


logger = logging.getLogger()

DISK_API_ORG_LABEL = "platform.neuromation.io/disk-api-org-name"
APOLO_ORG_LABEL = "platform.apolo.us/org"
DISK_API_PROJECT_LABEL = "platform.neuromation.io/project"
APOLO_PROJECT_LABEL = "platform.apolo.us/project"
USER_LABEL = "platform.neuromation.io/user"
APOLO_USER_LABEL = "platform.apolo.us/user"
DISK_API_MARK_LABEL = "platform.neuromation.io/disk-api-pvc"
APOLO_DISK_API_MARK_LABEL = "platform.apolo.us/disk"
DISK_API_DELETED_LABEL = "platform.neuromation.io/disk-api-pvc-deleted"
APOLO_DISK_API_DELETED_LABEL = "platform.apolo.us/disk-deleted"

DISK_API_NAME_ANNOTATION = "platform.neuromation.io/disk-api-pvc-name"
APOLO_DISK_API_NAME_ANNOTATION = "platform.apolo.us/disk-name"
DISK_API_CREATED_AT_ANNOTATION = "platform.neuromation.io/disk-api-pvc-created-at"
APOLO_DISK_API_CREATED_AT_ANNOTATION = "platform.apolo.us/disk-creation-date"
DISK_API_LAST_USAGE_ANNOTATION = "platform.neuromation.io/disk-api-pvc-last-usage"
APOLO_DISK_API_LAST_USAGE_ANNOTATION = "platform.apolo.us/disk-last-usage-date"
DISK_API_LIFE_SPAN_ANNOTATION = "platform.neuromation.io/disk-api-pvc-life-span"
APOLO_DISK_API_LIFE_SPAN_ANNOTATION = "platform.apolo.us/disk-life-span"
DISK_API_USED_BYTES_ANNOTATION = "platform.neuromation.io/disk-api-used-bytes"
APOLO_DISK_API_USED_BYTES_ANNOTATION = "platform.apolo.us/disk-bytes-used"

VCLUSTER_OBJECT_NAME_ANNOTATION = "vcluster.loft.sh/object-name"


@dataclass(frozen=True)
class DiskRequest:
    storage: int  # In bytes
    org_name: str
    project_name: str
    life_span: timedelta | None = None
    name: str | None = None


@dataclass(frozen=True)
class Disk:
    id: str
    storage: int  # In bytes
    owner: str
    project_name: str
    name: str | None
    org_name: str
    status: "Disk.Status"
    created_at: datetime
    last_usage: datetime | None
    life_span: timedelta | None
    used_bytes: int | None

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
    def __init__(
        self, kube_client_selector: KubeClientSelector, storage_class_name: str
    ) -> None:
        self._kube_client_selector = kube_client_selector
        self._storage_class_name = storage_class_name

    @staticmethod
    def get_disk_naming_name(
        name: str,
        org_name: str,
        project_name: str,
    ) -> str:
        """Get kubernetes resource name for a disk naming object."""
        return f"{name}--{org_name}--{project_name}"

    def _request_to_pvc(
        self, request: DiskRequest, username: str
    ) -> V1PersistentVolumeClaim:
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

        return V1PersistentVolumeClaim(
            kind="PersistentVolumeClaim",
            api_version="v1",
            metadata=V1ObjectMeta(
                name=f"disk-{uuid4()}", labels=labels, annotations=annotations
            ),
            spec=V1PersistentVolumeClaimSpec(
                access_modes=["ReadWriteOnce"],
                volume_mode="Filesystem",
                resources=V1VolumeResourceRequirements(
                    requests={"storage": str(request.storage)}
                ),
                storage_class_name=self._storage_class_name or None,
            ),
        )

    async def resolve_disk(
        self, disk_id_or_name: str, org_name: str, project_name: str
    ) -> Disk:
        try:
            return await self.get_disk(
                org_name=org_name,
                project_name=project_name,
                disk_id=disk_id_or_name,
            )
        except DiskNotFound:
            return await self.get_disk_by_name(
                name=disk_id_or_name,
                org_name=org_name,
                project_name=project_name,
            )

    async def _pvc_to_disk(self, pvc: V1PersistentVolumeClaim) -> Disk:
        status_map = {
            "Pending": Disk.Status.PENDING,
            "Bound": Disk.Status.READY,
            "Lost": Disk.Status.BROKEN,
        }

        annotations = pvc.metadata.annotations or {}
        labels = pvc.metadata.labels or {}
        storage_real = (
            pvc.status.capacity.get("storage")
            if pvc.status and pvc.status.capacity
            else None
        )
        storage_requested = (
            pvc.spec.resources.requests.get("storage")
            if pvc.spec.resources and pvc.spec.resources.requests
            else None
        )

        _T = TypeVar("_T")

        def _get_if_present(
            new_annotation: str, old_annotation: str, mapper: Callable[[str], _T]
        ) -> _T | None:
            if new_annotation in annotations:
                return mapper(annotations[new_annotation])
            if old_annotation in annotations:
                return mapper(annotations[old_annotation])
            return None

        username = labels.get(APOLO_USER_LABEL, labels.get(USER_LABEL, "")).replace(
            "--", "/"
        )
        last_usage = _get_if_present(
            APOLO_DISK_API_LAST_USAGE_ANNOTATION,
            DISK_API_LAST_USAGE_ANNOTATION,
            datetime_load,
        )
        life_span = _get_if_present(
            APOLO_DISK_API_LIFE_SPAN_ANNOTATION,
            DISK_API_LIFE_SPAN_ANNOTATION,
            timedelta_load,
        )
        used_bytes = _get_if_present(
            APOLO_DISK_API_USED_BYTES_ANNOTATION, DISK_API_USED_BYTES_ANNOTATION, int
        )

        org_name = labels.get(APOLO_ORG_LABEL, labels.get(DISK_API_ORG_LABEL, ""))
        project_name = labels.get(
            APOLO_PROJECT_LABEL, labels.get(DISK_API_PROJECT_LABEL, username)
        )
        disk_name = annotations.get(
            APOLO_DISK_API_NAME_ANNOTATION,
            annotations.get(DISK_API_NAME_ANNOTATION, None),
        )

        created_at = None
        if APOLO_DISK_API_CREATED_AT_ANNOTATION in annotations:
            created_at = datetime_load(
                annotations[APOLO_DISK_API_CREATED_AT_ANNOTATION]
            )
        elif DISK_API_CREATED_AT_ANNOTATION in annotations:
            created_at = datetime_load(annotations[DISK_API_CREATED_AT_ANNOTATION])

        storage_str = storage_real or storage_requested
        if not storage_str:
            raise DiskServiceError("PVC has no storage info")

        storage = _storage_str_to_int(storage_str)

        assert pvc.metadata.name is not None
        # in disks from vcluster accessed from host kube the name is mangled
        disk_id = annotations.get(VCLUSTER_OBJECT_NAME_ANNOTATION, pvc.metadata.name)

        assert pvc.status.phase is not None
        return Disk(
            id=disk_id,
            storage=storage,
            status=status_map[pvc.status.phase],
            owner=username,
            project_name=project_name,
            name=disk_name,
            org_name=org_name,
            created_at=created_at,  # type: ignore
            last_usage=last_usage,
            life_span=life_span,
            used_bytes=used_bytes,
        )

    async def create_disk(
        self,
        request: DiskRequest,
        username: str,
    ) -> Disk:
        async with self._kube_client_selector.get_client(
            org_name=request.org_name, project_name=request.project_name
        ) as kube_client:
            pvc = await kube_client.core_v1.persistent_volume_claim.create(
                model=self._request_to_pvc(request, username),
            )

        return await self._pvc_to_disk(pvc=pvc)

    async def get_disk(self, org_name: str, project_name: str, disk_id: str) -> Disk:
        async with self._kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            try:
                pvc = await kube_client.core_v1.persistent_volume_claim.get(
                    name=disk_id,
                )
            except ResourceNotFound:
                raise DiskNotFound from None
        return await self._pvc_to_disk(pvc)

    async def get_disk_by_name(
        self, name: str, org_name: str, project_name: str
    ) -> Disk:
        try:
            disk_naming_name = self.get_disk_naming_name(
                name,
                org_name=org_name,
                project_name=project_name,
            )
            async with self._kube_client_selector.get_client(
                org_name=org_name, project_name=project_name
            ) as kube_client:
                disk_naming = await kube_client.neuromation_io_v1.disk_naming.get(
                    name=disk_naming_name,
                )
            return await self.get_disk(org_name, project_name, disk_naming.spec.disk_id)
        except ResourceNotFound:
            logger.exception("get_disk_by_name: unhandled error")
            raise DiskNotFound from None

    async def get_project_disks(
        self,
        org_name: str,
        project_name: str,
        *,
        ensure_namespace: bool = True,
    ) -> list[Disk]:
        label_selectors = [
            f"{DISK_API_MARK_LABEL}=true",  # is apolo disk
            f"!{DISK_API_DELETED_LABEL}",  # not deleted
        ]

        async with self._kube_client_selector.get_client(
            org_name=org_name,
            project_name=project_name,
            ensure_namespace=ensure_namespace,
        ) as kube_client:
            try:
                pvc_list = await kube_client.core_v1.persistent_volume_claim.get_list(
                    label_selector=",".join(label_selectors)
                )
            except KubeClientException:
                return []
        return [await self._pvc_to_disk(pvc) for pvc in pvc_list.items]

    async def get_all_disks(
        self,
    ) -> list[Disk]:
        label_selectors = [
            f"{DISK_API_MARK_LABEL}=true",  # is apolo disk
            f"!{DISK_API_DELETED_LABEL}",  # not deleted
        ]
        kube_client = self._kube_client_selector.host_client
        pvc_list = await kube_client.core_v1.persistent_volume_claim.get_list(
            label_selector=",".join(label_selectors), all_namespaces=True
        )
        return [await self._pvc_to_disk(pvc) for pvc in pvc_list.items]

    async def remove_disk(self, disk: Disk, *, ensure_namespace: bool = True) -> None:
        try:
            async with self._kube_client_selector.get_client(
                org_name=disk.org_name,
                project_name=disk.project_name,
                ensure_namespace=ensure_namespace,
            ) as kube_client:
                if disk.name:
                    disk_naming_name = self.get_disk_naming_name(
                        disk.name,
                        org_name=disk.org_name,
                        project_name=disk.project_name,
                    )
                    try:
                        await kube_client.neuromation_io_v1.disk_naming.delete(
                            name=disk_naming_name,
                        )
                    except ResourceNotFound:
                        pass  # already removed

                patch_json_list = [
                    PatchAdd(
                        path=f"/metadata/labels/"
                        f"{escape_json_pointer(DISK_API_DELETED_LABEL)}",
                        value="true",
                    ),
                    PatchAdd(
                        path=f"/metadata/labels/"
                        f"{escape_json_pointer(APOLO_DISK_API_DELETED_LABEL)}",
                        value="true",
                    ),
                ]

                await kube_client.core_v1.persistent_volume_claim.patch_json(
                    name=disk.id,
                    patch_json_list=patch_json_list,
                )
                await kube_client.core_v1.persistent_volume_claim.delete(
                    name=disk.id,
                )
        except ResourceNotFound:
            raise DiskNotFound from None

    async def mark_disk_usage(
        self, namespace: str, disk_id: str, time: datetime
    ) -> None:
        time_dump = datetime_dump(time)

        patch_json_list = [
            PatchAdd(
                path=f"/metadata/annotations/"
                f"{escape_json_pointer(DISK_API_LAST_USAGE_ANNOTATION)}",
                value=time_dump,
            ),
            PatchAdd(
                path=f"/metadata/annotations/"
                f"{escape_json_pointer(APOLO_DISK_API_LAST_USAGE_ANNOTATION)}",
                value=time_dump,
            ),
        ]
        try:
            kube_client = self._kube_client_selector.host_client
            await kube_client.core_v1.persistent_volume_claim.patch_json(
                name=disk_id,
                patch_json_list=patch_json_list,
                namespace=namespace,
            )
        except ResourceNotFound:
            raise DiskNotFound from None

    async def update_disk_used_bytes(
        self, namespace: str, disk_id: str, used_bytes: int
    ) -> None:
        used_bytes_dump = str(used_bytes)

        patch_json_list = [
            PatchAdd(
                path=f"/metadata/annotations/"
                f"{escape_json_pointer(DISK_API_USED_BYTES_ANNOTATION)}",
                value=used_bytes_dump,
            ),
            PatchAdd(
                path=f"/metadata/annotations/"
                f"{escape_json_pointer(APOLO_DISK_API_USED_BYTES_ANNOTATION)}",
                value=used_bytes_dump,
            ),
        ]
        try:
            kube_client = self._kube_client_selector.host_client
            await kube_client.core_v1.persistent_volume_claim.patch_json(
                name=disk_id,
                patch_json_list=patch_json_list,
                namespace=namespace,
            )
        except ResourceNotFound:
            raise DiskNotFound from None
