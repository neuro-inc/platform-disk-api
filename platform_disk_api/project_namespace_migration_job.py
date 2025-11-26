from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Literal

from apolo_kube_client import (
    KubeClient,
    PatchAdd,
    ResourceNotFound,
    V1DiskNamingCRD,
    V1DiskNamingCRDMetadata,
    V1DiskNamingCRDSpec,
    V1ObjectMeta,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
)
from apolo_kube_client.apolo import NO_ORG, create_namespace, normalize_name
from neuro_logging import (
    init_logging,
)

from platform_disk_api.config import JobMigrateProjectNamespaceConfig
from platform_disk_api.config_factory import EnvironConfigFactory
from platform_disk_api.service import (
    APOLO_DISK_API_CREATED_AT_ANNOTATION,
    APOLO_DISK_API_LAST_USAGE_ANNOTATION,
    APOLO_DISK_API_LIFE_SPAN_ANNOTATION,
    APOLO_DISK_API_MARK_LABEL,
    APOLO_DISK_API_NAME_ANNOTATION,
    APOLO_DISK_API_USED_BYTES_ANNOTATION,
    APOLO_ORG_LABEL,
    APOLO_PROJECT_LABEL,
    APOLO_USER_LABEL,
    DISK_API_CREATED_AT_ANNOTATION,
    DISK_API_LAST_USAGE_ANNOTATION,
    DISK_API_LIFE_SPAN_ANNOTATION,
    DISK_API_MARK_LABEL,
    DISK_API_NAME_ANNOTATION,
    DISK_API_ORG_LABEL,
    DISK_API_PROJECT_LABEL,
    DISK_API_USED_BYTES_ANNOTATION,
    USER_LABEL,
)


logger = logging.getLogger(__name__)


CURRENT_NAMESPACE = "platform-jobs"


DISK_ANNOTATION_MAP = {
    DISK_API_NAME_ANNOTATION: APOLO_DISK_API_NAME_ANNOTATION,
    DISK_API_CREATED_AT_ANNOTATION: APOLO_DISK_API_CREATED_AT_ANNOTATION,
    DISK_API_LAST_USAGE_ANNOTATION: APOLO_DISK_API_LAST_USAGE_ANNOTATION,
    DISK_API_LIFE_SPAN_ANNOTATION: APOLO_DISK_API_LIFE_SPAN_ANNOTATION,
    DISK_API_USED_BYTES_ANNOTATION: APOLO_DISK_API_USED_BYTES_ANNOTATION,
}


class DiskMigrationError(Exception):
    pass


class PvcInUseError(DiskMigrationError):
    pass


async def migration_loop(
    config: JobMigrateProjectNamespaceConfig,
    disk_ids_filter: set[str] | None = None,
) -> None:
    """
    A main loop which iterates over all disks and tries to migrate them to a
    project-based namespaces.

    :param config: ...
    :param disk_ids_filter: optional filtering by disk IDs
    """
    async with KubeClient(config=config.kube) as kube_client:
        # get all disk namings
        disk_naming_list = await kube_client.neuromation_io_v1.disk_naming.get_list()
        pvc_to_disk_naming = {dn.spec.disk_id: dn for dn in disk_naming_list.items}

        pvc_list = await kube_client.core_v1.persistent_volume_claim.get_list(
            namespace=CURRENT_NAMESPACE, label_selector=f"{DISK_API_MARK_LABEL}=true"
        )

        for pvc in pvc_list.items:
            pvc_name = pvc.metadata.name
            assert pvc_name is not None
            if disk_ids_filter and pvc_name not in disk_ids_filter:
                continue

            # PV name (if bound).
            # sometimes users can create disks, but not mount them;
            # in such a case - PVC won't have a respective PV yet
            pv_name = pvc.spec.volume_name

            disk_naming = pvc_to_disk_naming.get(pvc_name)
            if not disk_naming:
                logger.error(
                    "PVC does not have a disk naming. Skipping",
                    extra={"pvc_name": pvc_name},
                )
                continue

            try:
                await migrate_disk(
                    kube_client,
                    pvc_name,
                    pvc=pvc,
                    disk_naming=disk_naming,
                    pv_name=pv_name,
                )
            except DiskMigrationError:
                logger.warning(
                    "unable to migrate PVC",
                    extra={
                        "pv_name": pv_name,
                        "pvc_name": pvc_name,
                    },
                )


async def migrate_disk(
    kube_client: KubeClient,
    pvc_name: str,
    pvc: V1PersistentVolumeClaim,
    disk_naming: V1DiskNamingCRD,
    pv_name: str | None = None,
) -> None:
    """
    an entry-point for a single disk migration.

    `pv_name` might be missing, in a cases when disk was created,
    but was never mounted to any job/app/etc.
    """
    logger.info("migrating disk: %s", pvc_name)

    org_name = pvc.metadata.labels.get(DISK_API_ORG_LABEL) or normalize_name(NO_ORG)
    project_name = pvc.metadata.labels.get(DISK_API_PROJECT_LABEL)
    if not project_name:
        user_label = pvc.metadata.labels[USER_LABEL]
        project_name, *_ = user_label.split("--")

    # create a new namespace
    new_namespace = await create_namespace(kube_client, org_name, project_name)

    if pv_name:
        # update reclaim policy, so underlying storage won't be deleted
        await update_reclaim_policy(kube_client, pv_name, policy="Retain")

    # delete a PVC
    await delete_pvc(kube_client, namespace=CURRENT_NAMESPACE, pvc_name=pvc_name)

    # wait until kube actually delete it
    await wait_pvc_deleted(kube_client, namespace=CURRENT_NAMESPACE, pvc_name=pvc_name)

    if pv_name:
        # remove a claim reference from the PV to release it
        await remove_claim_ref(kube_client, pv_name)

    assert new_namespace.metadata.name is not None
    # create a PVC in a new namespace
    await create_pvc(
        kube_client,
        pvc_name,
        old_pvc=pvc,
        namespace=new_namespace.metadata.name,
        org_name=org_name,
        project_name=project_name,
        pv_name=pv_name,
    )

    if pv_name:
        # wait until kube associate a newly created PVC with the old PV
        await wait_claim_ref_set(kube_client, pv_name, pvc_name)

    if pv_name:
        # update reclaim policy back
        await update_reclaim_policy(kube_client, pv_name, policy="Delete")

    # remove old disk naming
    logger.info("removing an old disk naming: %s", disk_naming)
    await kube_client.neuromation_io_v1.disk_naming.delete(
        name=disk_naming.metadata.name, namespace=CURRENT_NAMESPACE
    )
    logger.info("removed an old disk naming: %s", disk_naming)

    # create a new disk naming
    new_disk_naming = V1DiskNamingCRD(
        kind="DiskNaming",
        metadata=V1DiskNamingCRDMetadata(
            name=disk_naming.metadata.name,
            namespace=new_namespace.metadata.name,
        ),
        spec=V1DiskNamingCRDSpec(
            disk_id=disk_naming.spec.disk_id,
        ),
    )
    logger.info("creating a new disk naming: %s", new_disk_naming)
    await kube_client.neuromation_io_v1.disk_naming.create(
        model=new_disk_naming, namespace=new_namespace.metadata.name
    )
    logger.info("created a new disk naming: %s", new_disk_naming)

    logger.info("migrating done: %s", pvc_name)


async def _waiter() -> AsyncIterator[None]:
    """A wrapper which constantly yields for 60 seconds"""
    async with asyncio.timeout(60):
        while True:
            yield
            await asyncio.sleep(1)


async def update_reclaim_policy(
    kube_client: KubeClient, pv_name: str, policy: Literal["Retain", "Delete"]
) -> None:
    """
    Updates PV reclaim policy
    """
    logger.info("updating reclaim policy: %s", pv_name)

    patch_json_list = [
        PatchAdd(
            path="/spec/persistentVolumeReclaimPolicy",
            value=policy,
        )
    ]
    await kube_client.core_v1.persistent_volume.patch_json(
        name=pv_name, patch_json_list=patch_json_list
    )

    logger.info("updated reclaim policy: %s", pv_name)


async def delete_pvc(
    kube_client: KubeClient,
    namespace: str,
    pvc_name: str,
) -> None:
    logger.info("deleting pvc: %s", pvc_name)
    await ensure_pvc_deletable(kube_client, pvc_name)
    await kube_client.core_v1.persistent_volume_claim.delete(
        name=pvc_name, namespace=namespace
    )
    logger.info("deleted pvc: %s", pvc_name)


async def ensure_pvc_deletable(
    kube_client: KubeClient,
    pvc_name: str,
) -> None:
    """
    Checks if any of PODs is using this PVC as a volume
    """
    pod_list = await kube_client.core_v1.pod.get_list(all_namespaces=True)
    for pod in pod_list.items:
        assert pod.spec is not None
        pvc_in_use = {
            v.persistent_volume_claim.claim_name
            for v in pod.spec.volumes
            if v.persistent_volume_claim
        }
        if pvc_name in pvc_in_use:
            raise PvcInUseError()


async def wait_pvc_deleted(
    kube_client: KubeClient, namespace: str, pvc_name: str
) -> None:
    logger.info("waiting for pvc deletion: %s", pvc_name)
    async for _ in _waiter():
        try:
            await kube_client.core_v1.persistent_volume_claim.get(
                name=pvc_name, namespace=namespace
            )
        except ResourceNotFound:
            return


async def remove_claim_ref(
    kube_client: KubeClient,
    pv_name: str,
) -> None:
    logger.info("removing claim ref: %s", pv_name)

    patch_json_list = [
        PatchAdd(
            path="/spec/claimRef",
            value=None,
        )
    ]
    await kube_client.core_v1.persistent_volume.patch_json(
        name=pv_name,
        patch_json_list=patch_json_list,
    )

    logger.info("claim ref removed: %s", pv_name)


async def wait_claim_ref_set(
    kube_client: KubeClient,
    pv_name: str,
    pvc_name: str,
) -> None:
    logger.info("Waiting for claim ref to be set: %s; pv=%s", pvc_name, pv_name)
    async for _ in _waiter():
        pv = await kube_client.core_v1.persistent_volume.get(name=pv_name)
        if pv.spec.claim_ref and pv.spec.claim_ref.name == pvc_name:
            return


async def create_pvc(
    kube_client: KubeClient,
    pvc_name: str,
    old_pvc: V1PersistentVolumeClaim,
    namespace: str,
    org_name: str,
    project_name: str,
    pv_name: str | None = None,
) -> None:
    logger.info(
        "creating a new PVC: %s; namespace=%s; org_name=%s; project_name=%s; pv=%s",
        pvc_name,
        namespace,
        org_name,
        project_name,
        pv_name,
    )
    old_spec = old_pvc.spec
    old_metadata = old_pvc.metadata

    annotations = {}

    for annotation_key in DISK_ANNOTATION_MAP.keys():
        if annotation_key in old_metadata.annotations:
            annotation_value = old_metadata.annotations[annotation_key]
            apolo_annotation_key = DISK_ANNOTATION_MAP[annotation_key]
            annotations[annotation_key] = annotation_value
            annotations[apolo_annotation_key] = annotation_value

    spec = V1PersistentVolumeClaimSpec(
        access_modes=old_spec.access_modes,
        resources=old_spec.resources,
        storage_class_name=old_spec.storage_class_name,
        volume_mode=old_spec.volume_mode,
    )

    if pv_name:
        spec.volume_name = pv_name

    new_pvc_body = V1PersistentVolumeClaim(
        kind="PersistentVolumeClaim",
        metadata=V1ObjectMeta(
            name=pvc_name,
            namespace=namespace,
            uid=old_metadata.uid,  # keep old UID so PV can claim the new PVC
            labels={
                APOLO_ORG_LABEL: org_name,
                DISK_API_ORG_LABEL: org_name,
                APOLO_PROJECT_LABEL: project_name,
                DISK_API_PROJECT_LABEL: project_name,
                DISK_API_MARK_LABEL: "true",
                APOLO_DISK_API_MARK_LABEL: "true",
                USER_LABEL: old_metadata.labels[USER_LABEL],
                APOLO_USER_LABEL: old_metadata.labels[USER_LABEL],
            },
            annotations=annotations,
        ),
        spec=spec,
    )
    await kube_client.core_v1.persistent_volume_claim.create(
        model=new_pvc_body, namespace=namespace
    )
    logger.info("created a new PVC: %s", pvc_name)


def main() -> None:  # pragma: no coverage
    init_logging()
    config = EnvironConfigFactory().create_job_migrate_project()
    logging.info("Loaded config: %r", config)
    asyncio.run(migration_loop(config))


if __name__ == "__main__":
    main()
