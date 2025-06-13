from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any, Literal, Optional

from apolo_kube_client.apolo import NO_ORG, create_namespace, normalize_name
from apolo_kube_client.errors import ResourceNotFound
from neuro_logging import (
    init_logging,
)
from yarl import URL

from platform_disk_api.api import create_kube_client
from platform_disk_api.config import JobMigrateProjectNamespaceConfig
from platform_disk_api.config_factory import EnvironConfigFactory
from platform_disk_api.kube_client import DiskNaming, KubeClient
from platform_disk_api.service import (
    APOLO_DISK_API_MARK_LABEL,
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


class DiskMigrationError(Exception):
    pass


class PvcInUseError(DiskMigrationError):
    pass


async def migration_loop(
    config: JobMigrateProjectNamespaceConfig,
    disk_ids_filter: Optional[set[str]] = None,
) -> None:
    """
    A main loop which iterates over all disks and tries to migrate them to a
    project-based namespaces.

    :param config: ...
    :param disk_ids_filter: optional filtering by disk IDs
    """
    async with create_kube_client(config.kube) as kube_client:
        # get all disk namings
        disk_namings = await kube_client.list_disk_namings()
        pvc_to_disk_naming = {dn.disk_id: dn for dn in disk_namings}

        # get all the PVCs which are apolo disks
        pvc_url = URL(
            kube_client._generate_pvc_url(namespace=CURRENT_NAMESPACE)
        ).with_query(labelSelector=f"{DISK_API_MARK_LABEL}=true")
        all_pvc = await kube_client.get(pvc_url)

        for pvc in all_pvc["items"]:
            pvc_name = pvc["metadata"]["name"]
            if disk_ids_filter and pvc_name not in disk_ids_filter:
                continue

            # PV name (if bound).
            # sometimes users can create disks, but not mount them;
            # in such a case - PVC won't have a respective PV yet
            pv_name = pvc["spec"].get("volumeName")

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
    client: KubeClient,
    pvc_name: str,
    pvc: dict[str, Any],
    disk_naming: DiskNaming,
    pv_name: Optional[str] = None,
) -> None:
    """
    an entry-point for a single disk migration.

    `pv_name` might be missing, in a cases when disk was created,
    but was never mounted to any job/app/etc.
    """
    logger.info("migrating disk: %s", pvc_name)

    current_meta = pvc["metadata"]
    org_name = current_meta["labels"].get(DISK_API_ORG_LABEL) or normalize_name(NO_ORG)
    project_name = current_meta["labels"].get(DISK_API_PROJECT_LABEL)
    if not project_name:
        user_label = current_meta["labels"][USER_LABEL]
        project_name, *_ = user_label.split("--")

    # create a new namespace
    new_namespace = await create_namespace(client, org_name, project_name)

    if pv_name:
        # update reclaim policy, so underlying storage won't be deleted
        await update_reclaim_policy(client, pv_name, policy="Retain")

    # delete a PVC
    await delete_pvc(client, namespace=CURRENT_NAMESPACE, pvc_name=pvc_name)

    # wait until kube actually delete it
    await wait_pvc_deleted(client, namespace=CURRENT_NAMESPACE, pvc_name=pvc_name)

    if pv_name:
        # remove a claim reference from the PV to release it
        await remove_claim_ref(client, pv_name)

    # create a PVC in a new namespace
    await create_pvc(
        client,
        pvc_name,
        old_pvc=pvc,
        namespace=new_namespace.name,
        org_name=org_name,
        project_name=project_name,
        pv_name=pv_name,
    )

    if pv_name:
        # wait until kube associate a newly created PVC with the old PV
        await wait_claim_ref_set(client, pv_name, pvc_name)

    if pv_name:
        # update reclaim policy back
        await update_reclaim_policy(client, pv_name, policy="Delete")

    # remove old disk naming
    logger.info("removing an old disk naming: %s", disk_naming)
    await client.remove_disk_naming(
        namespace=CURRENT_NAMESPACE,
        name=disk_naming.name,
    )
    logger.info("removed an old disk naming: %s", disk_naming)

    # create a new disk naming
    new_disk_naming = DiskNaming(
        namespace=new_namespace.name,
        name=disk_naming.name,
        disk_id=disk_naming.disk_id,
    )
    logger.info("creating a new disk naming: %s", new_disk_naming)
    await client.create_disk_naming(new_disk_naming)
    logger.info("created a new disk naming: %s", new_disk_naming)

    logger.info("migrating done: %s", pvc_name)


async def _waiter() -> AsyncIterator[None]:
    """A wrapper which constantly yields for 60 seconds"""
    async with asyncio.timeout(60):
        while True:
            yield
            await asyncio.sleep(1)


async def update_reclaim_policy(
    client: KubeClient, pv_name: str, policy: Literal["Retain", "Delete"]
) -> None:
    """
    Updates PV reclaim policy
    """
    logger.info("updating reclaim policy: %s", pv_name)
    pv_url = f"{client.api_v1_url}/persistentvolumes/{pv_name}"
    await client.patch(
        pv_url,
        headers={"Content-Type": "application/merge-patch+json"},
        json={
            "spec": {
                "persistentVolumeReclaimPolicy": policy,
            }
        },
    )
    logger.info("updated reclaim policy: %s", pv_name)


async def delete_pvc(
    client: KubeClient,
    namespace: str,
    pvc_name: str,
) -> None:
    logger.info("deleting pvc: %s", pvc_name)
    url = client._generate_pvc_url(namespace=namespace, pvc_name=pvc_name)
    await ensure_pvc_deletable(client, pvc_name)
    await client.delete(url)
    logger.info("deleted pvc: %s", pvc_name)


async def ensure_pvc_deletable(
    client: KubeClient,
    pvc_name: str,
) -> None:
    """
    Checks if any of PODs is using this PVC as a volume
    """
    pods = await client.list_pods()
    for pod in pods.pods:
        if pvc_name in pod.pvc_in_use:
            raise PvcInUseError()


async def wait_pvc_deleted(client: KubeClient, namespace: str, pvc_name: str) -> None:
    logger.info("waiting for pvc deletion: %s", pvc_name)
    url = client._generate_pvc_url(namespace=namespace, pvc_name=pvc_name)
    async for _ in _waiter():
        try:
            await client.get(url)
        except ResourceNotFound:
            return


async def remove_claim_ref(
    client: KubeClient,
    pv_name: str,
) -> None:
    logger.info("removing claim ref: %s", pv_name)
    url = f"{client.api_v1_url}/persistentvolumes/{pv_name}"
    await client.patch(
        url,
        headers={"Content-Type": "application/merge-patch+json"},
        json={"spec": {"claimRef": None}},
    )
    logger.info("claim ref removed: %s", pv_name)


async def wait_claim_ref_set(
    client: KubeClient,
    pv_name: str,
    pvc_name: str,
) -> None:
    logger.info("Waiting for claim ref to be set: %s; pv=%s", pvc_name, pv_name)
    url = f"{client.api_v1_url}/persistentvolumes/{pv_name}"
    async for _ in _waiter():
        pv = await client.get(url)
        if (pv["spec"].get("claimRef", {}) or {}).get("name") == pvc_name:
            return


async def create_pvc(
    client: KubeClient,
    pvc_name: str,
    old_pvc: dict[str, Any],
    namespace: str,
    org_name: str,
    project_name: str,
    pv_name: Optional[str] = None,
) -> None:
    logger.info(
        "creating a new PVC: %s; namespace=%s; org_name=%s; project_name=%s; pv=%s",
        pvc_name,
        namespace,
        org_name,
        project_name,
        pv_name,
    )
    old_spec = old_pvc["spec"]
    old_metadata = old_pvc["metadata"]

    annotations = {}

    for annotation_key in (
        DISK_API_NAME_ANNOTATION,
        DISK_API_CREATED_AT_ANNOTATION,
        DISK_API_LAST_USAGE_ANNOTATION,
        DISK_API_LIFE_SPAN_ANNOTATION,
        DISK_API_USED_BYTES_ANNOTATION,
    ):
        if annotation_key in old_metadata["annotations"]:
            annotation_value = old_metadata["annotations"][annotation_key]
            apolo_annotation_key = annotation_key.replace(
                "platform.neuromation.io", "platform.apolo.us"
            )
            annotations[annotation_key] = annotation_value
            annotations[apolo_annotation_key] = annotation_value

    spec = {
        "accessModes": old_spec.get("accessModes", []),
        "resources": old_spec.get("resources", {}),
        "storageClassName": old_spec["storageClassName"],
        "volumeMode": old_spec.get("volumeMode", "Filesystem"),
    }

    if pv_name:
        spec["volumeName"] = pv_name

    new_pvc_body = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": pvc_name,
            "namespace": namespace,
            "uid": old_metadata["uid"],  # keep old UID so PV can claim the new PVC
            "labels": {
                APOLO_ORG_LABEL: org_name,
                DISK_API_ORG_LABEL: org_name,
                APOLO_PROJECT_LABEL: project_name,
                DISK_API_PROJECT_LABEL: project_name,
                DISK_API_MARK_LABEL: "true",
                APOLO_DISK_API_MARK_LABEL: "true",
                USER_LABEL: old_metadata["labels"][USER_LABEL],
                APOLO_USER_LABEL: old_metadata["labels"][USER_LABEL],
            },
            "annotations": annotations,
        },
        "spec": spec,
    }
    url = client._generate_pvc_url(namespace=namespace)
    await client.post(url, json=new_pvc_body)
    logger.info("created a new PVC: %s", pvc_name)


def main() -> None:  # pragma: no coverage
    init_logging()
    config = EnvironConfigFactory().create_job_migrate_project()
    logging.info("Loaded config: %r", config)
    asyncio.run(migration_loop(config))


if __name__ == "__main__":
    main()
