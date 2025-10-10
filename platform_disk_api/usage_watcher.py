from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator, Iterable
from contextlib import aclosing
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from apolo_kube_client import KubeClient, KubeClientUnauthorized, ResourceGone
from neuro_logging import init_logging, new_trace_cm, setup_sentry

from platform_disk_api.config import DiskUsageWatcherConfig
from platform_disk_api.config_factory import EnvironConfigFactory
from platform_disk_api.service import DiskNotFound, Service
from platform_disk_api.utils import utc_now


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PVCVolumeMetrics:
    namespace: str
    pvc_name: str
    used_bytes: int


async def update_last_used(
    service: Service, pvc_names: Iterable[tuple[str, str]], time: datetime
) -> None:
    for namespace, pvc_name in pvc_names:
        try:
            await service.mark_disk_usage(namespace, pvc_name, time)
        except DiskNotFound:
            pass


async def get_pvc_volumes_metrics(
    kube_client: KubeClient,
) -> AsyncGenerator[PVCVolumeMetrics]:
    node_list = await kube_client.core_v1.node.get_list()
    for node in node_list.items:
        try:
            stats: dict[str, Any] = await kube_client.core_v1.node.get_stats_summary(  # type: ignore
                node.metadata.name
            )
        except Exception as exc:
            logger.exception(
                "Failed to get stats for node %s: %s", node.metadata.name, exc
            )
            continue

        for pod in stats.get("pods", []):
            for volume in pod.get("volume", []):
                try:
                    yield PVCVolumeMetrics(
                        namespace=pod["podRef"]["namespace"],
                        pvc_name=volume["pvcRef"]["name"],
                        used_bytes=volume["usedBytes"],
                    )
                except KeyError:
                    pass


async def watch_disk_usage(kube_client: KubeClient, service: Service) -> None:  # noqa: C901
    resource_version: str | None = None
    while True:
        try:
            if resource_version is None:
                async with new_trace_cm(name="watch_disk_usage_start"):
                    pod_list = await kube_client.core_v1.pod.get_list(
                        all_namespaces=True
                    )
                    now = utc_now()
                    namespace_pvcs = set()
                    for pod in pod_list.items:
                        for pvc_claim_name in [
                            v.persistent_volume_claim.claim_name
                            for v in pod.spec.volumes
                            if v.persistent_volume_claim
                        ]:
                            namespace_pvcs.add((pod.metadata.namespace, pvc_claim_name))
                    await update_last_used(service, namespace_pvcs, now)
                    resource_version = pod_list.metadata.resource_version

            watch = kube_client.core_v1.pod.watch(
                all_namespaces=True, resource_version=resource_version
            )
            async with aclosing(watch.stream()) as event_stream:
                async for event in event_stream:
                    async with new_trace_cm(name="watch_disk_usage"):
                        namespace_pvcs = set()
                        for pvc_claim_name in [
                            v.persistent_volume_claim.claim_name
                            for v in event.object.spec.volumes
                            if v.persistent_volume_claim
                        ]:
                            namespace_pvcs.add(
                                (event.object.metadata.namespace, pvc_claim_name)
                            )
                        await update_last_used(service, namespace_pvcs, utc_now())

        except asyncio.CancelledError:
            raise
        except ResourceGone:
            resource_version = None
        except KubeClientUnauthorized:
            logger.info("Kube client unauthorized")
        except Exception:
            logger.exception("Failed to update disk usage")


async def watch_used_bytes(
    kube_client: KubeClient, service: Service, check_interval: float = 60
) -> None:
    while True:
        try:
            async with new_trace_cm(name="watch_used_bytes"):
                async with aclosing(get_pvc_volumes_metrics(kube_client)) as stat_agen:
                    async for stat in stat_agen:
                        try:
                            await service.update_disk_used_bytes(
                                stat.namespace, stat.pvc_name, stat.used_bytes
                            )
                        except DiskNotFound:
                            pass
            await asyncio.sleep(check_interval)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Failed to update used bytes")


async def watch_lifespan_ended(service: Service, check_interval: float = 600) -> None:
    while True:
        try:
            async with new_trace_cm(name="watch_lifespan_ended"):
                for disk in await service.get_all_disks():
                    if disk.life_span is None:
                        continue
                    lifespan_start = disk.last_usage or disk.created_at
                    if lifespan_start + disk.life_span < utc_now():
                        await service.remove_disk(disk)
            await asyncio.sleep(check_interval)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Failed to check lifespan")


async def async_main(config: DiskUsageWatcherConfig) -> None:
    async with KubeClient(config=config.kube) as kube_client:
        # We are not going to create disks using this service
        # instance, so its safe to provide invalid storage
        # class name
        service = Service(kube_client, "fake invalid value")
        async with asyncio.TaskGroup() as tg:
            tg.create_task(watch_disk_usage(kube_client, service))
            tg.create_task(watch_lifespan_ended(service))
            tg.create_task(watch_used_bytes(kube_client, service))


def main() -> None:  # pragma: no coverage
    init_logging()
    config = EnvironConfigFactory().create_disk_usage_watcher()
    logging.info("Loaded config: %r", config)
    setup_sentry()
    asyncio.run(async_main(config))
