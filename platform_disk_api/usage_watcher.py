import asyncio
import logging
from asyncio import CancelledError
from datetime import datetime
from typing import Iterable, List, Optional

import aiohttp
from platform_logging import (
    init_logging,
    make_sentry_trace_config,
    make_zipkin_trace_config,
    new_trace_cm,
    setup_sentry,
    setup_zipkin_tracer,
)

from platform_disk_api.api import create_kube_client
from platform_disk_api.config import DiskUsageWatcherConfig
from platform_disk_api.config_factory import EnvironConfigFactory
from platform_disk_api.kube_client import KubeClient, PodWatchEvent, ResourceGone
from platform_disk_api.service import DiskNotFound, Service
from platform_disk_api.utils import utc_now


logger = logging.getLogger(__name__)


async def update_last_used(
    service: Service, pvc_names: Iterable[str], time: datetime
) -> None:
    for pvc_name in pvc_names:
        try:
            await service.mark_disk_usage(pvc_name, time)
        except DiskNotFound:
            pass


async def watch_disk_usage(kube_client: KubeClient, service: Service) -> None:
    resource_version: Optional[str] = None
    while True:
        try:
            if resource_version is None:
                async with new_trace_cm(name="watch_disk_usage_start"):
                    list_result = await kube_client.list_pods()
                    now = utc_now()
                    pvc_names = {
                        pvc for pod in list_result.pods for pvc in pod.pvc_in_use
                    }
                    await update_last_used(service, pvc_names, now)
                    resource_version = list_result.resource_version
            async for event in kube_client.watch_pods(resource_version):
                async with new_trace_cm(name="watch_disk_usage"):
                    if event.type == PodWatchEvent.Type.BOOKMARK:
                        resource_version = event.resource_version
                    else:
                        await update_last_used(service, event.pod.pvc_in_use, utc_now())
        except asyncio.CancelledError:
            raise
        except ResourceGone:
            resource_version = None
        except Exception:
            logger.exception("Failed to update disk usage")


async def watch_used_bytes(
    kube_client: KubeClient, service: Service, check_interval: float = 60
) -> None:
    while True:
        try:
            async with new_trace_cm(name="watch_used_bytes"):
                async for stat in kube_client.get_pvc_volumes_metrics():
                    try:
                        await service.update_disk_used_bytes(
                            stat.pvc_name, stat.used_bytes
                        )
                    except DiskNotFound:
                        pass
            await asyncio.sleep(check_interval)
        except CancelledError:
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
                        await service.remove_disk(disk.id)
            await asyncio.sleep(check_interval)
        except CancelledError:
            raise
        except Exception:
            logger.exception("Failed to check lifespan")


async def async_main(config: DiskUsageWatcherConfig) -> None:
    async with create_kube_client(
        config.kube, make_tracing_trace_configs(config)
    ) as kube_client:
        # We are not going to create disks using this service
        # instance, so its safe to provide invalid storage
        # class name
        service = Service(kube_client, "fake invalid value")
        await asyncio.gather(
            watch_disk_usage(kube_client, service),
            watch_lifespan_ended(service),
            watch_used_bytes(kube_client, service),
        )


def make_tracing_trace_configs(
    config: DiskUsageWatcherConfig,
) -> List[aiohttp.TraceConfig]:
    trace_configs = []

    if config.zipkin:
        trace_configs.append(make_zipkin_trace_config())

    if config.sentry:
        trace_configs.append(make_sentry_trace_config())

    return trace_configs


def setup_tracing(config: DiskUsageWatcherConfig) -> None:
    if config.zipkin:
        setup_zipkin_tracer(
            config.zipkin.app_name,
            config.server.host,
            config.server.port,
            config.zipkin.url,
            config.zipkin.sample_rate,
        )

    if config.sentry:
        setup_sentry(
            config.sentry.dsn,
            app_name=config.sentry.app_name,
            cluster_name=config.sentry.cluster_name,
            sample_rate=config.sentry.sample_rate,
        )


def main() -> None:  # pragma: no coverage
    init_logging()
    config = EnvironConfigFactory().create_disk_usage_watcher()
    logging.info("Loaded config: %r", config)
    asyncio.run(async_main(config))
