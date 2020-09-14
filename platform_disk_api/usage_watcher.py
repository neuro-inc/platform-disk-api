import asyncio
import logging
from datetime import datetime
from typing import Iterable, Optional

from platform_logging import init_logging

from platform_disk_api.api import create_kube_client
from platform_disk_api.config import KubeConfig
from platform_disk_api.config_factory import EnvironConfigFactory
from platform_disk_api.kube_client import (
    KubeClient,
    PodWatchEvent,
    ResourceGone,
    ResourceNotFound,
)
from platform_disk_api.service import Service
from platform_disk_api.utils import utc_now


async def update_last_used(
    service: Service, pvc_names: Iterable[str], time: datetime
) -> None:
    for pvc_name in pvc_names:
        try:
            await service.mark_disk_usage(pvc_name, time)
        except ResourceNotFound:
            pass


async def watch_disk_usage(kube_client: KubeClient, service: Service) -> None:
    resource_version: Optional[str] = None
    while True:
        if resource_version is None:
            list_result = await kube_client.list_pods()
            now = utc_now()
            pvc_names = set(pvc for pod in list_result.pods for pvc in pod.pvc_in_use)
            await update_last_used(service, pvc_names, now)
            resource_version = list_result.resource_version
        try:
            async for event in kube_client.watch_pods():
                if event.type == PodWatchEvent.Type.BOOKMARK:
                    resource_version = event.resource_version
                else:
                    await update_last_used(service, event.pod.pvc_in_use, utc_now())
        except ResourceGone:
            resource_version = None


async def async_main(kube_config: KubeConfig) -> None:
    async with create_kube_client(kube_config) as kube_client:
        # We are not going to create disks using this service
        # instance, so its safe to provide invalid storage
        # class name
        service = Service(kube_client, "fake invalid value")
        await watch_disk_usage(kube_client, service)


def main() -> None:  # pragma: no coverage
    init_logging()
    kube_config = EnvironConfigFactory().create_kube()
    logging.info("Loaded k8s config: %r", kube_config)
    asyncio.run(async_main(kube_config))
