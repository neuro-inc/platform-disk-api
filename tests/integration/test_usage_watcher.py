import asyncio
from collections.abc import AsyncIterator, Callable
from dataclasses import replace
from datetime import timedelta

import pytest

from platform_disk_api.config import KubeConfig
from platform_disk_api.kube_client import KubeClient
from platform_disk_api.service import DiskNotFound, DiskRequest, Service
from platform_disk_api.usage_watcher import (
    utc_now,
    watch_disk_usage,
    watch_lifespan_ended,
)

from tests.integration.kube import KubeClientForTest


class TestUsageWatcher:
    @pytest.fixture
    def service(self, kube_client: KubeClient, k8s_storage_class: str) -> Service:
        return Service(
            kube_client=kube_client,
            storage_class_name=k8s_storage_class,
        )

    @pytest.fixture
    async def watcher_task(
        self,
        kube_config: KubeConfig,
        kube_client_factory: Callable[[KubeConfig], KubeClientForTest],
        service: Service,
    ) -> AsyncIterator[None]:
        kube_config = replace(kube_config, client_watch_timeout_s=1)  # Force reloads
        async with kube_client_factory(kube_config) as kube_client:
            task = asyncio.create_task(watch_disk_usage(kube_client, service))
            await asyncio.sleep(0)  # Allow task to start
            yield
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @pytest.fixture
    async def cleanup_task(self, service: Service) -> AsyncIterator[None]:
        task = asyncio.create_task(watch_lifespan_ended(service, check_interval=0.1))
        await asyncio.sleep(0)  # Allow task to start
        yield
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def test_usage_watcher_updates_label(
        self,
        watcher_task: None,
        kube_client: KubeClientForTest,
        service: Service,
    ) -> None:
        async def wait_for_last_usage(disk_id: str) -> None:
            while True:
                disk = await service.get_disk(disk_id)
                if disk.last_usage is not None:
                    break
                await asyncio.sleep(0.1)

        for _ in range(10):
            disk = await service.create_disk(DiskRequest(1024**2), "user")
            before_start = utc_now()
            async with kube_client.run_pod([disk.id]):
                await asyncio.wait_for(wait_for_last_usage(disk.id), timeout=10)

            disk = await service.get_disk(disk.id)
            assert disk.last_usage
            assert before_start < disk.last_usage

    async def test_task_cleanuped_no_usage(
        self,
        cleanup_task: None,
        service: Service,
    ) -> None:
        disk = await service.create_disk(
            DiskRequest(storage=1000, life_span=timedelta(seconds=1)), "user"
        )
        await asyncio.sleep(1.5)
        with pytest.raises(DiskNotFound):
            await service.get_disk(disk.id)

    async def test_task_cleanuped_with_usage(
        self,
        cleanup_task: None,
        service: Service,
    ) -> None:
        disk = await service.create_disk(
            DiskRequest(storage=1000, life_span=timedelta(seconds=2)), "user"
        )
        await asyncio.sleep(1.33)
        await service.mark_disk_usage(disk.id, utc_now())
        await asyncio.sleep(1.33)
        assert await service.get_disk(disk.id)
        await asyncio.sleep(1.33)
        with pytest.raises(DiskNotFound):
            await service.get_disk(disk.id)
