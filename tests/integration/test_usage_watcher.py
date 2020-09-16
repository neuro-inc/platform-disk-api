import asyncio
from typing import AsyncIterator

import pytest

from platform_disk_api.kube_client import KubeClient
from platform_disk_api.service import DiskRequest, Service
from platform_disk_api.usage_watcher import utc_now, watch_disk_usage
from tests.integration.conftest_kube import KubeClientForTest


pytestmark = pytest.mark.asyncio


class TestUsageWatcher:
    @pytest.fixture
    def service(self, kube_client: KubeClient, k8s_storage_class: str) -> Service:
        return Service(kube_client=kube_client, storage_class_name=k8s_storage_class)

    @pytest.fixture
    async def watcher_task(
        self, kube_client: KubeClient, service: Service
    ) -> AsyncIterator[None]:
        task = asyncio.create_task(watch_disk_usage(kube_client, service))
        await asyncio.sleep(0)  # Allow task to start
        yield
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def test_usage_watcher_updates_label(
        self, watcher_task: None, kube_client: KubeClientForTest, service: Service,
    ) -> None:
        async def wait_for_last_usage(disk_id: str) -> None:
            while True:
                disk = await service.get_disk(disk_id)
                if disk.last_usage is not None:
                    break
                await asyncio.sleep(0.1)

        for _ in range(10):
            disk = await service.create_disk(DiskRequest(1024 ** 2), "user")
            before_start = utc_now()
            async with kube_client.run_pod([disk.id]):
                await asyncio.wait_for(wait_for_last_usage(disk.id), timeout=10)
                pass

            disk = await service.get_disk(disk.id)
            assert disk.last_usage
            assert before_start < disk.last_usage
