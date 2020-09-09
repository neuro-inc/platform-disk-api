import asyncio
from typing import AsyncIterator, Awaitable, Callable
from uuid import uuid4

import pytest

from platform_disk_api.config import Config
from platform_disk_api.kube_client import (
    PersistentVolumeClaimRead,
    PersistentVolumeClaimWrite,
)
from platform_disk_api.usage_watcher import (
    DISK_API_LAST_USAGE_LABEL,
    utc_now,
    watch_disk_usage,
)
from platform_disk_api.utils import datetime_load
from tests.integration.conftest_kube import KubeClientForTest


pytestmark = pytest.mark.asyncio


class TestUsageWatcher:
    @pytest.fixture
    async def watcher_task(self, config: Config) -> AsyncIterator[None]:
        task = asyncio.create_task(watch_disk_usage(config))
        await asyncio.sleep(0)  # Allow task to start
        yield
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.fixture
    async def pvc_factory(
        self, cleanup_pvcs: None, kube_client: KubeClientForTest, k8s_storage_class: str
    ) -> AsyncIterator[Callable[[], Awaitable[PersistentVolumeClaimRead]]]:
        async def _factory() -> PersistentVolumeClaimRead:
            storage_to_request = 10 * 1024 * 1024  # 10 mb
            return await kube_client.create_pvc(
                PersistentVolumeClaimWrite(
                    name=str(uuid4()),
                    storage_class_name=k8s_storage_class,
                    storage=storage_to_request,
                )
            )

        yield _factory

    async def test_usage_watcher_updates_label(
        self,
        watcher_task: None,
        pvc_factory: Callable[[], Awaitable[PersistentVolumeClaimRead]],
        kube_client: KubeClientForTest,
    ) -> None:
        async def wait_for_label(pvc_name: str) -> None:
            while True:
                pvc = await kube_client.get_pvc(pvc_name)
                if DISK_API_LAST_USAGE_LABEL in pvc.labels:
                    return
                await asyncio.sleep(0.1)

        for _ in range(10):
            pvc = await pvc_factory()
            before_start = utc_now()
            async with kube_client.run_pod([pvc.name]):
                await asyncio.wait_for(wait_for_label(pvc.name), timeout=10)
                pass

            pvc = await kube_client.get_pvc(pvc.name)
            assert before_start < datetime_load(pvc.labels[DISK_API_LAST_USAGE_LABEL])
