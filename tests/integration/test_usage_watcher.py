import asyncio
from collections.abc import AsyncIterator
from copy import replace
from datetime import timedelta

import pytest
from apolo_kube_client import KubeClientProxy, KubeClientSelector, KubeConfig
from apolo_kube_client.apolo import generate_namespace_name

from platform_disk_api.service import DiskNotFound, DiskRequest, Service
from platform_disk_api.usage_watcher import (
    utc_now,
    watch_disk_usage,
    watch_lifespan_ended,
)

from .kube import run_pod


class TestUsageWatcher:
    @pytest.fixture
    async def watcher_task(
        self,
        kube_config: KubeConfig,
        kube_selector: KubeClientSelector,
        service: Service,
    ) -> AsyncIterator[None]:
        kube_selector._config = replace(kube_config, client_watch_timeout_s=1)
        # async with KubeClient(config=kube_config) as kube_client:
        task = asyncio.create_task(watch_disk_usage(service))
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
        scoped_kube_client: KubeClientProxy,
        service: Service,
        org_project: tuple[str, str],
    ) -> None:
        org, project = org_project

        async def wait_for_last_usage(disk_id: str) -> None:
            while True:
                disk = await service.get_disk(org, project, disk_id)
                if disk.last_usage is not None:
                    break
                await asyncio.sleep(0.1)

        for _ in range(10):
            disk = await service.create_disk(
                DiskRequest(
                    1024**2,
                    project_name=project,
                    org_name=org,
                ),
                "user",
            )
            before_start = utc_now()
            async with run_pod(scoped_kube_client, [disk.id]):
                await asyncio.wait_for(wait_for_last_usage(disk.id), timeout=10)

            disk = await service.get_disk(disk.org_name, disk.project_name, disk.id)
            assert disk.last_usage
            assert before_start < disk.last_usage

    async def test_task_cleanuped_no_usage(
        self,
        cleanup_task: None,
        service: Service,
        org_project: tuple[str, str],
    ) -> None:
        org, project = org_project
        disk = await service.create_disk(
            DiskRequest(
                storage=1000,
                life_span=timedelta(seconds=1),
                project_name=project,
                org_name=org,
            ),
            "user",
        )
        await asyncio.sleep(1.5)
        with pytest.raises(DiskNotFound):
            await service.get_disk(disk.org_name, disk.project_name, disk.id)

    async def test_task_cleaned_up_with_usage(
        self,
        cleanup_task: None,
        service: Service,
        org_project: tuple[str, str],
    ) -> None:
        org, project = org_project
        # give more waiting time for vcluster to sync
        wait_for = 5.33 if org.startswith("vcluster") else 1.33
        namespace_name = generate_namespace_name(org, project)
        disk = await service.create_disk(
            DiskRequest(
                storage=1000,
                life_span=timedelta(seconds=2),
                project_name=project,
                org_name=org,
            ),
            "user",
        )
        await asyncio.sleep(wait_for)
        real_disk_id = await service.resolve_disk_from_vcluster(disk.id, org, project)
        await service.mark_disk_usage(namespace_name, real_disk_id, utc_now())
        await asyncio.sleep(wait_for)
        assert await service.get_disk(disk.org_name, disk.project_name, disk.id)
        await asyncio.sleep(wait_for)
        with pytest.raises(DiskNotFound):
            await service.get_disk(disk.org_name, disk.project_name, disk.id)
