import asyncio

import pytest

from platform_disk_api.kube_client import KubeClient
from platform_disk_api.service import DiskRequest, Service


pytestmark = pytest.mark.asyncio


class TestService:
    @pytest.fixture
    def service(self, kube_client: KubeClient, k8s_storage_class: str) -> Service:
        return Service(kube_client=kube_client, storage_class_name=k8s_storage_class)

    async def test_create_disk(self, cleanup_pvcs: None, service: Service) -> None:
        request = DiskRequest(name="test", storage=1024 * 1024)
        disk = await service.create_disk(request)
        assert disk.name == request.name
        assert disk.storage >= request.storage
        disks = await service.get_all_disks()
        assert len(disks) == 1
        assert disks[0] == disk

    async def test_remove_disk(self, cleanup_pvcs: None, service: Service) -> None:
        request = DiskRequest(name="test", storage=1024 * 1024)
        await service.create_disk(request)
        await service.remove_disk(request.name)
        # Deletion is async in k8s, lets wait for it:

        async def wait_no_disk() -> None:
            while await service.get_all_disks():
                await asyncio.sleep(0.1)

        await asyncio.wait_for(wait_no_disk(), timeout=2)
