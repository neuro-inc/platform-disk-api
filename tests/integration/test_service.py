import asyncio

import pytest

from platform_disk_api.kube_client import KubeClient, PersistentVolumeClaimWrite
from platform_disk_api.service import DiskNotFound, DiskRequest, Service


pytestmark = pytest.mark.asyncio


class TestService:
    @pytest.fixture
    def service(self, kube_client: KubeClient, k8s_storage_class: str) -> Service:
        return Service(kube_client=kube_client, storage_class_name=k8s_storage_class)

    async def test_create_disk(self, cleanup_pvcs: None, service: Service) -> None:
        request = DiskRequest(storage=1024 * 1024)
        disk = await service.create_disk(request, "testuser")
        assert disk.storage >= request.storage
        assert disk.owner == "testuser"
        disks = await service.get_all_disks()
        assert len(disks) == 1
        assert disks[0].id == disk.id

    async def test_remove_disk(self, cleanup_pvcs: None, service: Service) -> None:
        request = DiskRequest(storage=1024 * 1024)
        disk = await service.create_disk(request, "testuser")
        await service.remove_disk(disk.id)
        # Deletion is async in k8s, lets wait for it:

        async def wait_no_disk() -> None:
            while await service.get_all_disks():
                await asyncio.sleep(0.1)

        await asyncio.wait_for(wait_no_disk(), timeout=2)

    async def test_get_disk(self, cleanup_pvcs: None, service: Service) -> None:
        request = DiskRequest(storage=1024 * 1024)
        disk_created = await service.create_disk(request, "testuser")
        disk_get = await service.get_disk(disk_created.id)
        assert disk_get.id == disk_created.id
        assert disk_get.owner == disk_created.owner
        assert disk_get.storage >= disk_created.storage

    async def test_get_non_existing_disk(
        self, cleanup_pvcs: None, service: Service
    ) -> None:
        with pytest.raises(DiskNotFound):
            await service.get_disk("no-disk-for-this-name")

    async def test_remove_non_existing_disk(
        self, cleanup_pvcs: None, service: Service
    ) -> None:
        with pytest.raises(DiskNotFound):
            await service.remove_disk("no-disk-for-this-name")

    async def test_get_all_disk_ignores_outer_pvcs(
        self, cleanup_pvcs: None, kube_client: KubeClient, service: Service
    ) -> None:
        await kube_client.create_pvc(
            PersistentVolumeClaimWrite(
                name="outer-pvc", storage_class_name="no-way", storage=200
            )
        )
        request = DiskRequest(storage=1024 * 1024)
        disk_created = await service.create_disk(request, "testuser")
        all_disks = await service.get_all_disks()
        assert len(all_disks) == 1
        assert all_disks[0].id == disk_created.id
