from datetime import timedelta

import pytest

from platform_disk_api.kube_client import KubeClient, PersistentVolumeClaimWrite
from platform_disk_api.service import DiskNameUsed, DiskNotFound, DiskRequest, Service
from platform_disk_api.utils import utc_now


pytestmark = pytest.mark.asyncio


class TestService:
    @pytest.fixture
    def service(
        self,
        kube_client: KubeClient,
        k8s_storage_class: str,
    ) -> Service:
        return Service(
            kube_client=kube_client,
            storage_class_name=k8s_storage_class,
        )

    async def test_create_disk(self, service: Service) -> None:
        request = DiskRequest(storage=1024 * 1024)
        disk = await service.create_disk(request, "testuser")
        assert disk.storage >= request.storage
        assert disk.owner == "testuser"
        disks = await service.get_all_disks()
        assert len(disks) == 1
        assert disks[0].id == disk.id

    async def test_create_disk_with_org(self, service: Service) -> None:
        request = DiskRequest(storage=1024 * 1024, org_name="test-org")
        disk = await service.create_disk(request, "testuser")
        assert disk.org_name == "test-org"
        disks = await service.get_all_disks()
        assert len(disks) == 1
        assert disks[0].org_name == "test-org"

    async def test_create_disk_with_same_name_fail(self, service: Service) -> None:
        request = DiskRequest(storage=1024 * 1024, name="test")
        await service.create_disk(request, "testuser")
        with pytest.raises(DiskNameUsed):
            await service.create_disk(request, "testuser")

    async def test_can_create_disk_with_same_name_after_delete(
        self, service: Service
    ) -> None:
        request = DiskRequest(storage=1024 * 1024, name="test")
        disk = await service.create_disk(request, "testuser")
        await service.remove_disk(disk.id)
        await service.create_disk(request, "testuser")

    # As pvc deletion is async, we should check that user will never
    # see deleted disk, so next test is executed multiple times
    @pytest.mark.parametrize("execution_number", range(10))
    async def test_remove_disk(self, execution_number: int, service: Service) -> None:
        request = DiskRequest(storage=1024 * 1024)
        disk = await service.create_disk(request, "testuser")
        await service.remove_disk(disk.id)
        assert await service.get_all_disks() == []

    async def test_get_disk(self, service: Service) -> None:
        request = DiskRequest(storage=1024 * 1024)
        disk_created = await service.create_disk(request, "testuser")
        disk_get = await service.get_disk(disk_created.id)
        assert disk_get.id == disk_created.id
        assert disk_get.owner == disk_created.owner
        assert disk_get.storage >= disk_created.storage

    async def test_get_disk_by_name(self, service: Service) -> None:
        request = DiskRequest(storage=1024 * 1024, name="test-name")
        disk_created = await service.create_disk(request, "testuser")
        disk_get = await service.get_disk_by_name("test-name", "testuser")
        assert disk_get.id == disk_created.id
        assert disk_get.owner == disk_created.owner
        assert disk_get.storage >= disk_created.storage

    async def test_get_non_existing_disk(self, service: Service) -> None:
        with pytest.raises(DiskNotFound):
            await service.get_disk("no-disk-for-this-name")

    async def test_remove_non_existing_disk(self, service: Service) -> None:
        with pytest.raises(DiskNotFound):
            await service.remove_disk("no-disk-for-this-name")

    async def test_get_all_disk_ignores_outer_pvcs(
        self, kube_client: KubeClient, service: Service
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

    async def test_life_span_stored(self, service: Service) -> None:
        life_span = timedelta(days=7)
        request = DiskRequest(storage=1024 * 1024, life_span=life_span)
        disk = await service.create_disk(request, "testuser")
        disk = await service.get_disk(disk.id)
        assert disk.life_span == life_span

    async def test_no_life_span_stored(self, service: Service) -> None:
        request = DiskRequest(storage=1024 * 1024)
        disk = await service.create_disk(request, "testuser")
        disk = await service.get_disk(disk.id)
        assert disk.life_span is None

    async def test_update_last_usage(self, service: Service) -> None:
        request = DiskRequest(storage=1024 * 1024)
        disk = await service.create_disk(request, "testuser")
        assert disk.last_usage is None
        last_usage_time = utc_now()
        await service.mark_disk_usage(disk.id, last_usage_time)
        disk = await service.get_disk(disk.id)
        assert disk.last_usage == last_usage_time
