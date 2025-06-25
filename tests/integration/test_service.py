import datetime
from datetime import timedelta
from uuid import uuid4

import pytest
from apolo_kube_client.apolo import generate_namespace_name
from apolo_kube_client.namespace import Namespace

from platform_disk_api.kube_client import KubeClient, PersistentVolumeClaimWrite
from platform_disk_api.service import (
    Disk,
    DiskNameUsed,
    DiskNotFound,
    DiskRequest,
    Service,
)
from platform_disk_api.utils import utc_now


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
        org_name, project_name = uuid4().hex, uuid4().hex
        request = DiskRequest(
            storage=1024 * 1024,
            project_name=project_name,
            org_name=org_name,
        )
        disk = await service.create_disk(request, "testuser")
        assert disk.storage >= request.storage
        assert disk.owner == "testuser"
        assert disk.project_name == project_name
        disks = await service.get_all_disks(org_name, project_name)
        assert len(disks) == 1
        assert disks[0].id == disk.id

    async def test_create_disk_with_org(self, service: Service) -> None:
        org_name, project_name = uuid4().hex, uuid4().hex
        request = DiskRequest(
            storage=1024 * 1024, org_name=org_name, project_name=project_name
        )
        disk = await service.create_disk(request, "testuser")
        assert disk.org_name == org_name
        disks = await service.get_all_disks(org_name, project_name)
        assert len(disks) == 1
        assert disks[0].org_name == org_name

    async def test_create_disk_with_same_name_fail(self, service: Service) -> None:
        org_name, project_name = uuid4().hex, uuid4().hex
        request = DiskRequest(
            storage=1024 * 1024,
            name="test",
            project_name=project_name,
            org_name=org_name,
        )
        await service.create_disk(request, "testuser")
        with pytest.raises(DiskNameUsed):
            await service.create_disk(request, "testuser")

    async def test_can_create_disk_with_same_name_after_delete(
        self, service: Service
    ) -> None:
        org_name, project_name = uuid4().hex, uuid4().hex
        request = DiskRequest(
            storage=1024 * 1024,
            name="test",
            project_name=project_name,
            org_name=org_name,
        )
        disk = await service.create_disk(request, "testuser")
        await service.remove_disk(disk)
        await service.create_disk(request, "testuser")

    # As pvc deletion is async, we should check that user will never
    # see deleted disk, so next test is executed multiple times
    @pytest.mark.parametrize("execution_number", range(10))
    async def test_remove_disk(self, execution_number: int, service: Service) -> None:
        org_name, project_name = uuid4().hex, uuid4().hex
        request = DiskRequest(
            storage=1024 * 1024,
            project_name=project_name,
            org_name=org_name,
        )
        disk = await service.create_disk(request, "testuser")
        await service.remove_disk(disk)
        assert await service.get_all_disks() == []

    async def test_get_disk(self, service: Service) -> None:
        org_name, project_name = uuid4().hex, uuid4().hex
        request = DiskRequest(
            storage=1024 * 1024,
            project_name=project_name,
            org_name=org_name,
        )
        disk_created = await service.create_disk(request, "testuser")
        disk_get = await service.get_disk(
            disk_created.org_name, disk_created.project_name, disk_created.id
        )
        assert disk_get.id == disk_created.id
        assert disk_get.owner == disk_created.owner
        assert disk_get.storage >= disk_created.storage
        assert disk_get.project_name >= project_name

    async def test_get_disk_by_name(self, service: Service) -> None:
        org_name, project_name = uuid4().hex, uuid4().hex
        request = DiskRequest(
            storage=1024 * 1024,
            name="test-name",
            project_name=project_name,
            org_name=org_name,
        )
        disk_created = await service.create_disk(request, "testuser")
        disk_get = await service.get_disk_by_name("test-name", org_name, project_name)
        assert disk_get.id == disk_created.id
        assert disk_get.owner == disk_created.owner
        assert disk_get.storage >= disk_created.storage

    async def test_get_disk_by_name__if_owner_and_project_name_same(
        self, service: Service
    ) -> None:
        project_name = uuid4().hex
        request = DiskRequest(
            storage=1024 * 1024,
            name="test-name",
            project_name=project_name,
            org_name="any",
        )
        disk_created = await service.create_disk(request, project_name)

        disk_get = await service.get_disk_by_name("test-name", "any", project_name)
        assert disk_get.id == disk_created.id
        assert disk_get.owner == disk_created.owner
        assert disk_get.storage >= disk_created.storage

    async def test_get_non_existing_disk(self, service: Service) -> None:
        with pytest.raises(DiskNotFound):
            await service.get_disk("not-found", "not-found", "no-disk-for-this-name")

    async def test_remove_non_existing_disk(self, service: Service) -> None:
        disk = Disk(
            id="no-disk-for-this-name",
            storage=1,
            owner="no-disk-for-this-name",
            project_name="no-disk-for-this-name",
            name="no-disk-for-this-name",
            org_name="no-disk-for-this-name",
            status=Disk.Status.READY,
            created_at=datetime.datetime.now(),
            last_usage=None,
            life_span=None,
            used_bytes=None,
        )
        with pytest.raises(DiskNotFound):
            await service.remove_disk(disk)

    async def test_get_all_disk_ignores_outer_pvcs(
        self,
        kube_client: KubeClient,
        service: Service,
        scoped_namespace: tuple[Namespace, str, str],
    ) -> None:
        namespace, org, project = scoped_namespace
        await kube_client.create_pvc(
            "default",
            PersistentVolumeClaimWrite(
                name="outer-pvc", storage_class_name="no-way", storage=200
            ),
        )
        request = DiskRequest(
            storage=1024 * 1024,
            project_name=project,
            org_name=org,
        )
        disk_created = await service.create_disk(request, "testuser")
        all_disks = await service.get_all_disks(org, project)
        assert len(all_disks) == 1
        assert all_disks[0].id == disk_created.id

    async def test_get_all_disk_in_project(
        self,
        kube_client: KubeClient,
        service: Service,
        scoped_namespace: tuple[Namespace, str, str],
    ) -> None:
        namespace, org, project = scoped_namespace
        await kube_client.create_pvc(
            "default",
            PersistentVolumeClaimWrite(
                name="outer-pvc", storage_class_name="no-way", storage=200
            ),
        )
        request = DiskRequest(
            storage=1024 * 1024,
            project_name="other-test-project",
            org_name="other-org",
        )
        await service.create_disk(request, "testuser")
        request = DiskRequest(
            storage=1024 * 1024,
            project_name=project,
            org_name=org,
        )
        disk_created = await service.create_disk(request, "testuser")
        project_disks = await service.get_all_disks(org_name=org, project_name=project)
        assert len(project_disks) == 1
        assert project_disks[0].id == disk_created.id

    async def test_life_span_stored(self, service: Service) -> None:
        org_name, project_name = uuid4().hex, uuid4().hex
        life_span = timedelta(days=7)
        request = DiskRequest(
            storage=1024 * 1024,
            life_span=life_span,
            project_name=project_name,
            org_name=org_name,
        )
        disk = await service.create_disk(request, "testuser")
        disk = await service.get_disk(disk.org_name, disk.project_name, disk.id)
        assert disk.life_span == life_span

    async def test_no_life_span_stored(self, service: Service) -> None:
        org_name, project_name = uuid4().hex, uuid4().hex
        request = DiskRequest(
            storage=1024 * 1024,
            project_name=project_name,
            org_name=org_name,
        )
        disk = await service.create_disk(request, "testuser")
        disk = await service.get_disk(disk.org_name, disk.project_name, disk.id)
        assert disk.life_span is None

    async def test_update_last_usage(self, service: Service) -> None:
        org_name, project_name = uuid4().hex, uuid4().hex
        namespace = generate_namespace_name(org_name, project_name)
        request = DiskRequest(
            storage=1024 * 1024,
            project_name=project_name,
            org_name=org_name,
        )
        disk = await service.create_disk(request, "testuser")
        assert disk.last_usage is None
        last_usage_time = utc_now()
        await service.mark_disk_usage(namespace, disk.id, last_usage_time)
        disk = await service.get_disk(disk.org_name, disk.project_name, disk.id)
        assert disk.last_usage == last_usage_time
