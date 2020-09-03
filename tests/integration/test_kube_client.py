import asyncio
from uuid import uuid4

import pytest

from platform_disk_api.kube_client import (
    KubeClient,
    MergeDiff,
    PersistentVolumeClaimWrite,
    ResourceExists,
    ResourceNotFound,
)


pytestmark = pytest.mark.asyncio


class TestKubeClient:
    async def test_create_single_pvc(
        self, cleanup_pvcs: None, kube_client: KubeClient, k8s_storage_class: str
    ) -> None:
        pvc = await kube_client.create_pvc(
            PersistentVolumeClaimWrite(
                name=str(uuid4()),
                storage_class_name=k8s_storage_class,
                storage=10 * 1024 * 1024,  # 10 mb
            )
        )
        pvcs = await kube_client.list_pvc()
        assert len(pvcs) == 1
        assert pvcs[0].name == pvc.name

    async def test_add_label_to_pvc(
        self, cleanup_pvcs: None, kube_client: KubeClient, k8s_storage_class: str
    ) -> None:
        pvc = await kube_client.create_pvc(
            PersistentVolumeClaimWrite(
                name=str(uuid4()),
                storage_class_name=k8s_storage_class,
                storage=10 * 1024 * 1024,  # 10 mb
            )
        )
        diff = MergeDiff.make_add_label_diff("hello/world", "value")
        await kube_client.update_pvc(pvc.name, diff)
        pvc = await kube_client.get_pvc(pvc.name)
        assert pvc.labels == {"hello/world": "value"}

    async def test_no_real_storage_after_created(
        self, cleanup_pvcs: None, kube_client: KubeClient, k8s_storage_class: str
    ) -> None:
        storage_to_request = 10 * 1024 * 1024  # 10 mb

        pvc = await kube_client.create_pvc(
            PersistentVolumeClaimWrite(
                name=str(uuid4()),
                storage_class_name=k8s_storage_class,
                storage=storage_to_request,
            )
        )
        assert pvc.storage_requested == storage_to_request

    async def test_storage_is_auto_provided(
        self, cleanup_pvcs: None, kube_client: KubeClient, k8s_storage_class: str
    ) -> None:
        storage_to_request = 10 * 1024 * 1024  # 10 mb
        pvc = await kube_client.create_pvc(
            PersistentVolumeClaimWrite(
                name=str(uuid4()),
                storage_class_name=k8s_storage_class,
                storage=storage_to_request,
            )
        )

        async def wait_for_storage() -> None:
            while True:
                pvc_read = await kube_client.get_pvc(pvc.name)
                if pvc_read.storage_real is not None:
                    assert pvc_read.storage_real >= storage_to_request
                    break
                await asyncio.sleep(0.1)

        await asyncio.wait_for(wait_for_storage(), timeout=30)

    async def test_multiple_pvc(
        self, cleanup_pvcs: None, kube_client: KubeClient, k8s_storage_class: str
    ) -> None:
        pvc_count = 5
        names = [str(uuid4()) for _ in range(pvc_count)]
        for name in names:
            await kube_client.create_pvc(
                PersistentVolumeClaimWrite(
                    name=name,
                    storage_class_name=k8s_storage_class,
                    storage=1 * 1024 * 1024,  # 1 mb
                )
            )
        pvcs = await kube_client.list_pvc()
        assert len(pvcs) == pvc_count
        assert set(names) == set(pvc.name for pvc in pvcs)

    async def test_create_same_name(
        self, cleanup_pvcs: None, kube_client: KubeClient, k8s_storage_class: str
    ) -> None:
        pvc = PersistentVolumeClaimWrite(
            name=str(uuid4()),
            storage_class_name=k8s_storage_class,
            storage=10 * 1024 * 1024,  # 10 mb
        )
        await kube_client.create_pvc(pvc)
        with pytest.raises(ResourceExists):
            await kube_client.create_pvc(pvc)

    async def test_retrieve(
        self, cleanup_pvcs: None, kube_client: KubeClient, k8s_storage_class: str
    ) -> None:
        pvc_write = PersistentVolumeClaimWrite(
            name=str(uuid4()),
            storage_class_name=k8s_storage_class,
            storage=10 * 1024 * 1024,  # 10 mb
        )
        await kube_client.create_pvc(pvc_write)
        pvc_read = await kube_client.get_pvc(pvc_write.name)
        assert pvc_read.name == pvc_write.name

    async def test_create_with_labels(
        self, cleanup_pvcs: None, kube_client: KubeClient, k8s_storage_class: str
    ) -> None:
        pvc = PersistentVolumeClaimWrite(
            name=str(uuid4()),
            storage_class_name=k8s_storage_class,
            storage=10 * 1024 * 1024,  # 10 mb
            labels=dict(foo="bar"),
        )
        await kube_client.create_pvc(pvc)
        pvcs = await kube_client.list_pvc()
        assert len(pvcs) == 1
        assert pvcs[0].labels == pvc.labels

    async def test_retrieve_does_not_exists(
        self, cleanup_pvcs: None, kube_client: KubeClient, k8s_storage_class: str
    ) -> None:
        with pytest.raises(ResourceNotFound):
            await kube_client.get_pvc("not-exists")

    async def test_delete_does_not_exists(
        self, cleanup_pvcs: None, kube_client: KubeClient, k8s_storage_class: str
    ) -> None:
        with pytest.raises(ResourceNotFound):
            await kube_client.remove_pvc("not-exists")
