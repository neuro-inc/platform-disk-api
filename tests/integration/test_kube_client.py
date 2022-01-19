import asyncio
from uuid import uuid4

import pytest

from platform_disk_api.kube_client import (
    DiskNaming,
    KubeClient,
    MergeDiff,
    PersistentVolumeClaimWrite,
    ResourceExists,
    ResourceNotFound,
)

from tests.integration.kube import KubeClientForTest



class TestKubeClient:
    async def test_create_single_pvc(
        self, kube_client: KubeClient, k8s_storage_class: str
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

    async def test_create_single_pvc_with_default_storage_class(
        self, kube_client: KubeClient
    ) -> None:
        pvc = await kube_client.create_pvc(
            PersistentVolumeClaimWrite(
                name=str(uuid4()),
                storage=10 * 1024 * 1024,  # 10 mb
            )
        )
        pvcs = await kube_client.list_pvc()
        assert len(pvcs) == 1
        assert pvcs[0].name == pvc.name
        assert pvcs[0].storage_class_name == "standard"

    async def test_add_label_to_pvc(
        self, kube_client: KubeClient, k8s_storage_class: str
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

    async def test_add_annotations_to_pvc(
        self, kube_client: KubeClient, k8s_storage_class: str
    ) -> None:
        pvc = await kube_client.create_pvc(
            PersistentVolumeClaimWrite(
                name=str(uuid4()),
                storage_class_name=k8s_storage_class,
                storage=10 * 1024 * 1024,  # 10 mb
            )
        )
        diff = MergeDiff.make_add_annotations_diff("hello/world", "value")
        await kube_client.update_pvc(pvc.name, diff)
        pvc = await kube_client.get_pvc(pvc.name)
        assert pvc.annotations.get("hello/world") == "value"

    async def test_no_real_storage_after_created(
        self, kube_client: KubeClient, k8s_storage_class: str
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
        self, kube_client: KubeClient, k8s_storage_class: str
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
        self, kube_client: KubeClient, k8s_storage_class: str
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
        assert set(names) == {pvc.name for pvc in pvcs}

    async def test_create_same_name(
        self, kube_client: KubeClient, k8s_storage_class: str
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
        self, kube_client: KubeClient, k8s_storage_class: str
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
        self, kube_client: KubeClient, k8s_storage_class: str
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

    async def test_create_with_annotations(
        self, kube_client: KubeClient, k8s_storage_class: str
    ) -> None:
        pvc = PersistentVolumeClaimWrite(
            name=str(uuid4()),
            storage_class_name=k8s_storage_class,
            storage=10 * 1024 * 1024,  # 10 mb
            annotations=dict(foo="bar"),
        )
        await kube_client.create_pvc(pvc)
        pvcs = await kube_client.list_pvc()
        assert len(pvcs) == 1
        assert pvcs[0].annotations.get("foo") == "bar"

    async def test_retrieve_does_not_exists(
        self, kube_client: KubeClient, k8s_storage_class: str
    ) -> None:
        with pytest.raises(ResourceNotFound):
            await kube_client.get_pvc("not-exists")

    async def test_delete_does_not_exists(
        self, kube_client: KubeClient, k8s_storage_class: str
    ) -> None:
        with pytest.raises(ResourceNotFound):
            await kube_client.remove_pvc("not-exists")

    async def test_list_pods(
        self, kube_client: KubeClientForTest, k8s_storage_class: str
    ) -> None:
        storage_to_request = 10 * 1024 * 1024  # 10 mb
        pvc = await kube_client.create_pvc(
            PersistentVolumeClaimWrite(
                name=str(uuid4()),
                storage_class_name=k8s_storage_class,
                storage=storage_to_request,
            )
        )
        async with kube_client.run_pod([pvc.name]) as created_pod:
            list_res = await kube_client.list_pods()
            assert pvc.name in created_pod.pvc_in_use
            assert created_pod in list_res.pods

    async def test_watch_pods(
        self, kube_client: KubeClientForTest, k8s_storage_class: str
    ) -> None:
        storage_to_request = 10 * 1024 * 1024  # 10 mb
        pvc1 = await kube_client.create_pvc(
            PersistentVolumeClaimWrite(
                name=str(uuid4()),
                storage_class_name=k8s_storage_class,
                storage=storage_to_request,
            )
        )
        pvc2 = await kube_client.create_pvc(
            PersistentVolumeClaimWrite(
                name=str(uuid4()),
                storage_class_name=k8s_storage_class,
                storage=storage_to_request,
            )
        )

        seen_pvc = set()

        async def watcher() -> None:
            async for event in kube_client.watch_pods():
                seen_pvc.update(event.pod.pvc_in_use)

        task = asyncio.create_task(watcher())

        async with kube_client.run_pod([pvc1.name]):
            await asyncio.sleep(0.5)

        assert pvc1.name in seen_pvc
        assert pvc2.name not in seen_pvc

        async with kube_client.run_pod([pvc2.name]):
            await asyncio.sleep(0.5)

        assert pvc1.name in seen_pvc
        assert pvc2.name in seen_pvc

        task.cancel()

    async def test_get_stats(
        self, kube_client: KubeClientForTest, k8s_storage_class: str
    ) -> None:
        # This stats is not supported by minikube, so no way to test it
        assert [metric async for metric in kube_client.get_pvc_volumes_metrics()] == []

    async def test_disk_naming_crud(self, kube_client: KubeClient) -> None:
        assert await kube_client.list_disk_namings() == []
        disk_name = DiskNaming(name="owner-user", disk_id="testing")
        await kube_client.create_disk_naming(disk_name)
        assert await kube_client.get_disk_naming(disk_name.name) == disk_name
        assert await kube_client.list_disk_namings() == [disk_name]
        await kube_client.remove_disk_naming(disk_name.name)
        assert await kube_client.list_disk_namings() == []
        with pytest.raises(ResourceNotFound):
            await kube_client.get_disk_naming(disk_name.name)
