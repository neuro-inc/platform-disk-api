from typing import AsyncIterator
from uuid import uuid4

import pytest

from platform_disk_api.kube_client import KubeClient, PersistentVolumeClaimWrite


pytestmark = pytest.mark.asyncio


class TestKubeClient:
    @pytest.fixture
    async def cleanup_pvcs(self, kube_client: KubeClient) -> AsyncIterator[None]:
        for pvc in await kube_client.list_pvc():
            await kube_client.remove_pvc(pvc.name)
        yield
        for pvc in await kube_client.list_pvc():
            await kube_client.remove_pvc(pvc.name)

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
