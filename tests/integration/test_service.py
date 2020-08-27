import pytest

from platform_disk_api.kube_client import KubeClient
from platform_disk_api.service import Service


pytestmark = pytest.mark.asyncio


class TestService:
    @pytest.fixture
    def service(self, kube_client: KubeClient) -> Service:
        return Service(kube_client=kube_client)

    async def test_created(self, service: Service) -> None:
        assert service
