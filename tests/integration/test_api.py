from dataclasses import dataclass
from typing import AsyncIterator, Awaitable, Callable

import aiohttp
import pytest
from aiohttp.web import HTTPOk
from aiohttp.web_exceptions import HTTPCreated, HTTPForbidden, HTTPUnauthorized

from platform_disk_api.api import create_app
from platform_disk_api.config import Config
from platform_disk_api.schema import DiskSchema

from .conftest import ApiAddress, create_local_app_server
from .conftest_auth import _User


pytestmark = pytest.mark.asyncio


@dataclass(frozen=True)
class DiskApiEndpoints:
    address: ApiAddress

    @property
    def api_v1_endpoint(self) -> str:
        return f"http://{self.address.host}:{self.address.port}/api/v1"

    @property
    def ping_url(self) -> str:
        return f"{self.api_v1_endpoint}/ping"

    @property
    def secured_ping_url(self) -> str:
        return f"{self.api_v1_endpoint}/secured-ping"

    @property
    def disk_url(self) -> str:
        return f"{self.api_v1_endpoint}/disk"


@pytest.fixture
async def disk_api(config: Config) -> AsyncIterator[DiskApiEndpoints]:
    app = await create_app(config)
    async with create_local_app_server(app, port=8080) as address:
        yield DiskApiEndpoints(address=address)


class TestApi:
    async def test_ping(
        self, disk_api: DiskApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(disk_api.ping_url) as resp:
            assert resp.status == HTTPOk.status_code
            text = await resp.text()
            assert text == "Pong"

    async def test_secured_ping(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        admin_token: str,
    ) -> None:
        headers = {"Authorization": f"Bearer {admin_token}"}
        async with client.get(disk_api.secured_ping_url, headers=headers) as resp:
            assert resp.status == HTTPOk.status_code
            text = await resp.text()
            assert text == "Secured Pong"

    async def test_secured_ping_no_token_provided_unauthorized(
        self, disk_api: DiskApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        url = disk_api.secured_ping_url
        async with client.get(url) as resp:
            assert resp.status == HTTPUnauthorized.status_code

    async def test_secured_ping_non_existing_token_unauthorized(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        token_factory: Callable[[str], str],
    ) -> None:
        url = disk_api.secured_ping_url
        token = token_factory("non-existing-user")
        headers = {"Authorization": f"Bearer {token}"}
        async with client.get(url, headers=headers) as resp:
            assert resp.status == HTTPUnauthorized.status_code

    async def test_ping_unknown_origin(
        self, disk_api: DiskApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(
            disk_api.ping_url, headers={"Origin": "http://unknown"}
        ) as response:
            assert response.status == HTTPOk.status_code, await response.text()
            assert "Access-Control-Allow-Origin" not in response.headers

    async def test_ping_allowed_origin(
        self, disk_api: DiskApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(
            disk_api.ping_url, headers={"Origin": "https://neu.ro"}
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            assert resp.headers["Access-Control-Allow-Origin"] == "https://neu.ro"
            assert resp.headers["Access-Control-Allow-Credentials"] == "true"
            assert resp.headers["Access-Control-Expose-Headers"] == ""

    async def test_ping_options_no_headers(
        self, disk_api: DiskApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.options(disk_api.ping_url) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()
            assert await resp.text() == (
                "CORS preflight request failed: "
                "origin header is not specified in the request"
            )

    async def test_ping_options_unknown_origin(
        self, disk_api: DiskApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.options(
            disk_api.ping_url,
            headers={
                "Origin": "http://unknown",
                "Access-Control-Request-Method": "GET",
            },
        ) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()
            assert await resp.text() == (
                "CORS preflight request failed: "
                "origin 'http://unknown' is not allowed"
            )

    async def test_ping_options(
        self, disk_api: DiskApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.options(
            disk_api.ping_url,
            headers={
                "Origin": "https://neu.ro",
                "Access-Control-Request-Method": "GET",
            },
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            assert resp.headers["Access-Control-Allow-Origin"] == "https://neu.ro"
            assert resp.headers["Access-Control-Allow-Credentials"] == "true"
            assert resp.headers["Access-Control-Allow-Methods"] == "GET"

    async def test_disk_create(
        self,
        cleanup_pvcs: None,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Awaitable[_User]],
    ) -> None:
        user = await regular_user_factory()
        async with client.post(
            disk_api.disk_url,
            json={"name": "test-disk", "storage": 500},
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            disk = DiskSchema().load(await resp.json())
            assert disk.name == "test-disk"
            assert disk.storage >= 500
