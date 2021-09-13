from dataclasses import dataclass, replace
from typing import AsyncIterator, Awaitable, Callable, List

import aiohttp
import pytest
from aiohttp.web import HTTPOk
from aiohttp.web_exceptions import (
    HTTPCreated,
    HTTPForbidden,
    HTTPNoContent,
    HTTPNotFound,
    HTTPUnauthorized,
)
from neuro_auth_client import AuthClient, Permission, User

from platform_disk_api.api import create_app
from platform_disk_api.config import Config
from platform_disk_api.schema import DiskSchema
from platform_disk_api.service import Disk

from .auth import _User
from .conftest import ApiAddress, create_local_app_server


pytestmark = pytest.mark.asyncio


@dataclass(frozen=True)
class DiskApiEndpoints:
    address: ApiAddress

    @property
    def server_base_url(self) -> str:
        return f"http://{self.address.host}:{self.address.port}"

    @property
    def api_v1_endpoint(self) -> str:
        return f"{self.server_base_url}/api/v1"

    @property
    def openapi_json_url(self) -> str:
        return f"{self.server_base_url}/api/docs/v1/disk/swagger.json"

    @property
    def ping_url(self) -> str:
        return f"{self.api_v1_endpoint}/ping"

    @property
    def secured_ping_url(self) -> str:
        return f"{self.api_v1_endpoint}/secured-ping"

    @property
    def disk_url(self) -> str:
        return f"{self.api_v1_endpoint}/disk"

    def single_disk_url(self, disk_name: str) -> str:
        return f"{self.api_v1_endpoint}/disk/{disk_name}"


@pytest.fixture
async def disk_api(config: Config) -> AsyncIterator[DiskApiEndpoints]:
    app = await create_app(config)
    async with create_local_app_server(app, port=8080) as address:
        yield DiskApiEndpoints(address=address)


@pytest.fixture
async def grant_disk_permission(
    auth_client: AuthClient,
    token_factory: Callable[[str], str],
    admin_token: str,
    cluster_name: str,
) -> AsyncIterator[Callable[[User, Disk], Awaitable[None]]]:
    async def _grant(user: User, disk: Disk) -> None:
        permission = Permission(
            uri=f"disk://{cluster_name}/{disk.owner}/{disk.id}", action="read"
        )
        await auth_client.grant_user_permissions(user.name, [permission], admin_token)

    yield _grant


class TestApi:
    async def test_doc_available_when_enabled(
        self, config: Config, client: aiohttp.ClientSession
    ) -> None:
        config = replace(config, enable_docs=True)
        app = await create_app(config)
        async with create_local_app_server(app, port=8080) as address:
            endpoints = DiskApiEndpoints(address=address)
            async with client.get(endpoints.openapi_json_url) as resp:
                assert resp.status == HTTPOk.status_code
                assert await resp.json()

    async def test_no_docs_when_disabled(
        self, config: Config, client: aiohttp.ClientSession
    ) -> None:
        config = replace(config, enable_docs=False)
        app = await create_app(config)
        async with create_local_app_server(app, port=8080) as address:
            endpoints = DiskApiEndpoints(address=address)
            async with client.get(endpoints.openapi_json_url) as resp:
                assert resp.status == HTTPNotFound.status_code

    async def test_ping(
        self, disk_api: DiskApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(disk_api.ping_url) as resp:
            assert resp.status == HTTPOk.status_code
            text = await resp.text()
            assert text == "Pong"

    async def test_ping_includes_version(
        self, disk_api: DiskApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(disk_api.ping_url) as resp:
            assert resp.status == HTTPOk.status_code
            assert "platform-disk-api" in resp.headers["X-Service-Version"]

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
            # TODO: re-enable this when aiohttp-cors is updated
            # assert resp.headers["Access-Control-Expose-Headers"] == ""

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
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Awaitable[_User]],
    ) -> None:
        user = await regular_user_factory()
        async with client.post(
            disk_api.disk_url,
            json={"storage": 500},
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            disk: Disk = DiskSchema().load(await resp.json())
            assert disk.owner == user.name
            assert disk.storage >= 500

    async def test_disk_create_username_with_slash(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[str], Awaitable[_User]],
    ) -> None:
        await regular_user_factory("test")
        user = await regular_user_factory("test/with/additional/parts")
        async with client.post(
            disk_api.disk_url,
            json={"storage": 500, "name": "test"},
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            disk: Disk = DiskSchema().load(await resp.json())
            assert disk.owner == user.name
            assert disk.storage >= 500
        async with client.get(
            disk_api.disk_url,
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            disks: List[Disk] = DiskSchema(many=True).load(await resp.json())
            assert len(disks) == 1
            assert disks[0] == disk

    async def test_storage_limit_single_disk(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Awaitable[_User]],
        config: Config,
    ) -> None:
        user = await regular_user_factory()
        async with client.post(
            disk_api.disk_url,
            json={"storage": config.disk.storage_limit_per_user + 100},
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()
            assert (await resp.json())["code"] == "over_limit"

    async def test_storage_limit_multiple_disk(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Awaitable[_User]],
        config: Config,
    ) -> None:
        user = await regular_user_factory()
        await client.post(
            disk_api.disk_url,
            json={"storage": config.disk.storage_limit_per_user - 100},
            headers=user.headers,
        )
        async with client.post(
            disk_api.disk_url,
            json={"storage": 200},
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()
            assert (await resp.json())["code"] == "over_limit"

    async def test_list_disk_includes_only_own(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Awaitable[_User]],
    ) -> None:
        user1 = await regular_user_factory()
        user2 = await regular_user_factory()
        user_1_disks = []
        user_2_disks = []
        for _ in range(3):
            async with client.post(
                disk_api.disk_url,
                json={"storage": 500},
                headers=user1.headers,
            ) as resp:
                disk = DiskSchema().load(await resp.json())
                user_1_disks.append(disk.id)
        for _ in range(4):
            async with client.post(
                disk_api.disk_url,
                json={"storage": 500},
                headers=user2.headers,
            ) as resp:
                disk = DiskSchema().load(await resp.json())
                user_2_disks.append(disk.id)
        async with client.get(
            disk_api.disk_url,
            headers=user1.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            disks: List[Disk] = DiskSchema(many=True).load(await resp.json())
            assert len(disks) == len(user_1_disks)
            assert {disk.id for disk in disks} == set(user_1_disks)
        async with client.get(
            disk_api.disk_url,
            headers=user2.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            disks = DiskSchema(many=True).load(await resp.json())
            assert len(disks) == len(user_2_disks)
            assert {disk.id for disk in disks} == set(user_2_disks)

    async def test_list_disk_includes_shared_disk(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Awaitable[_User]],
        grant_disk_permission: Callable[[_User, Disk], Awaitable[None]],
    ) -> None:
        user1 = await regular_user_factory()
        user2 = await regular_user_factory()
        async with await client.post(
            disk_api.disk_url,
            json={"storage": 500},
            headers=user1.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code
            disk = DiskSchema().load(await resp.json())
        async with client.get(
            disk_api.disk_url,
            headers=user2.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            assert await resp.json() == []
        await grant_disk_permission(user2, disk)
        async with client.get(
            disk_api.disk_url,
            headers=user2.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            disks: List[Disk] = DiskSchema(many=True).load(await resp.json())
            assert len(disks) == 1
            assert disks[0].id == disk.id

    async def test_can_delete_own_disk(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Awaitable[_User]],
    ) -> None:
        user = await regular_user_factory()
        async with await client.post(
            disk_api.disk_url,
            json={"storage": 500},
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code
            disk = DiskSchema().load(await resp.json())
        async with await client.delete(
            disk_api.single_disk_url(disk.id),
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPNoContent.status_code
        async with await client.get(
            disk_api.disk_url,
            headers=user.headers,
        ) as resp:
            assert await resp.json() == []

    async def test_cannot_delete_another_disk(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Awaitable[_User]],
    ) -> None:
        user1 = await regular_user_factory()
        user2 = await regular_user_factory()
        async with await client.post(
            disk_api.disk_url,
            json={"storage": 500},
            headers=user1.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code
            disk = DiskSchema().load(await resp.json())
        async with await client.delete(
            disk_api.single_disk_url(disk.id),
            headers=user2.headers,
        ) as resp:
            assert resp.status == HTTPForbidden.status_code
        async with await client.get(
            disk_api.disk_url,
            headers=user1.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            disks: List[Disk] = DiskSchema(many=True).load(await resp.json())
            assert len(disks) == 1
            assert disks[0].id == disk.id

    async def test_get_disk(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Awaitable[_User]],
    ) -> None:
        user = await regular_user_factory()
        async with await client.post(
            disk_api.disk_url,
            json={"storage": 500},
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code
            disk = DiskSchema().load(await resp.json())
        async with await client.get(
            disk_api.single_disk_url(disk.id),
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code
            disk_got = DiskSchema().load(await resp.json())
            assert disk.id == disk_got.id

    async def test_get_disk_by_name(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Awaitable[_User]],
    ) -> None:
        user = await regular_user_factory()
        async with await client.post(
            disk_api.disk_url,
            json={"storage": 500, "name": "test-name"},
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code
            disk = DiskSchema().load(await resp.json())
        async with await client.get(
            disk_api.single_disk_url(disk.name),
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code
            disk_got = DiskSchema().load(await resp.json())
            assert disk.id == disk_got.id
        async with await client.delete(
            disk_api.single_disk_url(disk.name),
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPNoContent.status_code

    async def test_get_wrong_id(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Awaitable[_User]],
    ) -> None:
        user = await regular_user_factory()
        async with await client.get(
            disk_api.single_disk_url("wrong-id"),
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPNotFound.status_code

    async def test_delete_wrong_id(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Awaitable[_User]],
    ) -> None:
        user = await regular_user_factory()
        async with await client.delete(
            disk_api.single_disk_url("wrong-id"),
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPNotFound.status_code
