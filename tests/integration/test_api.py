from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, replace

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

from platform_disk_api.api import create_app
from platform_disk_api.config import Config
from platform_disk_api.schema import DiskSchema
from platform_disk_api.service import Disk
from tests.integration.conftest import ApiAddress, create_local_app_server
from tests.integration.conftest_admin import _User


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

    def org_disk_url(self, org_name: str) -> str:
        return f"{self.api_v1_endpoint}/disk?org_name={org_name}"

    def project_disk_url(self, project_name: str) -> str:
        return f"{self.api_v1_endpoint}/disk?project_name={project_name}"

    def single_disk_url(self, disk_name: str) -> str:
        return f"{self.api_v1_endpoint}/disk/{disk_name}"


@pytest.fixture
async def disk_api(config: Config) -> AsyncIterator[DiskApiEndpoints]:
    app = await create_app(config)
    async with create_local_app_server(app, port=8080) as address:
        yield DiskApiEndpoints(address=address)


@pytest.fixture
def org(org_project: tuple[str, str]) -> str:
    org, _ = org_project
    return org


@pytest.fixture
def project(org_project: tuple[str, str]) -> str:
    _, project = org_project
    return project


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
                "CORS preflight request failed: origin 'http://unknown' is not allowed"
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
        regular_user_factory: Callable[..., Awaitable[_User]],
        project: str,
        org: str,
    ) -> None:
        user = await regular_user_factory(project_name=project, org_name=org)
        async with client.post(
            disk_api.disk_url,
            json={"storage": 500, "project_name": project, "org_name": org},
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            disk: Disk = DiskSchema().load(await resp.json())
            assert disk.owner == user.name
            assert disk.storage >= 500

    async def test_disk_create_with_default_project(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        project: str,
        org: str,
    ) -> None:
        user = await regular_user_factory(project_name=project, org_name=org)
        async with client.post(
            disk_api.disk_url,
            json={"storage": 500, "project_name": project, "org_name": org},
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            disk: Disk = DiskSchema().load(await resp.json())
            assert disk.owner == user.name
            assert disk.project_name == project
            assert disk.storage >= 500

    async def test_disk_create_with_org(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        project: str,
        org: str,
    ) -> None:
        user = await regular_user_factory(project_name=project, org_name=org)
        async with client.post(
            disk_api.disk_url,
            json={"storage": 500, "org_name": org, "project_name": project},
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            disk: Disk = DiskSchema().load(await resp.json())
            assert disk.owner == user.name
            assert disk.storage >= 500
            assert disk.org_name == org

    async def test_disk_create_username_with_hyphen(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        project: str,
        org: str,
    ) -> None:
        """Test that usernames with hyphens work correctly."""
        await regular_user_factory("test")
        user = await regular_user_factory(
            "test-with-additional-parts", project_name=project, org_name=org
        )
        async with client.post(
            disk_api.disk_url,
            json={
                "storage": 500,
                "name": "test",
                "project_name": project,
                "org_name": org,
            },
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            disk: Disk = DiskSchema().load(await resp.json())
            assert disk.owner == user.name
            assert disk.storage >= 500
        async with client.get(
            disk_api.disk_url,
            headers=user.headers,
            params={"project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            disks: list[Disk] = DiskSchema(many=True).load(await resp.json())
            assert len(disks) == 1
            assert disks[0].id == disk.id
            assert disks[0].storage == disk.storage
            assert disks[0].name == disk.name

    async def test_disk_create_project_unauthorized(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        project: str,
        org: str,
    ) -> None:
        user = await regular_user_factory(project_name=project, org_name=org)
        async with client.post(
            disk_api.disk_url,
            json={
                "storage": 500,
                "project_name": "other-test-project",
                "org_name": "other-test-org",
            },
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()

    async def test_storage_limit_single_disk(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        config: Config,
        project: str,
        org: str,
    ) -> None:
        user = await regular_user_factory(project_name=project, org_name=org)
        async with client.post(
            disk_api.disk_url,
            json={
                "storage": config.disk.storage_limit_per_project + 100,
                "project_name": project,
                "org_name": org,
            },
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()
            assert (await resp.json())["code"] == "over_limit"

    async def test_storage_limit_multiple_disk(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        config: Config,
        project: str,
        org: str,
    ) -> None:
        user = await regular_user_factory(project_name=project, org_name=org)
        await client.post(
            disk_api.disk_url,
            json={
                "storage": config.disk.storage_limit_per_project - 100,
                "project_name": project,
                "org_name": org,
            },
            headers=user.headers,
        )
        async with client.post(
            disk_api.disk_url,
            json={"storage": 200, "project_name": project, "org_name": org},
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()
            assert (await resp.json())["code"] == "over_limit"

    async def test_list_disk_includes_only_own(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        project: str,
        org: str,
    ) -> None:
        """Test that users can only list disks in their own projects."""
        project1 = project
        project2 = f"{project}2"
        user1 = await regular_user_factory(project_name=project1, org_name=org)
        user2 = await regular_user_factory(project_name=project2, org_name=org)
        user_1_disks = []
        user_2_disks = []
        for _ in range(3):
            async with client.post(
                disk_api.disk_url,
                json={"storage": 500, "project_name": project1, "org_name": org},
                headers=user1.headers,
            ) as resp:
                assert resp.status == HTTPCreated.status_code, await resp.text()
                disk = DiskSchema().load(await resp.json())
                user_1_disks.append(disk.id)
        for _ in range(4):
            async with client.post(
                disk_api.disk_url,
                json={"storage": 500, "project_name": project2, "org_name": org},
                headers=user2.headers,
            ) as resp:
                assert resp.status == HTTPCreated.status_code, await resp.text()
                disk = DiskSchema().load(await resp.json())
                user_2_disks.append(disk.id)
        async with client.get(
            disk_api.disk_url,
            headers=user1.headers,
            params={"project_name": project1, "org_name": org},
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            disks: list[Disk] = DiskSchema(many=True).load(await resp.json())
            assert len(disks) == len(user_1_disks)
            assert {disk.id for disk in disks} == set(user_1_disks)
        async with client.get(
            disk_api.disk_url,
            headers=user2.headers,
            params={"project_name": project2, "org_name": org},
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            disks = DiskSchema(many=True).load(await resp.json())
            assert len(disks) == len(user_2_disks)
            assert {disk.id for disk in disks} == set(user_2_disks)

    async def test_list_disk_no_permission_returns_forbidden(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
    ) -> None:
        """Test that listing disks without permission returns 403 Forbidden."""
        user1 = await regular_user_factory(
            project_name="test-project1", org_name="test-org1"
        )
        user2 = await regular_user_factory(
            project_name="test-project2", org_name="test-org2"
        )
        # user1 creates a disk in their project
        async with await client.post(
            disk_api.disk_url,
            json={
                "storage": 500,
                "project_name": "test-project1",
                "org_name": "test-org1",
            },
            headers=user1.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code
        # user2 tries to list disks in user1's project - should get 403 Forbidden
        async with client.get(
            disk_api.disk_url,
            headers=user2.headers,
            params={
                "project_name": "test-project1",
                "org_name": "test-org1",
            },
        ) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()

    async def test_list_disk_in_project(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        project: str,
        org: str,
    ) -> None:
        user = await regular_user_factory(project_name=project, org_name=org)
        async with await client.post(
            disk_api.disk_url,
            json={"storage": 500, "project_name": project, "org_name": org},
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code
            disk1 = DiskSchema().load(await resp.json())
        async with await client.post(
            disk_api.disk_url,
            json={"storage": 500, "project_name": project, "org_name": org},
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code

        async with client.get(
            disk_api.project_disk_url(project),
            headers=user.headers,
            params={"project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            disks: list[Disk] = DiskSchema(many=True).load(await resp.json())
            disk_ids = [d.id for d in disks]
            assert disk1.id in disk_ids, disks

    async def test_list_disk_in_project__owner_and_project_name_same(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        project: str,
        org: str,
    ) -> None:
        user = await regular_user_factory(project, project_name=project, org_name=org)
        async with await client.post(
            disk_api.disk_url,
            json={"storage": 500, "project_name": project, "org_name": org},
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code
            disk = DiskSchema().load(await resp.json())

        async with client.get(
            disk_api.disk_url,
            headers=user.headers,
            params={"project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            disks: list[Disk] = DiskSchema(many=True).load(await resp.json())
            disk_ids = [d.id for d in disks]
            assert disk.id in disk_ids, disks

    async def test_can_delete_own_disk(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        project: str,
        org: str,
    ) -> None:
        user = await regular_user_factory(project_name=project, org_name=org)
        async with await client.post(
            disk_api.disk_url,
            json={"storage": 500, "project_name": project, "org_name": org},
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code
            disk = DiskSchema().load(await resp.json())

        async with await client.delete(
            disk_api.single_disk_url(disk.id),
            headers=user.headers,
            params={"project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPNoContent.status_code
        async with await client.get(
            disk_api.disk_url,
            headers=user.headers,
            params={"project_name": project, "org_name": org},
        ) as resp:
            assert await resp.json() == []

    async def test_cannot_delete_another_disk(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        project: str,
        org: str,
    ) -> None:
        user1 = await regular_user_factory(project_name=project, org_name=org)
        user2 = await regular_user_factory(
            project_name=f"{project}2", org_name=f"{org}2"
        )
        async with await client.post(
            disk_api.disk_url,
            json={"storage": 500, "project_name": project, "org_name": org},
            headers=user1.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code
            disk = DiskSchema().load(await resp.json())
        async with await client.delete(
            disk_api.single_disk_url(disk.id),
            headers=user2.headers,
            params={"project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPForbidden.status_code
        async with await client.get(
            disk_api.disk_url,
            headers=user1.headers,
            params={"project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            disks: list[Disk] = DiskSchema(many=True).load(await resp.json())
            assert len(disks) == 1
            assert disks[0].id == disk.id

    async def test_cannot_delete_another_project_disk(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        project: str,
        org: str,
    ) -> None:
        user1 = await regular_user_factory(project_name=project, org_name=org)
        user2 = await regular_user_factory(
            project_name=f"{project}2", org_name=f"{org}2"
        )
        async with await client.post(
            disk_api.disk_url,
            json={"storage": 500, "project_name": project, "org_name": org},
            headers=user1.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code
            disk = DiskSchema().load(await resp.json())
        async with await client.delete(
            disk_api.single_disk_url(disk.id),
            headers=user2.headers,
            params={"project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPForbidden.status_code
        async with await client.get(
            disk_api.disk_url,
            headers=user1.headers,
            params={"project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            disks: list[Disk] = DiskSchema(many=True).load(await resp.json())
            assert len(disks) == 1
            assert disks[0].id == disk.id

    async def test_get_disk(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        project: str,
        org: str,
    ) -> None:
        user = await regular_user_factory(project_name=project, org_name=org)
        async with await client.post(
            disk_api.disk_url,
            json={"storage": 500, "project_name": project, "org_name": org},
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code
            disk = DiskSchema().load(await resp.json())
        async with await client.get(
            disk_api.single_disk_url(disk.id),
            headers=user.headers,
            params={"project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPOk.status_code
            disk_got = DiskSchema().load(await resp.json())
            assert disk.id == disk_got.id

    async def test_get_disk_by_name(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        project: str,
        org: str,
    ) -> None:
        user = await regular_user_factory(project_name=project, org_name=org)
        async with await client.post(
            disk_api.disk_url,
            json={
                "storage": 500,
                "name": "test-name",
                "project_name": project,
                "org_name": org,
            },
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code
            disk = DiskSchema().load(await resp.json())
        async with await client.get(
            disk_api.single_disk_url(disk.name),
            headers=user.headers,
            params={"project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPOk.status_code
            disk_got = DiskSchema().load(await resp.json())
            assert disk.id == disk_got.id
        async with await client.delete(
            disk_api.single_disk_url(disk.name),
            headers=user.headers,
            params={"project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPNoContent.status_code

    async def test_get_disk_by_name__owner_and_project_name_same(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        project: str,
        org: str,
    ) -> None:
        user = await regular_user_factory(project, project_name=project, org_name=org)
        async with client.post(
            disk_api.disk_url,
            json={
                "storage": 500,
                "name": "test-name",
                "project_name": project,
                "org_name": org,
            },
            headers=user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            disk = DiskSchema().load(await resp.json())
        async with await client.get(
            disk_api.single_disk_url(disk.name),
            headers=user.headers,
            params={"project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPOk.status_code
            disk_got = DiskSchema().load(await resp.json())
            assert disk.id == disk_got.id
        async with await client.get(
            disk_api.single_disk_url(disk.name),
            headers=user.headers,
            params={"owner": user.name, "project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPOk.status_code
            disk_got = DiskSchema().load(await resp.json())
            assert disk.id == disk_got.id
        async with await client.delete(
            disk_api.single_disk_url(disk.name),
            headers=user.headers,
            params={"project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPNoContent.status_code

    async def test_get_shared_disk_by_name(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        project: str,
        org: str,
    ) -> None:
        """Test that users in the same project can access each other's disks."""
        user1 = await regular_user_factory(project_name=project, org_name=org)
        user2 = await regular_user_factory(project_name=project, org_name=org)
        async with await client.post(
            disk_api.disk_url,
            json={
                "storage": 500,
                "name": "test-name",
                "project_name": project,
                "org_name": org,
            },
            headers=user1.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code
            disk = DiskSchema().load(await resp.json())
        # user2 is in the same project, so should have access to the disk
        async with await client.get(
            disk_api.single_disk_url(disk.name),
            headers=user2.headers,
            params={"owner": user1.name, "project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPOk.status_code
            disk_got = DiskSchema().load(await resp.json())
            assert disk.id == disk_got.id
        async with await client.delete(
            disk_api.single_disk_url(disk.name),
            headers=user2.headers,
            params={"owner": user1.name, "project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPNoContent.status_code

    async def test_get_wrong_id(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        project: str,
        org: str,
    ) -> None:
        user = await regular_user_factory(project_name=project, org_name=org)
        async with await client.get(
            disk_api.single_disk_url("wrong-id"),
            headers=user.headers,
            params={"project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPNotFound.status_code

    async def test_delete_wrong_id(
        self,
        disk_api: DiskApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
        project: str,
        org: str,
    ) -> None:
        user = await regular_user_factory(project_name=project, org_name=org)
        async with await client.delete(
            disk_api.single_disk_url("wrong-id"),
            headers=user.headers,
            params={"project_name": project, "org_name": org},
        ) as resp:
            assert resp.status == HTTPNotFound.status_code
