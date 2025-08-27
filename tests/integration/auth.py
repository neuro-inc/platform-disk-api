import asyncio
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Callable,
    Coroutine,
)
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import aiodocker
import pytest
from aiodocker.containers import DockerContainer
from aiodocker.utils import JSONObject
from aiohttp import ClientError
from aiohttp.hdrs import AUTHORIZATION
from jose import jwt
from neuro_auth_client import AuthClient, Permission, User as AuthClientUser
from yarl import URL

from platform_disk_api.config import AuthConfig
from tests.integration.conftest import random_name


@pytest.fixture(scope="session")
def auth_server_image_name() -> str:
    with open("PLATFORMAUTHAPI_IMAGE") as f:
        return f.read().strip()


@pytest.fixture(scope="session")
async def auth_server(
    docker: aiodocker.Docker, reuse_docker: bool, auth_server_image_name: str
) -> AsyncIterator[AuthConfig]:
    image_name = auth_server_image_name
    container_name = "auth_server"
    container_config: JSONObject = {
        "Image": image_name,
        "AttachStdout": False,
        "AttachStderr": False,
        "HostConfig": {"PublishAllPorts": True},
        "Env": ["NP_JWT_SECRET=secret"],
    }

    if reuse_docker:
        try:
            container = await docker.containers.get(container_name)
            if container["State"]["Running"]:
                auth_config = await create_auth_config(container)
                await wait_for_auth_server(auth_config)
                yield auth_config
                return
        except aiodocker.exceptions.DockerError:
            pass

    try:
        await docker.images.inspect(auth_server_image_name)
    except aiodocker.exceptions.DockerError:
        await docker.images.pull(auth_server_image_name)

    container = await docker.containers.create_or_replace(
        name=container_name, config=container_config
    )
    await container.start()

    auth_config = await create_auth_config(container)
    await wait_for_auth_server(auth_config)
    yield auth_config

    if not reuse_docker:
        await container.kill()
        await container.delete(force=True)


def create_token(name: str) -> str:
    payload = {"identity": name}
    return jwt.encode(payload, "secret", algorithm="HS256")


@pytest.fixture(scope="session")
def token_factory() -> Callable[[str], str]:
    return create_token


@pytest.fixture
def admin_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("admin")


async def create_auth_config(
    container: DockerContainer,
) -> AuthConfig:
    host = "0.0.0.0"
    port_info = await container.port(8080)
    if not port_info:
        raise RuntimeError("Port 8080 not mapped in the container!")
    port = int(port_info[0]["HostPort"])
    url = URL(f"http://{host}:{port}")
    token = create_token("compute")
    return AuthConfig(
        url=url,
        token=token,
    )


@pytest.fixture
async def auth_config(auth_server: AuthConfig) -> AuthConfig:
    return auth_server


@asynccontextmanager
async def create_auth_client(config: AuthConfig) -> AsyncGenerator[AuthClient]:
    async with AuthClient(url=config.url, token=config.token) as client:
        yield client


@pytest.fixture
async def auth_client(auth_server: AuthConfig) -> AsyncGenerator[AuthClient]:
    async with create_auth_client(auth_server) as client:
        yield client


async def wait_for_auth_server(
    config: AuthConfig, timeout_s: float = 30, interval_s: float = 1
) -> None:
    async with asyncio.timeout(timeout_s):
        while True:
            try:
                async with create_auth_client(config) as auth_client:
                    await auth_client.ping()
                    break
            except (AssertionError, ClientError):
                pass
            await asyncio.sleep(interval_s)


@dataclass(frozen=True)
class _User:
    name: str
    token: str

    @property
    def headers(self) -> dict[str, str]:
        return {AUTHORIZATION: f"Bearer {self.token}"}


@pytest.fixture
def test_cluster_name() -> str:
    return "test-cluster"


@pytest.fixture
async def regular_user_factory(
    auth_client: AuthClient,
    token_factory: Callable[[str], str],
    admin_token: str,
    cluster_name: str,
) -> Callable[
    [str | None, bool, str | None, bool, str | None], Coroutine[Any, Any, _User]
]:
    async def _factory(
        name: str | None = None,
        skip_grant: bool = False,
        org_name: str | None = None,
        org_level: bool = False,
        project_name: str | None = None,
    ) -> _User:
        if not name:
            name = f"user-{random_name()}"
        user = AuthClientUser(name=name)
        await auth_client.add_user(user, token=admin_token)
        if not skip_grant:
            org_path = f"/{org_name}" if org_name else ""
            project_path = f"/{project_name}" if project_name else ""
            name_path = "" if org_level else f"/{name}"
            permissions = [
                Permission(uri=f"disk://{cluster_name}/{name}", action="write")
            ]
            if org_path:
                permissions.append(
                    Permission(
                        uri=f"disk://{cluster_name}{org_path}{name_path}",
                        action="write",
                    )
                )
            if project_path:
                permissions.append(
                    Permission(
                        uri=f"disk://{cluster_name}{org_path}{project_path}",
                        action="write",
                    )
                )
            await auth_client.grant_user_permissions(
                name, permissions, token=admin_token
            )

        return _User(name=user.name, token=token_factory(user.name))

    return _factory
