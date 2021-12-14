import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
)

import aiodocker
import pytest
from aiohttp import ClientError
from aiohttp.hdrs import AUTHORIZATION
from async_timeout import timeout
from jose import jwt
from neuro_auth_client import (
    AuthClient,
    Cluster as AuthCluster,
    Permission,
    User as AuthClientUser,
)
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
    container_config = {
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
    container: aiodocker.containers.DockerContainer,
) -> AuthConfig:
    host = "0.0.0.0"
    port = int((await container.port(8080))[0]["HostPort"])
    url = URL(f"http://{host}:{port}")
    token = create_token("compute")
    return AuthConfig(
        url=url,
        token=token,
    )


@pytest.fixture
async def auth_config(auth_server: AuthConfig) -> AsyncIterator[AuthConfig]:
    yield auth_server


@asynccontextmanager
async def create_auth_client(config: AuthConfig) -> AsyncGenerator[AuthClient, None]:
    async with AuthClient(url=config.url, token=config.token) as client:
        yield client


@pytest.fixture
async def auth_client(auth_server: AuthConfig) -> AsyncGenerator[AuthClient, None]:
    async with create_auth_client(auth_server) as client:
        yield client


async def wait_for_auth_server(
    config: AuthConfig, timeout_s: float = 30, interval_s: float = 1
) -> None:
    async with timeout(timeout_s):
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
    clusters: List[AuthCluster] = field(default_factory=list)

    @property
    def cluster_name(self) -> str:
        assert self.clusters, "Test user has no access to any cluster"
        return self.clusters[0].name

    @property
    def headers(self) -> Dict[str, str]:
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
) -> AsyncIterator[Callable[[Optional[str]], Awaitable[_User]]]:
    async def _factory(
        name: Optional[str] = None,
        skip_grant: bool = False,
        org_name: Optional[str] = None,
        org_level: bool = False,
    ) -> _User:
        if not name:
            name = f"user-{random_name()}"
        user = AuthClientUser(name=name, clusters=[AuthCluster(name=cluster_name)])
        await auth_client.add_user(user, token=admin_token)
        if not skip_grant:
            # Grant permissions to the user home directory
            if org_name is None:
                permission = Permission(
                    uri=f"disk://{cluster_name}/{name}", action="write"
                )
            elif org_level:
                permission = Permission(
                    uri=f"disk://{cluster_name}/{org_name}", action="write"
                )
            else:
                permission = Permission(
                    uri=f"disk://{cluster_name}/{org_name}/{name}", action="write"
                )
            await auth_client.grant_user_permissions(
                name, [permission], token=admin_token
            )

        return _User(name=user.name, token=token_factory(user.name))

    yield _factory
