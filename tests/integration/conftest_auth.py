from dataclasses import dataclass
from typing import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterator,
    Optional,
)

import pytest
from aiohttp.hdrs import AUTHORIZATION
from jose import jwt
from neuro_auth_client import AuthClient, Cluster, User as AuthClientUser
from yarl import URL

from platform_disk_api.api import create_auth_client
from platform_disk_api.config import PlatformAuthConfig
from tests.integration.conftest import get_service_url, random_name


@pytest.fixture(scope="session")
def token_factory() -> Iterator[Callable[[str], str]]:
    def _factory(name: str) -> str:
        payload = {"identity": name}
        return jwt.encode(payload, "secret", algorithm="HS256")

    yield _factory


@pytest.fixture(scope="session")
def admin_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("admin")


@pytest.fixture(scope="session")
def compute_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("compute")


@pytest.fixture(scope="session")
def auth_config(token_factory: Callable[[str], str]) -> Iterator[PlatformAuthConfig]:
    platform_auth = get_service_url("platformauthapi", namespace="default")
    yield PlatformAuthConfig(
        url=URL(platform_auth),
        token=token_factory("compute"),  # token is hard-coded in the yaml configuration
    )


@pytest.fixture
async def auth_client(
    auth_config: PlatformAuthConfig,
) -> AsyncGenerator[AuthClient, None]:
    async with create_auth_client(auth_config) as client:
        await client.ping()
        yield client


@dataclass(frozen=True)
class _User(AuthClientUser):
    token: str = ""

    @property
    def headers(self) -> Dict[str, str]:
        return {AUTHORIZATION: f"Bearer {self.token}"}


@pytest.fixture
async def regular_user_factory(
    auth_client: AuthClient,
    token_factory: Callable[[str], str],
    admin_token: str,
    cluster_name: str,
) -> AsyncIterator[Callable[[Optional[str]], Awaitable[_User]]]:
    async def _factory(name: Optional[str] = None, skip_grant: bool = False) -> _User:
        if not name:
            name = f"user-{random_name()}"
        user = AuthClientUser(name=name, clusters=[Cluster(name=cluster_name)])
        await auth_client.add_user(user, token=admin_token)
        if not skip_grant:
            # Grant permissions to the user home directory
            headers = auth_client._generate_headers(admin_token)
            payload = [
                {"uri": f"disk://{cluster_name}/{name}", "action": "write"},
            ]
            async with auth_client._request(
                "POST",
                f"/api/v1/users/{name}/permissions",
                headers=headers,
                json=payload,
            ) as p:
                assert p.status == 201
        return _User(name=user.name, token=token_factory(user.name))  # type: ignore

    yield _factory
