import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass

import pytest
from aiohttp.hdrs import AUTHORIZATION
from jose import jwt
from neuro_admin_client import AdminClient
from neuro_admin_client.auth_client import AuthClient, User
from neuro_admin_client.entities import (
    ClusterUserRoleType,
    OrgUserRoleType,
    ProjectUserRoleType,
)
from yarl import URL

from platform_disk_api.config import AuthConfig
from tests.integration.conftest import get_service_url, random_name


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def token_factory() -> Callable[[str], str]:
    def _factory(name: str) -> str:
        payload = {"identity": name}
        return jwt.encode(payload, "secret", algorithm="HS256")

    return _factory


@pytest.fixture(scope="session")
def admin_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("admin")


@pytest.fixture(scope="session")
def admin_url() -> URL:
    """URL to platform-admin service for user/entity management."""
    platform_admin = get_service_url("platformadmin", namespace="default")
    # platform-admin uses /apis/admin/v1/ prefix for its API
    return URL(platform_admin) / "apis/admin/v1"


@pytest.fixture(scope="session")
def auth_config(admin_url: URL, admin_token: str) -> AuthConfig:
    """
    Platform-disk-api uses this for token verification and permission checks.
    Points to platform-admin which handles both via neuro_admin_client.AuthClient.
    """
    return AuthConfig(url=admin_url, token=admin_token)


@pytest.fixture
async def auth_client(admin_url: URL, admin_token: str) -> AsyncIterator[AuthClient]:
    """AuthClient for creating users on platform-admin (per-test)."""
    async with AuthClient(admin_url, admin_token) as client:
        yield client


@pytest.fixture(scope="session")
async def admin_client(admin_url: URL, admin_token: str) -> AsyncIterator[AdminClient]:
    """AdminClient for entity management on platform-admin."""
    async with AdminClient(base_url=admin_url, service_token=admin_token) as client:
        yield client


@pytest.fixture(scope="session")
async def _setup_admin_user(admin_url: URL, admin_token: str) -> None:
    """Ensure admin user exists on platform-admin for token verification."""
    # Create auth user
    async with AuthClient(admin_url, admin_token) as client:
        try:
            user = User(name="admin", email="admin@apolo.us")
            await client.add_user(user)
        except Exception:
            pass  # Already exists

    # Create user entity
    async with AdminClient(base_url=admin_url, service_token=admin_token) as client:
        try:
            await client.create_user(name="admin", email="admin@apolo.us")
        except Exception:
            pass  # Already exists


@pytest.fixture(scope="session")
async def _setup_cluster(
    admin_client: AdminClient, cluster_name: str, _setup_admin_user: None
) -> None:
    """Ensure cluster exists and admin has cluster admin role."""
    try:
        await admin_client.create_cluster(name=cluster_name)
    except Exception:
        pass  # Already exists

    # Grant admin user cluster admin role (needed for permission checks)
    try:
        await admin_client.create_cluster_user(
            cluster_name=cluster_name,
            user_name="admin",
            role=ClusterUserRoleType.ADMIN,
        )
    except Exception:
        pass  # Already exists


@dataclass(frozen=True)
class _User:
    name: str
    token: str

    @property
    def headers(self) -> dict[str, str]:
        return {AUTHORIZATION: f"Bearer {self.token}"}


@pytest.fixture
async def regular_user_factory(  # noqa: C901
    auth_client: AuthClient,
    admin_client: AdminClient,
    admin_url: URL,
    token_factory: Callable[[str], str],
    cluster_name: str,
    _setup_cluster: None,
) -> Callable[..., Awaitable[_User]]:
    """
    Factory that creates users with proper permissions.

    Creates users on platform-admin (for token verification) and sets up
    entity hierarchy. Permissions are derived from entity membership.
    """

    async def _factory(  # noqa: C901
        name: str | None = None,
        skip_grant: bool = False,
        org_name: str | None = None,
        project_name: str | None = None,
    ) -> _User:
        if not name:
            name = f"user-{random_name()}"

        try:
            async with AuthClient(
                admin_url, token_factory("admin")
            ) as temp_auth_client:
                user = User(name=name, email=f"{name}@apolo.us")
                await temp_auth_client.add_user(user)
                logger.info("Created auth user %s", (name,))
        except Exception as e:
            logger.warning("Failed to create auth user %s: %s", (name, e))

        try:
            async with AdminClient(
                base_url=admin_url, service_token=token_factory("admin")
            ) as temp_admin_client:
                await temp_admin_client.create_user(name=name, email=f"{name}@apolo.us")
                logger.info("Created user entity %s", (name,))
        except Exception as e:
            logger.warning("Failed to create user entity %s: %s", (name, e))

        # Always add user to cluster (needed for permission checks by cluster admin)
        try:
            async with AdminClient(
                base_url=admin_url, service_token=token_factory("admin")
            ) as temp_client:
                await temp_client.create_cluster_user(
                    cluster_name=cluster_name,
                    user_name=name,
                    role=ClusterUserRoleType.MEMBER,
                )
        except Exception as e:
            logger.warning(
                "Failed to add user %s to cluster %s: %s", (user, cluster_name, e)
            )

        if not skip_grant and org_name and project_name:
            # Set up entity hierarchy on platform-admin
            # Permissions are derived from entity membership
            async with AdminClient(
                base_url=admin_url, service_token=token_factory("admin")
            ) as temp_admin_client:
                try:
                    await temp_admin_client.create_org(name=org_name)
                    logger.info("Created org %s", (org_name,))
                except Exception as e:
                    logger.warning("Failed to create org %s: %s", (org_name, e))

                try:
                    await temp_admin_client.create_org_cluster(
                        cluster_name=cluster_name, org_name=org_name
                    )
                    logger.info(
                        "Created org_cluster %s in %s", (org_name, cluster_name)
                    )
                except Exception as e:
                    logger.warning("Failed to create org_cluster: %s", (e,))

                try:
                    await temp_admin_client.create_org_user(
                        org_name=org_name,
                        user_name=name,
                        role=OrgUserRoleType.USER,
                    )
                    logger.info("Added user %s to org %s", (user, org_name))
                except Exception as e:
                    logger.warning("Failed to add user to org: %s", (e,))

                try:
                    await temp_admin_client.create_project(
                        name=project_name,
                        cluster_name=cluster_name,
                        org_name=org_name,
                    )
                    logger.info("Created project %s", (project_name,))
                except Exception as e:
                    logger.warning("Failed to create project: %s", (e,))

                try:
                    await temp_admin_client.create_project_user(
                        project_name=project_name,
                        cluster_name=cluster_name,
                        org_name=org_name,
                        user_name=name,
                        role=ProjectUserRoleType.WRITER,
                    )
                    logger.info("Added user %s to project %s", (name, project_name))
                except Exception as e:
                    logger.warning("Failed to add user to project: %s", (e,))

        return _User(name=name, token=token_factory(name))

    return _factory
