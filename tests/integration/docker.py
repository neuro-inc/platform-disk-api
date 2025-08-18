from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import aiodocker
import pytest
from aiodocker.utils import JSONObject

PYTEST_REUSE_DOCKER_OPT = "--reuse-docker"

DOCKER_NETWORK = "it_net"
PG_CONTAINER = "it_pg"
POSTGRES_IMAGE = "postgres:14"

PG_USER = "admin"
PG_PASSWORD = "adminpass"
PG_DB = "platformadmin"


def pytest_addoption(parser: Any) -> None:
    parser.addoption(
        PYTEST_REUSE_DOCKER_OPT,
        action="store_true",
        help="Reuse existing docker containers",
    )


@pytest.fixture(scope="session")
def reuse_docker(request: Any) -> bool:
    return request.config.getoption(PYTEST_REUSE_DOCKER_OPT)


@pytest.fixture(scope="session")
async def docker() -> AsyncIterator[aiodocker.Docker]:
    client = aiodocker.Docker(api_version="v1.34")
    try:
        yield client
    finally:
        await client.close()


async def _ensure_network(docker: aiodocker.Docker, name: str) -> None:
    try:
        net = await docker.networks.get(name)
        await net.show()
        return
    except aiodocker.exceptions.DockerError:
        pass

    await docker.networks.create({"Name": name})


@pytest.fixture(scope="session")
async def docker_network(docker: aiodocker.Docker, reuse_docker: bool) -> str:
    await _ensure_network(docker, DOCKER_NETWORK)
    return DOCKER_NETWORK


@pytest.fixture(scope="session")
async def postgres(
    docker: aiodocker.Docker,
    docker_network: str,
    reuse_docker: bool,
) -> AsyncIterator[dict[str, str]]:
    if reuse_docker:
        try:
            c = await docker.containers.get(PG_CONTAINER)
            if c["State"]["Running"]:
                yield {
                    "dsn_sync": f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_CONTAINER}:5432/{PG_DB}",
                    "host_for_admin": PG_CONTAINER,
                    "user": PG_USER,
                    "password": PG_PASSWORD,
                    "db": PG_DB,
                }
                return
        except aiodocker.exceptions.DockerError:
            pass
    try:
        await docker.images.inspect(POSTGRES_IMAGE)
    except aiodocker.exceptions.DockerError:
        await docker.images.pull(POSTGRES_IMAGE)

    cfg: JSONObject = {
        "Image": POSTGRES_IMAGE,
        "name": PG_CONTAINER,
        "Env": [
            f"POSTGRES_USER={PG_USER}",
            f"POSTGRES_PASSWORD={PG_PASSWORD}",
            f"POSTGRES_DB={PG_DB}",
        ],
        "HostConfig": {
            "NetworkMode": docker_network,
        },
        "NetworkingConfig": {"EndpointsConfig": {docker_network: {}}},
    }

    container = await docker.containers.create_or_replace(name=PG_CONTAINER, config=cfg)
    await container.start()

    async def _pg_ready() -> bool:
        try:
            logs = await container.log(stdout=True, stderr=True)
            return any(
                "database system is ready to accept connections" in line
                for line in logs
            )
        except Exception:
            return False

    for _ in range(90):
        if await _pg_ready():
            break
        await asyncio.sleep(1)
    else:
        logs = await container.log(stdout=True, stderr=True)
        raise RuntimeError(
            "Postgres did not become ready in time.\n" + "".join(logs[-200:])
        )

    try:
        yield {
            "dsn_sync": f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_CONTAINER}:5432/{PG_DB}",
            "host_for_admin": PG_CONTAINER,
            "user": PG_USER,
            "password": PG_PASSWORD,
            "db": PG_DB,
        }
    finally:
        if not reuse_docker:
            await container.kill()
            await container.delete(force=True)
