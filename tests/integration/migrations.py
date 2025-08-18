from __future__ import annotations
import asyncio
import aiodocker
from aiodocker.utils import JSONObject
import pytest

MIGRATIONS_CONTAINER = "platformadmin-migrations"
MIGRATION_CMD = ["alembic", "upgrade", "head"]


async def _run_migrations_once(
    docker: aiodocker.Docker,
    image: str,
    docker_network: str,
    dsn: str,
    timeout_s: int = 120,
) -> None:
    try:
        old = await docker.containers.get(MIGRATIONS_CONTAINER)
        try:
            await old.kill()
        except Exception:
            pass
        try:
            await old.delete(force=True)
        except Exception:
            pass
    except aiodocker.exceptions.DockerError:
        pass

    cfg: JSONObject = {
        "Image": image,
        "name": MIGRATIONS_CONTAINER,
        "Cmd": MIGRATION_CMD,
        "Env": [f"NP_ADMIN_POSTGRES_DSN={dsn}"],
        "HostConfig": {
            "NetworkMode": docker_network,
            "AutoRemove": False,
        },
        "NetworkingConfig": {"EndpointsConfig": {docker_network: {}}},
    }

    try:
        await docker.images.inspect(image)
    except aiodocker.exceptions.DockerError:
        await docker.images.pull(image)

    job = await docker.containers.create_or_replace(
        name=MIGRATIONS_CONTAINER, config=cfg
    )
    await job.start()

    # wait for completion with timeout; capture logs if non-zero exit
    # try:
    # aiodocker wait(): { "StatusCode": int }
    result = await asyncio.wait_for(job.wait(), timeout=timeout_s)
    status = int(result.get("StatusCode", 1))
    if status != 0:
        logs = ""
        try:
            logs = "".join(await job.log(stdout=True, stderr=True))
        except Exception:
            pass
        raise RuntimeError(f"alembic upgrade failed (exit {status}).\n{logs}")
    # finally:
    #     # AutoRemove should clean it, but try anyway
    #     try:
    #         await job.delete(force=True)
    #     except Exception:
    #         pass


@pytest.fixture(scope="session")
async def run_platformadmin_migrations(
    docker: aiodocker.Docker,
    docker_network: str,
    postgres: dict,
    auth_server_image_name: str,
) -> None:
    dsn = postgres["dsn_sync"]
    await _run_migrations_once(
        docker=docker,
        image=auth_server_image_name,
        docker_network=docker_network,
        dsn=dsn,
    )
