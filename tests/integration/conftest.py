import asyncio
import logging
import secrets
import subprocess
import time
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import aiohttp
import aiohttp.web
import pytest
from apolo_events_client import EventsClientConfig
from apolo_kube_client import (
    KubeClient,
    KubeClientSelector,
    KubeConfig,
    ResourceNotFound,
    V1Namespace,
)
from apolo_kube_client.apolo import generate_namespace_name

from platform_disk_api.config import (
    AuthConfig,
    Config,
    CORSConfig,
    DiskConfig,
    ServerConfig,
)
from platform_disk_api.service import DISK_API_MARK_LABEL, Service


logger = logging.getLogger(__name__)


pytest_plugins = [
    "tests.integration.conftest_clusters",
    "tests.integration.docker",
    "tests.integration.auth",
    "tests.integration.kube",
]


def random_name(length: int = 8) -> str:
    return secrets.token_hex(length // 2 + length % 2)[:length]


@pytest.fixture(scope="session")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    loop = asyncio.get_event_loop_policy().new_event_loop()
    loop.set_debug(True)

    watcher = asyncio.SafeChildWatcher()
    watcher.attach_loop(loop)
    asyncio.get_event_loop_policy().set_child_watcher(watcher)

    yield loop
    loop.close()


@pytest.fixture
async def k8s_storage_class() -> str:
    return "test-storage-class"  # Same as in storageclass.yml


@pytest.fixture
def service(
    kube_selector: KubeClientSelector,
    k8s_storage_class: str,
) -> Service:
    return Service(
        kube_client_selector=kube_selector,
        storage_class_name=k8s_storage_class,
    )


@pytest.fixture(autouse=True)
async def cleanup_disks(kube_client: KubeClient) -> AsyncIterator[None]:
    """
    Remove all PVCs and disk naming CRDs created during a single test.
    """

    async def _clean_k8s(kube_client: KubeClient) -> None:
        # Limit cleanup to PVCs managed by the disk API to avoid
        # interfering with other PVCs (including vcluster internals).
        while True:
            pvc_list = await kube_client.core_v1.persistent_volume_claim.get_list(
                all_namespaces=True,
                # make sure we do not delete a vcluster service PVC
                label_selector=f"{DISK_API_MARK_LABEL}=true,release!=vcluster",
            )
            for pvc in pvc_list.items:
                if (
                    not pvc.metadata
                    or not pvc.metadata.name
                    or not pvc.metadata.namespace
                ):
                    continue
                try:
                    await kube_client.core_v1.persistent_volume_claim.delete(
                        name=pvc.metadata.name,
                        namespace=pvc.metadata.namespace,
                    )
                except ResourceNotFound:
                    pass

            disk_naming_list = await kube_client.neuromation_io_v1.disk_naming.get_list(
                all_namespaces=True
            )
            for disk_naming in disk_naming_list.items:
                if (
                    not disk_naming.metadata
                    or not disk_naming.metadata.name
                    or not disk_naming.metadata.namespace
                ):
                    continue
                try:
                    await kube_client.neuromation_io_v1.disk_naming.delete(
                        name=disk_naming.metadata.name,
                        namespace=disk_naming.metadata.namespace,
                    )
                except ResourceNotFound:
                    pass

            if not pvc_list.items and not disk_naming_list.items:
                break

            await asyncio.sleep(0.1)

    yield
    await _clean_k8s(kube_client)


@pytest.fixture
async def client() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture
def config_factory(
    auth_config: AuthConfig,
    kube_config: KubeConfig,
    cluster_name: str,
    k8s_storage_class: str,
    events_config: EventsClientConfig,
) -> Callable[..., Config]:
    def _f(**kwargs: Any) -> Config:
        defaults = {
            "server": ServerConfig(host="0.0.0.0", port=8080),
            "platform_auth": auth_config,
            "kube": kube_config,
            "cluster_name": cluster_name,
            "disk": DiskConfig(
                k8s_storage_class=k8s_storage_class,
                storage_limit_per_project=1024 * 1024 * 20,  # 20mb
            ),
            "cors": CORSConfig(allowed_origins=["https://neu.ro"]),
            "events": events_config,
        }
        kwargs = {**defaults, **kwargs}
        return Config(**kwargs)

    return _f


@pytest.fixture
def config(
    config_factory: Callable[..., Config],
) -> Config:
    return config_factory()


@dataclass(frozen=True)
class ApiAddress:
    host: str
    port: int


@asynccontextmanager
async def create_local_app_server(
    app: aiohttp.web.Application, port: int = 8080
) -> AsyncIterator[ApiAddress]:
    runner = aiohttp.web.AppRunner(app)
    try:
        await runner.setup()
        api_address = ApiAddress("0.0.0.0", port)
        site = aiohttp.web.TCPSite(runner, api_address.host, api_address.port)
        await site.start()
        yield api_address
    finally:
        await runner.shutdown()
        await runner.cleanup()


def get_service_url(service_name: str, namespace: str = "default") -> str:
    # ignore type because the linter does not know that `pytest.fail` throws an
    # exception, so it requires to `return None` explicitly, so that the method
    # will return `Optional[List[str]]` which is incorrect
    timeout_s = 60
    interval_s = 10

    while timeout_s:
        process = subprocess.run(
            ("minikube", "service", "-n", namespace, service_name, "--url"),
            stdout=subprocess.PIPE,
        )
        output = process.stdout
        if output:
            url = output.decode().strip()
            # Sometimes `minikube service ... --url` returns a prefixed
            # string such as: "* https://127.0.0.1:8081/"
            start_idx = url.find("http")
            if start_idx > 0:
                url = url[start_idx:]
            return url
        time.sleep(interval_s)
        timeout_s -= interval_s

    pytest.fail(f"Service {service_name} is unavailable.")


@pytest.fixture
def cluster_name() -> str:
    return "test-cluster"


@pytest.fixture
async def scoped_namespace(
    kube_client: KubeClient,
    org_project: tuple[str, str],
) -> tuple[V1Namespace, str, str]:
    org, project = org_project
    namespace = await kube_client.core_v1.namespace.get(
        generate_namespace_name(org, project)
    )
    return namespace, org, project
