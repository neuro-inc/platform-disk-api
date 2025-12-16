import asyncio
import json
import subprocess
import uuid
from asyncio import timeout
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from apolo_kube_client import (
    KubeClient,
    KubeClientAuthType,
    KubeClientProxy,
    KubeClientSelector,
    KubeConfig,
    V1Container,
    V1ObjectMeta,
    V1PersistentVolumeClaimVolumeSource,
    V1Pod,
    V1PodSpec,
    V1Volume,
)


@pytest.fixture(scope="session")
def kube_config_payload() -> dict[str, Any]:
    result = subprocess.run(
        ["kubectl", "config", "view", "-o", "json"], stdout=subprocess.PIPE
    )
    payload_str = result.stdout.decode().rstrip()
    return json.loads(payload_str)


@pytest.fixture(scope="session")
def kube_config_cluster_payload(kube_config_payload: dict[str, Any]) -> Any:
    cluster_name = "minikube"
    clusters = {
        cluster["name"]: cluster["cluster"]
        for cluster in kube_config_payload["clusters"]
    }
    return clusters[cluster_name]


@pytest.fixture(scope="session")
def kube_config_user_payload(kube_config_payload: dict[str, Any]) -> Any:
    user_name = "minikube"
    users = {user["name"]: user["user"] for user in kube_config_payload["users"]}
    return users[user_name]


@pytest.fixture(scope="session")
def cert_authority_data_pem(
    kube_config_cluster_payload: dict[str, Any],
) -> str | None:
    ca_path = kube_config_cluster_payload["certificate-authority"]
    if ca_path:
        return Path(ca_path).read_text()
    return None


@pytest.fixture(scope="session")
async def kube_config(
    kube_config_cluster_payload: dict[str, Any],
    kube_config_user_payload: dict[str, Any],
    cert_authority_data_pem: str | None,
) -> KubeConfig:
    cluster = kube_config_cluster_payload
    user = kube_config_user_payload
    return KubeConfig(
        endpoint_url=cluster["server"],
        cert_authority_data_pem=cert_authority_data_pem,
        auth_cert_path=user["client-certificate"],
        auth_cert_key_path=user["client-key"],
        auth_type=KubeClientAuthType.CERTIFICATE,
    )


@asynccontextmanager
async def run_pod(
    kube_client: KubeClientProxy, pvc_names: list[str]
) -> AsyncIterator[V1Pod]:
    pod = V1Pod(
        metadata=V1ObjectMeta(
            name=str(uuid.uuid4()),
        ),
        spec=V1PodSpec(
            automount_service_account_token=False,
            containers=[
                V1Container(
                    name="hello",
                    image="busybox",
                    command=["sh", "-c", "sleep 1"],
                )
            ],
            volumes=[
                V1Volume(
                    name=f"disk-{i}",
                    persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                        claim_name=name
                    ),
                )
                for i, name in enumerate(pvc_names)
            ],
        ),
    )
    pod = await kube_client.core_v1.pod.create(model=pod)
    yield pod
    assert pod.metadata.name is not None
    await kube_client.core_v1.pod.delete(name=pod.metadata.name)


@pytest.fixture
async def kube_selector(kube_config: KubeConfig) -> AsyncIterator[KubeClientSelector]:
    async with KubeClientSelector(config=kube_config) as kube_client_selector:
        yield kube_client_selector


@pytest.fixture
async def kube_client(kube_selector: KubeClientSelector) -> KubeClient:
    return kube_selector.host_client


@pytest.fixture
async def scoped_kube_client(
    kube_selector: KubeClientSelector, org_project: tuple[str, str]
) -> AsyncIterator[KubeClientProxy]:
    org_name, project_name = org_project
    async with kube_selector.get_client(
        org_name=org_name, project_name=project_name
    ) as cli:
        yield cli


async def wait_no_namings(
    kube_client: KubeClient,
    timeout_s: int = 30,
    poll_interval_s: int = 1,
) -> None:
    async with timeout(timeout_s):
        while True:
            disk_namings = await kube_client.neuromation_io_v1.disk_naming.get_list(
                all_namespaces=True
            )
            if not disk_namings.items:
                return
            await asyncio.sleep(poll_interval_s)
