import json
import subprocess
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

import pytest

from platform_disk_api.config import KubeClientAuthType, KubeConfig
from platform_disk_api.kube_client import KubeClient, PodRead, ResourceNotFound


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
    kube_config_cluster_payload: dict[str, Any]
) -> Optional[str]:
    ca_path = kube_config_cluster_payload["certificate-authority"]
    if ca_path:
        return Path(ca_path).read_text()
    return None


@pytest.fixture
async def kube_config(
    kube_config_cluster_payload: dict[str, Any],
    kube_config_user_payload: dict[str, Any],
    cert_authority_data_pem: Optional[str],
) -> KubeConfig:
    cluster = kube_config_cluster_payload
    user = kube_config_user_payload
    kube_config = KubeConfig(
        endpoint_url=cluster["server"],
        cert_authority_data_pem=cert_authority_data_pem,
        auth_cert_path=user["client-certificate"],
        auth_cert_key_path=user["client-key"],
        auth_type=KubeClientAuthType.CERTIFICATE,
        namespace="default",
    )
    return kube_config


class KubeClientForTest(KubeClient):
    @asynccontextmanager
    async def run_pod(self, pvc_names: list[str]) -> AsyncIterator[PodRead]:
        json = {
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {"name": str(uuid.uuid4())},
            "spec": {
                "automountServiceAccountToken": False,
                "containers": [
                    {
                        "name": "hello",
                        "image": "busybox",
                        "command": ["sh", "-c", "sleep 1"],
                    }
                ],
                "volumes": [
                    {"name": f"disk-{i}", "persistentVolumeClaim": {"claimName": name}}
                    for (i, name) in enumerate(pvc_names)
                ],
            },
        }
        url = self._pod_url
        payload = await self._request(method="POST", url=url, json=json)
        self._raise_for_status(payload)
        yield PodRead.from_primitive(payload)
        await self._request(method="DELETE", url=f"{url}/{payload['metadata']['name']}")


@pytest.fixture
def kube_client_factory() -> Callable[[KubeConfig], KubeClientForTest]:
    def make_kube_client(kube_config: KubeConfig) -> KubeClientForTest:
        return KubeClientForTest(
            base_url=kube_config.endpoint_url,
            auth_type=kube_config.auth_type,
            cert_authority_data_pem=kube_config.cert_authority_data_pem,
            cert_authority_path=None,  # disabled, see `cert_authority_data_pem`
            auth_cert_path=kube_config.auth_cert_path,
            auth_cert_key_path=kube_config.auth_cert_key_path,
            namespace=kube_config.namespace,
            conn_timeout_s=kube_config.client_conn_timeout_s,
            read_timeout_s=kube_config.client_read_timeout_s,
            watch_timeout_s=kube_config.client_watch_timeout_s,
            conn_pool_size=kube_config.client_conn_pool_size,
        )

    return make_kube_client


@pytest.fixture
async def kube_client(
    kube_config: KubeConfig,
    kube_client_factory: Callable[[KubeConfig], KubeClientForTest],
) -> AsyncIterator[KubeClientForTest]:
    client = kube_client_factory(kube_config)

    async def _clean_k8s(kube_client: KubeClient) -> None:
        for pvc in await kube_client.list_pvc():
            try:
                await kube_client.remove_pvc(pvc.name)
            except ResourceNotFound:
                pass
        for disk_naming in await kube_client.list_disk_namings():
            await kube_client.remove_disk_naming(disk_naming.name)

    async with client:
        await _clean_k8s(client)
        yield client
        await _clean_k8s(client)
