from __future__ import annotations

import asyncio
import json
from asyncio.timeouts import timeout
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import pytest
from apolo_kube_client.errors import KubeClientException, ResourceInvalid
from apolo_kube_client.namespace import Namespace

from platform_disk_api.admission_controller.api import (
    ANNOTATION_APOLO_INJECT_DISK,
    INJECTED_VOLUME_NAME_PREFIX,
    LABEL_APOLO_ORG_NAME,
    LABEL_APOLO_PROJECT_NAME,
)
from platform_disk_api.kube_client import KubeClient
from platform_disk_api.service import (
    APOLO_DISK_API_CREATED_AT_ANNOTATION,
    APOLO_DISK_API_MARK_LABEL,
    APOLO_DISK_API_NAME_ANNOTATION,
    APOLO_ORG_LABEL,
    APOLO_PROJECT_LABEL,
    APOLO_USER_LABEL,
    DISK_API_CREATED_AT_ANNOTATION,
    DISK_API_MARK_LABEL,
    DISK_API_NAME_ANNOTATION,
    DISK_API_ORG_LABEL,
    DISK_API_PROJECT_LABEL,
    USER_LABEL,
    Disk,
    DiskRequest,
    Service,
)


@asynccontextmanager
async def pod_cm(
    kube_client: KubeClient,
    namespace: str,
    annotations: dict[str, Any] | None = None,
    labels: dict[str, Any] | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """
    A context manager for creating the pod, returning the response,
    and deleting the POD at the end
    """
    pod_name = str(uuid4())
    payload = {
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": {
            "name": pod_name,
        },
        "spec": {
            "containers": [
                {
                    "name": "hello",
                    "image": "busybox",
                    "command": ["sh", "-c", "sleep 5"],
                }
            ]
        },
    }
    if annotations is not None:
        payload["metadata"]["annotations"] = annotations  # type: ignore[index]

    if labels is not None:
        payload["metadata"]["labels"] = labels  # type: ignore[index]

    url = f"{kube_client.generate_namespace_url(namespace)}/pods"
    response = await kube_client.post(
        url=url,
        json=payload,
    )

    # wait until a POD is running
    async with asyncio.timeout(60):
        while (await kube_client.get(f"{url}/{pod_name}"))["status"][  # noqa ASYNC110
            "phase"
        ] != "Running":
            await asyncio.sleep(0.1)

    yield response

    await kube_client.delete(f"{url}/{pod_name}")


class TestAdmissionController:
    # @pytest.fixture
    # def service(
    #     self,
    #     kube_client: KubeClient,
    #     k8s_storage_class: str,
    # ) -> Service:
    #     return Service(
    #         kube_client=kube_client,
    #         storage_class_name=k8s_storage_class,
    #     )

    @pytest.fixture
    def statefulset_manifest_factory(
        self,
        k8s_storage_class: str,
    ) -> Callable[..., dict[str, Any]]:
        def _factory(
            labels: dict[str, str],
            annotations: dict[str, str],
            storage_class_name: str = k8s_storage_class,
        ) -> dict[str, Any]:
            return {
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {"name": "test-statefulset"},
                "spec": {
                    "serviceName": "ubuntu-service",
                    "replicas": 2,
                    "selector": {"matchLabels": {"app": "ubuntu"}},
                    "template": {
                        "metadata": {"labels": {"app": "ubuntu"}},
                        "spec": {
                            "containers": [
                                {
                                    "name": "ubuntu",
                                    "image": "ubuntu:latest",
                                    "command": ["sleep", "infinity"],
                                    "volumeMounts": [
                                        {
                                            "name": "ubuntu-data",
                                            "mountPath": "/mnt/data",
                                        }
                                    ],
                                }
                            ],
                            "terminationGracePeriodSeconds": 1,
                        },
                    },
                    "volumeClaimTemplates": [
                        {
                            "metadata": {
                                "name": "ubuntu-data",
                                "annotations": annotations,
                                "labels": labels,
                            },
                            "spec": {
                                "accessModes": ["ReadWriteOnce"],
                                "resources": {"requests": {"storage": "10Mi"}},
                                "storageClassName": storage_class_name,
                            },
                        }
                    ],
                },
            }

        return _factory

    @staticmethod
    async def _wait_statefulset(
        kube_client: KubeClient,
        name: str,
        namespace_name: str,
    ) -> None:
        """
        Wait for a stateful set to spawn all the POD's
        """
        url = kube_client._generate_statefulsets_url(namespace_name)
        url = f"{url}/{name}"
        async with timeout(60):
            while True:
                response = await kube_client.get(url)
                requested_replicas = response["spec"]["replicas"]
                actual_replicas = response["status"].get("currentReplicas", 0) or 0
                if requested_replicas == actual_replicas:
                    break
                await asyncio.sleep(1)

    @pytest.fixture
    async def disk_no_name(
        self,
        service: Service,
        scoped_namespace: tuple[Namespace, str, str],
    ) -> Disk:
        namespace, org, project = scoped_namespace
        request = DiskRequest(
            storage=1024 * 1024,
            project_name=project,
            org_name=org,
        )
        return await service.create_disk(request, "testuser")

    @pytest.fixture
    def disk_name(self) -> str:
        return str(uuid4())

    @pytest.fixture
    async def disk_with_name(
        self,
        service: Service,
        scoped_namespace: tuple[Namespace, str, str],
        disk_name: str,
    ) -> Disk:
        namespace, org, project = scoped_namespace
        request = DiskRequest(
            storage=1024 * 1024,
            project_name=project,
            org_name=org,
            name=disk_name,
        )
        return await service.create_disk(request, "testuser")

    async def test__create_disk__no_name(
        self,
        disk_no_name: Disk,
        kube_client: KubeClient,
    ) -> None:
        """
        Creating a disk without a name shouldn't lead to a DiskNaming object creation
        """
        disk_namings = await kube_client.list_disk_namings()
        assert not disk_namings

    async def test__create_disk__name_provided_disk_naming_created(
        self,
        kube_client: KubeClient,
        service: Service,
        disk_with_name: Disk,
        scoped_namespace: tuple[Namespace, str, str],
    ) -> None:
        """
        Whenever a disk name is provided,
        admission controller will create a DiskNaming kube object
        """
        _, org, project = scoped_namespace
        disk_namings = await kube_client.list_disk_namings()
        assert len(disk_namings) == 1
        disk_naming = disk_namings[0]
        assert disk_naming.name == f"{disk_with_name.name}--{org}--{project}"
        assert disk_naming.namespace == disk_with_name.namespace

    async def test__create_statefulset__no_name(
        self,
        service: Service,
        kube_client: KubeClient,
        k8s_storage_class: str,
        statefulset_manifest_factory: Callable[..., dict[str, Any]],
        scoped_namespace: tuple[Namespace, str, str],
    ) -> None:
        """
        Ensure that PVC is created properly for the statefulset,
        based on a volumeClaimTemplate
        """
        namespace, org, project = scoped_namespace
        manifest = statefulset_manifest_factory(
            labels={},
            annotations={},
            storage_class_name=k8s_storage_class,
        )
        url = kube_client._generate_statefulsets_url(namespace.name)
        response = await kube_client.post(url, json=manifest)

        # let's wait for statefulset pods to be running
        await self._wait_statefulset(
            kube_client=kube_client,
            name=response["metadata"]["name"],
            namespace_name=namespace.name,
        )

        # there should be two PVCs created
        pvcs = await kube_client.list_pvc(namespace.name)
        assert len(pvcs) == 2

        for pvc in pvcs:
            assert APOLO_DISK_API_CREATED_AT_ANNOTATION in pvc.annotations
            assert DISK_API_CREATED_AT_ANNOTATION in pvc.annotations

            # ensure name annotation does not present
            assert APOLO_DISK_API_NAME_ANNOTATION not in pvc.annotations
            assert DISK_API_NAME_ANNOTATION not in pvc.annotations

            assert pvc.labels[DISK_API_MARK_LABEL] == "true"
            assert pvc.labels[APOLO_DISK_API_MARK_LABEL] == "true"
            assert pvc.labels[DISK_API_ORG_LABEL] == org
            assert pvc.labels[APOLO_ORG_LABEL] == org
            assert pvc.labels[DISK_API_PROJECT_LABEL] == project
            assert pvc.labels[APOLO_PROJECT_LABEL] == project
            assert pvc.labels[APOLO_USER_LABEL] == project
            assert pvc.labels[USER_LABEL] == project

        # no disk namings should be created
        disk_namings = await kube_client.list_disk_namings()
        assert not disk_namings

    async def test__create_statefulset__with_name(
        self,
        service: Service,
        kube_client: KubeClient,
        k8s_storage_class: str,
        statefulset_manifest_factory: Callable[..., dict[str, Any]],
        scoped_namespace: tuple[Namespace, str, str],
    ) -> None:
        """
        Ensure that both PVC and DiskNaming are created properly for the statefulset,
        based on a volumeClaimTemplate
        """
        disk_name = "test-disk"
        namespace, org, project = scoped_namespace
        manifest = statefulset_manifest_factory(
            labels={},
            annotations={APOLO_DISK_API_NAME_ANNOTATION: disk_name},
            storage_class_name=k8s_storage_class,
        )
        url = kube_client._generate_statefulsets_url(namespace.name)
        response = await kube_client.post(url, json=manifest)

        # let's wait for statefulset pods to be running
        await self._wait_statefulset(
            kube_client=kube_client,
            name=response["metadata"]["name"],
            namespace_name=namespace.name,
        )

        # there should be two PVCs created
        pvcs = await kube_client.list_pvc(namespace.name)
        assert len(pvcs) == 2

        for idx, pvc in enumerate(
            sorted(
                pvcs,
                key=lambda p: p.annotations[APOLO_DISK_API_NAME_ANNOTATION],
            )
        ):
            assert APOLO_DISK_API_CREATED_AT_ANNOTATION in pvc.annotations
            assert DISK_API_CREATED_AT_ANNOTATION in pvc.annotations

            # ensure name annotation is present now
            assert (
                pvc.annotations[APOLO_DISK_API_NAME_ANNOTATION] == f"{disk_name}-{idx}"
            )

            assert pvc.labels[DISK_API_MARK_LABEL] == "true"
            assert pvc.labels[APOLO_DISK_API_MARK_LABEL] == "true"
            assert pvc.labels[DISK_API_ORG_LABEL] == org
            assert pvc.labels[APOLO_ORG_LABEL] == org
            assert pvc.labels[DISK_API_PROJECT_LABEL] == project
            assert pvc.labels[APOLO_PROJECT_LABEL] == project
            assert pvc.labels[APOLO_USER_LABEL] == project
            assert pvc.labels[USER_LABEL] == project

        # ensure that both disk namings are now created
        disk_namings = await kube_client.list_disk_namings()
        assert len(disk_namings) == 2

        for idx, disk_naming in enumerate(sorted(disk_namings, key=lambda d: d.name)):
            assert disk_naming.name == f"{disk_name}-{idx}--{org}--{project}"

    async def test__create_statefulset__invalid_storage_class(
        self,
        service: Service,
        kube_client: KubeClient,
        k8s_storage_class: str,
        statefulset_manifest_factory: Callable[..., dict[str, Any]],
        scoped_namespace: tuple[Namespace, str, str],
    ) -> None:
        """
        Ensures that the admission controller will use
        a proper storage class available to it
        """
        namespace, org, project = scoped_namespace
        manifest = statefulset_manifest_factory(
            labels={},
            annotations={},
            storage_class_name="invalid-storage-class",
        )
        url = kube_client._generate_statefulsets_url(namespace.name)
        response = await kube_client.post(url, json=manifest)

        # let's wait for statefulset pods to be running
        await self._wait_statefulset(
            kube_client=kube_client,
            name=response["metadata"]["name"],
            namespace_name=namespace.name,
        )

        # ensure storage class was overridden
        pvcs = await kube_client.list_pvc(namespace.name)
        for pvc in pvcs:
            assert pvc.storage_class_name == k8s_storage_class

    async def test__pod_without_annotations_will_be_ignored(
        self,
        service: Service,
        kube_client: KubeClient,
        scoped_namespace: tuple[Namespace, str, str],
    ) -> None:
        namespace, org, project = scoped_namespace
        async with pod_cm(kube_client, namespace.name) as response:
            assert response["kind"] == "Pod"

    async def test__pod_invalid_annotation_will_prohibit_pod_creation(
        self,
        service: Service,
        kube_client: KubeClient,
        scoped_namespace: tuple[Namespace, str, str],
    ) -> None:
        namespace, org, project = scoped_namespace
        with pytest.raises(ResourceInvalid) as e:
            async with pod_cm(
                kube_client,
                namespace.name,
                labels={
                    LABEL_APOLO_ORG_NAME: org,
                    LABEL_APOLO_PROJECT_NAME: project,
                    ANNOTATION_APOLO_INJECT_DISK: "true",
                },
                annotations={ANNOTATION_APOLO_INJECT_DISK: "invalid"},
            ):
                pass

        assert "injection spec is invalid" in str(e.value)

    async def test__pod_with_another_org_name(
        self,
        service: Service,
        kube_client: KubeClient,
        scoped_namespace: tuple[Namespace, str, str],
    ) -> None:
        namespace, org, project = scoped_namespace
        with pytest.raises(KubeClientException) as e:
            async with pod_cm(
                kube_client,
                namespace.name,
                labels={
                    LABEL_APOLO_ORG_NAME: "invalid org",
                    LABEL_APOLO_PROJECT_NAME: project,
                    ANNOTATION_APOLO_INJECT_DISK: "true",
                },
                annotations={
                    ANNOTATION_APOLO_INJECT_DISK: json.dumps(
                        [
                            {
                                "mount_path": "/mnt/disk",
                                "disk_uri": f"disk://default/{org}/{project}/any",
                            }
                        ]
                    )
                },
            ):
                pass

        assert "metadata value mismatch" in str(e.value)

    async def test__pod_with_another_project_name(
        self,
        service: Service,
        kube_client: KubeClient,
        scoped_namespace: tuple[Namespace, str, str],
    ) -> None:
        namespace, org, project = scoped_namespace
        with pytest.raises(KubeClientException) as e:
            async with pod_cm(
                kube_client,
                namespace.name,
                labels={
                    LABEL_APOLO_ORG_NAME: org,
                    LABEL_APOLO_PROJECT_NAME: "invalid project",
                    ANNOTATION_APOLO_INJECT_DISK: "true",
                },
                annotations={
                    ANNOTATION_APOLO_INJECT_DISK: json.dumps(
                        [
                            {
                                "mount_path": "/mnt/disk",
                                "disk_uri": f"disk://default/{org}/{project}/any",
                            }
                        ]
                    )
                },
            ):
                pass

        assert "metadata value mismatch" in str(e.value)

    async def test__pod_with_another_org_name_in_disk_annotation(
        self,
        service: Service,
        kube_client: KubeClient,
        scoped_namespace: tuple[Namespace, str, str],
    ) -> None:
        namespace, org, project = scoped_namespace
        with pytest.raises(KubeClientException) as e:
            async with pod_cm(
                kube_client,
                namespace.name,
                labels={
                    LABEL_APOLO_ORG_NAME: org,
                    LABEL_APOLO_PROJECT_NAME: project,
                    ANNOTATION_APOLO_INJECT_DISK: "true",
                },
                annotations={
                    ANNOTATION_APOLO_INJECT_DISK: json.dumps(
                        [
                            {
                                "mount_path": "/mnt/disk",
                                "disk_uri": f"disk://default/invalid-org/{project}/any",
                            }
                        ]
                    )
                },
            ):
                pass

        assert "metadata value mismatch" in str(e.value)

    async def test__pod_with_another_project_name_in_disk_annotation(
        self,
        service: Service,
        kube_client: KubeClient,
        scoped_namespace: tuple[Namespace, str, str],
    ) -> None:
        namespace, org, project = scoped_namespace
        with pytest.raises(KubeClientException) as e:
            async with pod_cm(
                kube_client,
                namespace.name,
                labels={
                    LABEL_APOLO_ORG_NAME: org,
                    LABEL_APOLO_PROJECT_NAME: project,
                    ANNOTATION_APOLO_INJECT_DISK: "true",
                },
                annotations={
                    ANNOTATION_APOLO_INJECT_DISK: json.dumps(
                        [
                            {
                                "mount_path": "/mnt/disk",
                                "disk_uri": f"disk://default/{org}/invalid-project/any",
                            }
                        ]
                    )
                },
            ):
                pass

        assert "metadata value mismatch" in str(e.value)

    async def test__inject_single_disk(
        self,
        service: Service,
        kube_client: KubeClient,
        scoped_namespace: tuple[Namespace, str, str],
        disk_no_name: Disk,
    ) -> None:
        namespace, org, project = scoped_namespace

        # now let's create a POD with the proper annotation
        async with pod_cm(
            kube_client,
            namespace=namespace.name,
            annotations={
                ANNOTATION_APOLO_INJECT_DISK: json.dumps(
                    [
                        {
                            "mount_path": "/mnt/disk",
                            "disk_uri": f"disk://default/{org}/{project}/{disk_no_name.id}",
                        }
                    ]
                )
            },
            labels={
                LABEL_APOLO_ORG_NAME: org,
                LABEL_APOLO_PROJECT_NAME: project,
                ANNOTATION_APOLO_INJECT_DISK: "true",
            },
        ) as response:
            spec = response["spec"]
            volumes = spec["volumes"]
            container = spec["containers"][0]

            volumes = [v for v in volumes if "persistentVolumeClaim" in v]
            assert len(volumes) == 1
            assert volumes[0]["name"].startswith(INJECTED_VOLUME_NAME_PREFIX)
            assert volumes[0]["persistentVolumeClaim"]["claimName"] == disk_no_name.id

            mounts_by_path = {v["mountPath"]: v for v in container["volumeMounts"]}
            assert mounts_by_path["/mnt/disk"]["name"].startswith(
                INJECTED_VOLUME_NAME_PREFIX
            )

    async def test_inject_multiple_disks(
        self,
        service: Service,
        kube_client: KubeClient,
        scoped_namespace: tuple[Namespace, str, str],
    ) -> None:
        namespace, org, project = scoped_namespace

        # create two disks
        request = DiskRequest(
            storage=1024 * 1024,
            project_name=project,
            org_name=org,
        )
        disk_1 = await service.create_disk(request, "testuser")

        request = DiskRequest(
            storage=1024 * 1024,
            project_name=project,
            org_name=org,
        )
        disk_2 = await service.create_disk(request, "testuser")

        mount_path_1, mount_path_2 = "/mnt/disk1", "/mnt/disk2"
        async with pod_cm(
            kube_client,
            namespace.name,
            annotations={
                ANNOTATION_APOLO_INJECT_DISK: json.dumps(
                    [
                        {
                            "mount_path": mount_path_1,
                            "disk_uri": f"disk://default/{org}/{project}/{disk_1.id}",
                        },
                        {
                            "mount_path": mount_path_2,
                            "disk_uri": f"disk://default/{org}/{project}/{disk_2.id}",
                        },
                    ]
                ),
            },
            labels={
                LABEL_APOLO_ORG_NAME: org,
                LABEL_APOLO_PROJECT_NAME: project,
                ANNOTATION_APOLO_INJECT_DISK: "true",
            },
        ) as response:
            spec = response["spec"]
            volumes = spec["volumes"]
            container = spec["containers"][0]

            volumes = [v for v in volumes if "persistentVolumeClaim" in v]
            assert len(volumes) == 2

            for volume in volumes:
                assert volume["name"].startswith(INJECTED_VOLUME_NAME_PREFIX)
                assert volume["persistentVolumeClaim"]["claimName"] in {
                    disk_1.id,
                    disk_2.id,
                }

            mounts_by_path = {v["mountPath"]: v for v in container["volumeMounts"]}
            assert mounts_by_path[mount_path_1]["name"].startswith(
                INJECTED_VOLUME_NAME_PREFIX
            )
            assert mounts_by_path[mount_path_2]["name"].startswith(
                INJECTED_VOLUME_NAME_PREFIX
            )

    async def test__inject_disk_by_name(
        self,
        service: Service,
        kube_client: KubeClient,
        scoped_namespace: tuple[Namespace, str, str],
        disk_with_name: Disk,
        disk_name: str,
    ) -> None:
        namespace, org, project = scoped_namespace

        # now let's create a POD with the proper annotation
        async with pod_cm(
            kube_client,
            namespace=namespace.name,
            annotations={
                ANNOTATION_APOLO_INJECT_DISK: json.dumps(
                    [
                        {
                            "mount_path": "/mnt/disk",
                            "disk_uri": f"disk://default/{org}/{project}/{disk_name}",
                        }
                    ]
                )
            },
            labels={
                LABEL_APOLO_ORG_NAME: org,
                LABEL_APOLO_PROJECT_NAME: project,
                ANNOTATION_APOLO_INJECT_DISK: "true",
            },
        ) as response:
            spec = response["spec"]
            volumes = spec["volumes"]
            container = spec["containers"][0]

            volumes = [v for v in volumes if "persistentVolumeClaim" in v]
            assert len(volumes) == 1
            assert volumes[0]["name"].startswith(INJECTED_VOLUME_NAME_PREFIX)
            # ensure claim name uses a disk ID (e.g. PVC name)
            assert volumes[0]["persistentVolumeClaim"]["claimName"] == disk_with_name.id

            mounts_by_path = {v["mountPath"]: v for v in container["volumeMounts"]}
            assert mounts_by_path["/mnt/disk"]["name"].startswith(
                INJECTED_VOLUME_NAME_PREFIX
            )

    async def test_inject_multiple_disks__one_by_id_another_by_name(
        self,
        service: Service,
        kube_client: KubeClient,
        scoped_namespace: tuple[Namespace, str, str],
    ) -> None:
        namespace, org, project = scoped_namespace

        # create two disks
        request = DiskRequest(
            storage=1024 * 1024,
            project_name=project,
            org_name=org,
        )
        disk_1 = await service.create_disk(request, "testuser")

        disk_2_name = "disk-2"
        request = DiskRequest(
            storage=1024 * 1024,
            project_name=project,
            org_name=org,
            name=disk_2_name,
        )
        disk_2 = await service.create_disk(request, "testuser")

        mount_path_1, mount_path_2 = "/mnt/disk1", "/mnt/disk2"
        async with pod_cm(
            kube_client,
            namespace.name,
            annotations={
                ANNOTATION_APOLO_INJECT_DISK: json.dumps(
                    [
                        {
                            "mount_path": mount_path_1,
                            "disk_uri": f"disk://default/{org}/{project}/{disk_1.id}",
                        },
                        {
                            "mount_path": mount_path_2,
                            "disk_uri": f"disk://default/{org}/{project}/{disk_2_name}",
                        },
                    ]
                ),
            },
            labels={
                LABEL_APOLO_ORG_NAME: org,
                LABEL_APOLO_PROJECT_NAME: project,
                ANNOTATION_APOLO_INJECT_DISK: "true",
            },
        ) as response:
            spec = response["spec"]
            volumes = spec["volumes"]
            container = spec["containers"][0]

            volumes = [v for v in volumes if "persistentVolumeClaim" in v]
            assert len(volumes) == 2

            for volume in volumes:
                assert volume["name"].startswith(INJECTED_VOLUME_NAME_PREFIX)
                assert volume["persistentVolumeClaim"]["claimName"] in {
                    disk_1.id,
                    disk_2.id,
                }

            mounts_by_path = {v["mountPath"]: v for v in container["volumeMounts"]}
            assert mounts_by_path[mount_path_1]["name"].startswith(
                INJECTED_VOLUME_NAME_PREFIX
            )
            assert mounts_by_path[mount_path_2]["name"].startswith(
                INJECTED_VOLUME_NAME_PREFIX
            )
