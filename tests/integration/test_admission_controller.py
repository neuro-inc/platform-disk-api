from __future__ import annotations

from collections.abc import Callable
import asyncio
from asyncio.timeouts import timeout
from typing import Any
from uuid import uuid4

import pytest
from apolo_kube_client.namespace import Namespace

from platform_disk_api.kube_client import KubeClient
from platform_disk_api.service import (
    DiskRequest,
    Service,
    APOLO_DISK_API_CREATED_AT_ANNOTATION,
    DISK_API_CREATED_AT_ANNOTATION,
    DISK_API_MARK_LABEL,
    APOLO_DISK_API_MARK_LABEL,
    DISK_API_NAME_ANNOTATION,
    APOLO_DISK_API_NAME_ANNOTATION,
    DISK_API_ORG_LABEL,
    APOLO_ORG_LABEL,
    DISK_API_PROJECT_LABEL,
    APOLO_PROJECT_LABEL,
    APOLO_USER_LABEL,
    USER_LABEL,
)


class TestAdmissionController:
    @pytest.fixture
    def service(
        self,
        kube_client: KubeClient,
        k8s_storage_class: str,
    ) -> Service:
        return Service(
            kube_client=kube_client,
            storage_class_name=k8s_storage_class,
        )

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
            manifest = {
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
            return manifest

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

    async def test__create_disk__no_name(
        self,
        kube_client: KubeClient,
        service: Service,
    ) -> None:
        """
        Creating a disk without a name shouldn't lead to a DiskNaming object creation
        """
        org_name, project_name = uuid4().hex, uuid4().hex
        request = DiskRequest(
            storage=1024 * 1024,
            project_name=project_name,
            org_name=org_name,
        )
        await service.create_disk(request, "testuser")
        disk_namings = await kube_client.list_disk_namings()
        assert not disk_namings

    async def test__create_disk__name_provided_disk_naming_created(
        self,
        kube_client: KubeClient,
        service: Service,
    ) -> None:
        """
        Whenever disk name is provided,
        admission controller will create a DiskNaming kube object
        """
        org_name, project_name = uuid4().hex, uuid4().hex
        disk_name = "test-disk"
        request = DiskRequest(
            storage=1024 * 1024,
            project_name=project_name,
            org_name=org_name,
            name=disk_name,
        )
        created_disk = await service.create_disk(request, "testuser")
        disk_namings = await kube_client.list_disk_namings()
        assert len(disk_namings) == 1
        disk_naming = disk_namings[0]
        assert disk_naming.name == f"{disk_name}--{org_name}--{project_name}"
        assert disk_naming.namespace == created_disk.namespace

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
        disk_name = "some-disk"
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
