from __future__ import annotations

import asyncio
import json
from asyncio.timeouts import timeout
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import pytest
from apolo_kube_client import (
    KubeClient,
    KubeClientException,
    KubeClientProxy,
    ResourceInvalid,
    ResourceNotFound,
    V1Container,
    V1LabelSelector,
    V1Namespace,
    V1ObjectMeta,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1Pod,
    V1PodSpec,
    V1PodTemplateSpec,
    V1StatefulSet,
    V1StatefulSetSpec,
    V1VolumeMount,
    V1VolumeResourceRequirements,
)

from platform_disk_api.admission_controller.api import (
    ANNOTATION_APOLO_INJECT_DISK,
    INJECTED_VOLUME_NAME_PREFIX,
    LABEL_APOLO_ORG_NAME,
    LABEL_APOLO_PROJECT_NAME,
)
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
    VCLUSTER_OBJECT_NAME_ANNOTATION,
    Disk,
    DiskRequest,
    Service,
)


@asynccontextmanager
async def pod_cm(
    kube_client: KubeClientProxy,
    annotations: dict[str, Any] | None = None,
    labels: dict[str, Any] | None = None,
) -> AsyncIterator[V1Pod]:
    """
    A context manager for creating the pod, returning the response,
    and deleting the POD at the end
    """
    pod_name = str(uuid4())

    pod = V1Pod(
        metadata=V1ObjectMeta(name=pod_name),
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="hello",
                    image="busybox",
                    command=["sh", "-c", "sleep 5"],
                )
            ]
        ),
    )

    if annotations is not None:
        pod.metadata.annotations = annotations

    if labels is not None:
        pod.metadata.labels = labels

    pod = await kube_client.core_v1.pod.create(model=pod)

    # wait until a POD is running
    async with asyncio.timeout(60):
        while pod.status.phase != "Running":
            pod = await kube_client.core_v1.pod.get(name=pod_name)
            await asyncio.sleep(0.5)

    yield pod

    await kube_client.core_v1.pod.delete(name=pod_name)


class TestAdmissionController:
    @pytest.fixture
    def statefulset_manifest_factory(
        self,
        k8s_storage_class: str,
    ) -> Callable[..., V1StatefulSet]:
        def _factory(
            labels: dict[str, str],
            annotations: dict[str, str],
            storage_class_name: str = k8s_storage_class,
        ) -> V1StatefulSet:
            return V1StatefulSet(
                metadata=V1ObjectMeta(name=f"test-statefulset-{uuid4().hex}"),
                spec=V1StatefulSetSpec(
                    service_name="ubuntu-service",
                    replicas=2,
                    selector=V1LabelSelector(match_labels={"app": "ubuntu"}),
                    template=V1PodTemplateSpec(
                        metadata=V1ObjectMeta(labels={"app": "ubuntu"}),
                        spec=V1PodSpec(
                            containers=[
                                V1Container(
                                    name="ubuntu",
                                    image="ubuntu:latest",
                                    command=["sleep", "infinity"],
                                    volume_mounts=[
                                        V1VolumeMount(
                                            name="ubuntu-data",
                                            mount_path="/mnt/data",
                                        )
                                    ],
                                )
                            ],
                            termination_grace_period_seconds=1,
                        ),
                    ),
                    volume_claim_templates=[
                        V1PersistentVolumeClaim(
                            metadata=V1ObjectMeta(
                                name="ubuntu-data",
                                annotations=annotations,
                                labels=labels,
                            ),
                            spec=V1PersistentVolumeClaimSpec(
                                access_modes=["ReadWriteOnce"],
                                resources=V1VolumeResourceRequirements(
                                    requests={"storage": "10Mi"}
                                ),
                                storage_class_name=storage_class_name,
                            ),
                        )
                    ],
                ),
            )

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
        async with timeout(60):
            while True:
                statefulset = await kube_client.apps_v1.statefulset.get(
                    name=name, namespace=namespace_name
                )
                assert statefulset.spec is not None
                assert statefulset.status is not None
                requested_replicas = statefulset.spec.replicas
                actual_replicas = statefulset.status.current_replicas or 0
                if requested_replicas == actual_replicas:
                    break
                await asyncio.sleep(1)

    @pytest.fixture
    async def disk_no_name(
        self,
        service: Service,
        scoped_namespace: tuple[V1Namespace, str, str],
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
        scoped_namespace: tuple[V1Namespace, str, str],
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
        disk_naming_list = await kube_client.neuromation_io_v1.disk_naming.get_list()
        assert not disk_naming_list.items

    async def test__create_disk__name_provided_disk_naming_created(
        self,
        kube_client: KubeClient,
        service: Service,
        disk_with_name: Disk,
        scoped_namespace: tuple[V1Namespace, str, str],
    ) -> None:
        """
        Whenever a disk name is provided,
        admission controller will create a DiskNaming kube object
        """
        namespace, org, project = scoped_namespace
        assert namespace.metadata.name is not None
        disk_naming_list = await kube_client.neuromation_io_v1.disk_naming.get_list(
            namespace=namespace.metadata.name
        )
        assert len(disk_naming_list.items) == 1
        disk_naming = disk_naming_list.items[0]
        assert disk_naming.metadata.name == f"{disk_with_name.name}--{org}--{project}"
        assert disk_naming.metadata.namespace == disk_with_name.namespace

    async def test__create_statefulset__no_name(
        self,
        service: Service,
        kube_client: KubeClient,
        k8s_storage_class: str,
        statefulset_manifest_factory: Callable[..., V1StatefulSet],
        scoped_namespace: tuple[V1Namespace, str, str],
    ) -> None:
        """
        Ensure that PVC is created properly for the statefulset,
        based on a volumeClaimTemplate
        """
        namespace, org, project = scoped_namespace
        assert namespace.metadata.name is not None
        statefulset = statefulset_manifest_factory(
            labels={},
            annotations={},
            storage_class_name=k8s_storage_class,
        )
        statefulset = await kube_client.apps_v1.statefulset.create(
            model=statefulset,
            namespace=namespace.metadata.name,
        )

        assert statefulset.metadata.name is not None
        assert namespace.metadata.name is not None
        # let's wait for statefulset pods to be running
        await self._wait_statefulset(
            kube_client=kube_client,
            name=statefulset.metadata.name,
            namespace_name=namespace.metadata.name,
        )

        # there should be two PVCs created
        pvcs = await kube_client.core_v1.persistent_volume_claim.get_list(
            namespace=namespace.metadata.name,
            label_selector=f"{DISK_API_MARK_LABEL}=true,release!=vcluster",
        )
        assert len(pvcs.items) == 2

        for pvc in pvcs.items:
            assert APOLO_DISK_API_CREATED_AT_ANNOTATION in pvc.metadata.annotations
            assert DISK_API_CREATED_AT_ANNOTATION in pvc.metadata.annotations

            # ensure name annotation does not present
            assert APOLO_DISK_API_NAME_ANNOTATION not in pvc.metadata.annotations
            assert DISK_API_NAME_ANNOTATION not in pvc.metadata.annotations

            assert pvc.metadata.labels[DISK_API_MARK_LABEL] == "true"
            assert pvc.metadata.labels[APOLO_DISK_API_MARK_LABEL] == "true"
            assert pvc.metadata.labels[DISK_API_ORG_LABEL] == org
            assert pvc.metadata.labels[APOLO_ORG_LABEL] == org
            assert pvc.metadata.labels[DISK_API_PROJECT_LABEL] == project
            assert pvc.metadata.labels[APOLO_PROJECT_LABEL] == project
            assert pvc.metadata.labels[APOLO_USER_LABEL] == project
            assert pvc.metadata.labels[USER_LABEL] == project

        # no disk namings should be created
        disk_naming_list = await kube_client.neuromation_io_v1.disk_naming.get_list(
            namespace=namespace.metadata.name
        )
        assert not disk_naming_list.items
        await self._delete_stateful_set(
            kube_client, statefulset.metadata.name, namespace.metadata.name
        )

    async def test__create_statefulset__with_name(
        self,
        service: Service,
        kube_client: KubeClient,
        k8s_storage_class: str,
        statefulset_manifest_factory: Callable[..., V1StatefulSet],
        scoped_namespace: tuple[V1Namespace, str, str],
    ) -> None:
        """
        Ensure that both PVC and DiskNaming are created properly for the statefulset,
        based on a volumeClaimTemplate
        """
        disk_name = "test-disk"
        namespace, org, project = scoped_namespace
        assert namespace.metadata.name is not None
        statefulset = statefulset_manifest_factory(
            labels={},
            annotations={APOLO_DISK_API_NAME_ANNOTATION: disk_name},
            storage_class_name=k8s_storage_class,
        )
        statefulset = await kube_client.apps_v1.statefulset.create(
            model=statefulset,
            namespace=namespace.metadata.name,
        )
        assert statefulset.metadata.name is not None
        # let's wait for statefulset pods to be running
        await self._wait_statefulset(
            kube_client=kube_client,
            name=statefulset.metadata.name,
            namespace_name=namespace.metadata.name,
        )

        # there should be two PVCs created
        pvc_list = await kube_client.core_v1.persistent_volume_claim.get_list(
            namespace=namespace.metadata.name,
            label_selector=f"{DISK_API_MARK_LABEL}=true,release!=vcluster",
        )
        assert len(pvc_list.items) == 2

        for idx, pvc in enumerate(
            sorted(
                pvc_list.items,
                key=lambda p: p.metadata.annotations[APOLO_DISK_API_NAME_ANNOTATION],
            )
        ):
            assert APOLO_DISK_API_CREATED_AT_ANNOTATION in pvc.metadata.annotations
            assert DISK_API_CREATED_AT_ANNOTATION in pvc.metadata.annotations

            # ensure name annotation is present now
            assert (
                pvc.metadata.annotations[APOLO_DISK_API_NAME_ANNOTATION]
                == f"{disk_name}-{idx}"
            )

            assert pvc.metadata.labels[DISK_API_MARK_LABEL] == "true"
            assert pvc.metadata.labels[APOLO_DISK_API_MARK_LABEL] == "true"
            assert pvc.metadata.labels[DISK_API_ORG_LABEL] == org
            assert pvc.metadata.labels[APOLO_ORG_LABEL] == org
            assert pvc.metadata.labels[DISK_API_PROJECT_LABEL] == project
            assert pvc.metadata.labels[APOLO_PROJECT_LABEL] == project
            assert pvc.metadata.labels[APOLO_USER_LABEL] == project
            assert pvc.metadata.labels[USER_LABEL] == project

        # ensure that both disk namings are now created
        disk_naming_list = await kube_client.neuromation_io_v1.disk_naming.get_list(
            namespace=namespace.metadata.name
        )
        assert len(disk_naming_list.items) == 2

        for idx, disk_naming in enumerate(
            sorted(disk_naming_list.items, key=lambda d: str(d.metadata.name))
        ):
            assert disk_naming.metadata.name == f"{disk_name}-{idx}--{org}--{project}"

        await self._delete_stateful_set(
            kube_client, statefulset.metadata.name, namespace.metadata.name
        )

    @staticmethod
    async def _delete_stateful_set(
        kube_client: KubeClient, name: str, namespace: str
    ) -> None:
        await kube_client.apps_v1.statefulset.delete(
            name=name,
            namespace=namespace,
        )
        async with timeout(30):
            while True:
                try:
                    await kube_client.apps_v1.statefulset.get(
                        name=name,
                        namespace=namespace,
                    )
                except ResourceNotFound:
                    break
                finally:
                    await asyncio.sleep(1)

    async def test__create_statefulset__invalid_storage_class(
        self,
        service: Service,
        kube_client: KubeClient,
        k8s_storage_class: str,
        statefulset_manifest_factory: Callable[..., V1StatefulSet],
        scoped_namespace: tuple[V1Namespace, str, str],
    ) -> None:
        """
        Ensures that the admission controller will use
        a proper storage class available to it
        """
        namespace, org, project = scoped_namespace
        assert namespace.metadata.name is not None
        statefulset = statefulset_manifest_factory(
            labels={},
            annotations={},
            storage_class_name="invalid-storage-class",
        )
        statefulset = await kube_client.apps_v1.statefulset.create(
            model=statefulset,
            namespace=namespace.metadata.name,
        )
        assert statefulset.metadata.name is not None
        # let's wait for statefulset pods to be running
        await self._wait_statefulset(
            kube_client=kube_client,
            name=statefulset.metadata.name,
            namespace_name=namespace.metadata.name,
        )

        # ensure storage class was overridden
        pvc_list = await kube_client.core_v1.persistent_volume_claim.get_list(
            namespace=namespace.metadata.name
        )
        pvc: V1PersistentVolumeClaim
        for pvc in pvc_list.items:
            assert pvc.spec.storage_class_name == k8s_storage_class

        await kube_client.apps_v1.statefulset.delete(
            name=statefulset.metadata.name,
            namespace=namespace.metadata.name,
        )

    async def test__pod_without_annotations_will_be_ignored(
        self,
        service: Service,
        scoped_kube_client: KubeClientProxy,
    ) -> None:
        async with pod_cm(scoped_kube_client) as pod:
            assert pod.kind == "Pod"

    async def test__pod_invalid_annotation_will_prohibit_pod_creation(
        self,
        service: Service,
        scoped_kube_client: KubeClientProxy,
        org_project: tuple[str, str],
    ) -> None:
        org, project = org_project
        if org.startswith("vcluster-"):
            pytest.skip("not applicable for vcluster org_project")
        with pytest.raises(ResourceInvalid) as e:
            async with pod_cm(
                scoped_kube_client,
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
        scoped_kube_client: KubeClientProxy,
        org_project: tuple[str, str],
    ) -> None:
        org, project = org_project
        if org.startswith("vcluster-"):
            pytest.skip("not applicable for vcluster org_project")
        with pytest.raises(KubeClientException) as e:
            async with pod_cm(
                scoped_kube_client,
                labels={
                    LABEL_APOLO_ORG_NAME: "invalid-org",
                    LABEL_APOLO_PROJECT_NAME: project,
                    ANNOTATION_APOLO_INJECT_DISK: "true",
                },
                annotations={
                    ANNOTATION_APOLO_INJECT_DISK: json.dumps(
                        [
                            {
                                "mount_path": "/mnt/disk",
                                "disk_uri": (f"disk://default/{org}/{project}/any"),
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
        scoped_kube_client: KubeClientProxy,
        org_project: tuple[str, str],
    ) -> None:
        org, project = org_project
        if org.startswith("vcluster-"):
            pytest.skip("not applicable for vcluster org_project")
        with pytest.raises(KubeClientException) as e:
            async with pod_cm(
                scoped_kube_client,
                labels={
                    LABEL_APOLO_ORG_NAME: org,
                    LABEL_APOLO_PROJECT_NAME: "invalid-project",
                    ANNOTATION_APOLO_INJECT_DISK: "true",
                },
                annotations={
                    ANNOTATION_APOLO_INJECT_DISK: json.dumps(
                        [
                            {
                                "mount_path": "/mnt/disk",
                                "disk_uri": (f"disk://default/{org}/{project}/any"),
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
        scoped_kube_client: KubeClientProxy,
        org_project: tuple[str, str],
    ) -> None:
        org, project = org_project
        if org.startswith("vcluster-"):
            pytest.skip("not applicable for vcluster org_project")
        with pytest.raises(KubeClientException) as e:
            async with pod_cm(
                scoped_kube_client,
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
                                "disk_uri": (
                                    f"disk://default/invalid-org/{project}/any"
                                ),
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
        scoped_kube_client: KubeClientProxy,
        org_project: tuple[str, str],
    ) -> None:
        org, project = org_project
        if org.startswith("vcluster-"):
            pytest.skip("not applicable for vcluster org_project")
        with pytest.raises(KubeClientException) as e:
            async with pod_cm(
                scoped_kube_client,
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
                                "disk_uri": (
                                    f"disk://default/{org}/invalid-project/any"
                                ),
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
        scoped_kube_client: KubeClientProxy,
        kube_client: KubeClient,
        scoped_namespace: tuple[V1Namespace, str, str],
        disk_no_name: Disk,
    ) -> None:
        namespace, org, project = scoped_namespace
        assert namespace.metadata.name is not None
        test_id = uuid4().hex

        # now let's create a POD with the proper annotation
        async with pod_cm(
            scoped_kube_client,
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
                "disk-api-test-id": test_id,
            },
        ):
            pods = await kube_client.core_v1.pod.get_list(
                namespace=namespace.metadata.name,
                label_selector=(
                    f"{ANNOTATION_APOLO_INJECT_DISK}=true,disk-api-test-id={test_id}"
                ),
            )
            assert len(pods.items) == 1
            pod = pods.items[0]
            assert pod.spec is not None
            container = pod.spec.containers[0]

            # those volumes may have different names in a host cluster,
            # in a case of vcluster scope test
            volumes = [v for v in pod.spec.volumes if v.persistent_volume_claim]
            assert len(volumes) == 1
            assert volumes[0].name.startswith(INJECTED_VOLUME_NAME_PREFIX)
            assert volumes[0].persistent_volume_claim is not None

            # get host cluster PVCs
            pvc = await kube_client.core_v1.persistent_volume_claim.get(
                name=volumes[0].persistent_volume_claim.claim_name,
                namespace=namespace.metadata.name,
            )
            pvc_annotations = pvc.metadata.annotations or {}
            disk_id = pvc_annotations.get(
                VCLUSTER_OBJECT_NAME_ANNOTATION,
                pvc.metadata.name,
            )
            assert disk_id == disk_no_name.id

            mounts_by_path = {v.mount_path: v for v in container.volume_mounts}
            assert mounts_by_path["/mnt/disk"].name.startswith(
                INJECTED_VOLUME_NAME_PREFIX
            )

    async def test_inject_multiple_disks(
        self,
        service: Service,
        scoped_kube_client: KubeClientProxy,
        kube_client: KubeClient,
        scoped_namespace: tuple[V1Namespace, str, str],
    ) -> None:
        namespace, org, project = scoped_namespace
        assert namespace.metadata.name is not None
        test_id = uuid4().hex

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
            scoped_kube_client,
            annotations={
                ANNOTATION_APOLO_INJECT_DISK: json.dumps(
                    [
                        {
                            "mount_path": mount_path_1,
                            "disk_uri": (f"disk://default/{org}/{project}/{disk_1.id}"),
                        },
                        {
                            "mount_path": mount_path_2,
                            "disk_uri": (f"disk://default/{org}/{project}/{disk_2.id}"),
                        },
                    ]
                ),
            },
            labels={
                LABEL_APOLO_ORG_NAME: org,
                LABEL_APOLO_PROJECT_NAME: project,
                ANNOTATION_APOLO_INJECT_DISK: "true",
                "disk-api-test-id": test_id,
            },
        ):
            pods = await kube_client.core_v1.pod.get_list(
                namespace=namespace.metadata.name,
                label_selector=(
                    f"{ANNOTATION_APOLO_INJECT_DISK}=true,disk-api-test-id={test_id}"
                ),
            )
            assert len(pods.items) == 1
            pod = pods.items[0]
            assert pod.spec is not None
            container = pod.spec.containers[0]

            volumes = [v for v in pod.spec.volumes if v.persistent_volume_claim]
            assert len(volumes) == 2

            pvc_names = {
                v.persistent_volume_claim.claim_name
                for v in volumes
                if v.persistent_volume_claim is not None
            }
            assert len(pvc_names) == 2

            expected_ids = {disk_1.id, disk_2.id}
            resolved_ids = set()
            for pvc_name in pvc_names:
                pvc = await kube_client.core_v1.persistent_volume_claim.get(
                    # noqa: E501
                    name=pvc_name,
                    namespace=namespace.metadata.name,
                )
                pvc_annotations = pvc.metadata.annotations or {}
                disk_id = pvc_annotations.get(
                    VCLUSTER_OBJECT_NAME_ANNOTATION,
                    pvc.metadata.name,
                )
                resolved_ids.add(disk_id)

            assert resolved_ids == expected_ids

            mounts_by_path = {v.mount_path: v for v in container.volume_mounts}
            assert mounts_by_path[mount_path_1].name.startswith(
                INJECTED_VOLUME_NAME_PREFIX
            )
            assert mounts_by_path[mount_path_2].name.startswith(
                INJECTED_VOLUME_NAME_PREFIX
            )

    async def test__inject_disk_by_name(
        self,
        service: Service,
        scoped_kube_client: KubeClientProxy,
        kube_client: KubeClient,
        scoped_namespace: tuple[V1Namespace, str, str],
        disk_with_name: Disk,
        disk_name: str,
    ) -> None:
        namespace, org, project = scoped_namespace
        assert namespace.metadata.name is not None
        test_id = uuid4().hex

        async with pod_cm(
            scoped_kube_client,
            annotations={
                ANNOTATION_APOLO_INJECT_DISK: json.dumps(
                    [
                        {
                            "mount_path": "/mnt/disk",
                            "disk_uri": (f"disk://default/{org}/{project}/{disk_name}"),
                        }
                    ]
                )
            },
            labels={
                LABEL_APOLO_ORG_NAME: org,
                LABEL_APOLO_PROJECT_NAME: project,
                ANNOTATION_APOLO_INJECT_DISK: "true",
                "disk-api-test-id": test_id,
            },
        ):
            pods = await kube_client.core_v1.pod.get_list(
                namespace=namespace.metadata.name,
                label_selector=(
                    f"{ANNOTATION_APOLO_INJECT_DISK}=true,disk-api-test-id={test_id}"
                ),
            )
            assert len(pods.items) == 1
            pod = pods.items[0]
            assert pod.spec is not None
            container = pod.spec.containers[0]

            volumes = [v for v in pod.spec.volumes if v.persistent_volume_claim]
            assert len(volumes) == 1
            volume = volumes[0]
            assert volume.name.startswith(INJECTED_VOLUME_NAME_PREFIX)
            assert volume.persistent_volume_claim is not None

            pvc = await kube_client.core_v1.persistent_volume_claim.get(
                # noqa: E501
                name=volume.persistent_volume_claim.claim_name,
                namespace=namespace.metadata.name,
            )
            pvc_annotations = pvc.metadata.annotations or {}
            disk_id = pvc_annotations.get(
                VCLUSTER_OBJECT_NAME_ANNOTATION,
                pvc.metadata.name,
            )
            assert disk_id == disk_with_name.id

            mounts_by_path = {v.mount_path: v for v in container.volume_mounts}

            assert mounts_by_path["/mnt/disk"].name.startswith(
                INJECTED_VOLUME_NAME_PREFIX
            )

    async def test_inject_multiple_disks__one_by_id_another_by_name(
        self,
        service: Service,
        scoped_kube_client: KubeClientProxy,
        kube_client: KubeClient,
        scoped_namespace: tuple[V1Namespace, str, str],
    ) -> None:
        namespace, org, project = scoped_namespace
        assert namespace.metadata.name is not None
        test_id = uuid4().hex

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
        assert namespace.metadata.name is not None

        async with pod_cm(
            scoped_kube_client,
            annotations={
                ANNOTATION_APOLO_INJECT_DISK: json.dumps(
                    [
                        {
                            "mount_path": mount_path_1,
                            "disk_uri": (f"disk://default/{org}/{project}/{disk_1.id}"),
                        },
                        {
                            "mount_path": mount_path_2,
                            "disk_uri": (
                                f"disk://default/{org}/{project}/{disk_2_name}"
                            ),
                        },
                    ]
                ),
            },
            labels={
                LABEL_APOLO_ORG_NAME: org,
                LABEL_APOLO_PROJECT_NAME: project,
                ANNOTATION_APOLO_INJECT_DISK: "true",
                "disk-api-test-id": test_id,
            },
        ):
            pods = await kube_client.core_v1.pod.get_list(
                namespace=namespace.metadata.name,
                label_selector=(
                    f"{ANNOTATION_APOLO_INJECT_DISK}=true,disk-api-test-id={test_id}"
                ),
            )
            assert len(pods.items) == 1
            pod = pods.items[0]
            assert pod.spec is not None
            container = pod.spec.containers[0]

            volumes = [v for v in pod.spec.volumes if v.persistent_volume_claim]
            assert len(volumes) == 2

            pvc_names = {
                v.persistent_volume_claim.claim_name
                for v in volumes
                if v.persistent_volume_claim is not None
            }
            assert len(pvc_names) == 2

            expected_ids = {disk_1.id, disk_2.id}
            resolved_ids = set()
            for pvc_name in pvc_names:
                pvc = await kube_client.core_v1.persistent_volume_claim.get(
                    name=pvc_name,
                    namespace=namespace.metadata.name,
                )
                pvc_annotations = pvc.metadata.annotations or {}
                disk_id = pvc_annotations.get(
                    VCLUSTER_OBJECT_NAME_ANNOTATION,
                    pvc.metadata.name,
                )
                resolved_ids.add(disk_id)

            assert resolved_ids == expected_ids

            mounts_by_path = {v.mount_path: v for v in container.volume_mounts}
            assert mounts_by_path[mount_path_1].name.startswith(
                INJECTED_VOLUME_NAME_PREFIX
            )
            assert mounts_by_path[mount_path_2].name.startswith(
                INJECTED_VOLUME_NAME_PREFIX
            )
