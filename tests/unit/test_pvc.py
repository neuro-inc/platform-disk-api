import pytest

from platform_disk_api.kube_client import (
    PersistentVolumeClaimRead,
    PersistentVolumeClaimWrite,
)


class TestPVCSerialization:
    @pytest.mark.parametrize("name,storage_class,storage", [("test", "test-stor", 100)])
    def test_pvc_to_primitive(
        self, name: str, storage_class: str, storage: int
    ) -> None:
        pvc = PersistentVolumeClaimWrite(
            name=name, storage_class_name=storage_class, storage=storage,
        )
        assert pvc.to_primitive() == {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": name},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "volumeMode": "Filesystem",
                "resources": {"requests": {"storage": storage}},
                "storageClassName": storage_class,
            },
        }

    @pytest.mark.parametrize("name,storage_class,storage", [("test", "test-stor", 100)])
    def test_pvc_from_primitive_pending(
        self, name: str, storage_class: str, storage: int
    ) -> None:
        pvc = PersistentVolumeClaimRead.from_primitive(
            {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {"name": name},
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "volumeMode": "Filesystem",
                    "resources": {"requests": {"storage": storage}},
                    "storageClassName": storage_class,
                },
                "status": {"phase": PersistentVolumeClaimRead.Phase.PENDING},
            }
        )
        assert pvc == PersistentVolumeClaimRead(
            name=name,
            storage_class_name=storage_class,
            phase="Pending",
            storage_requested=storage,
            storage_real=None,
        )

    @pytest.mark.parametrize("name,storage_class,storage", [("test", "test-stor", 100)])
    def test_pvc_from_primitive_bound(
        self, name: str, storage_class: str, storage: int
    ) -> None:
        pvc = PersistentVolumeClaimRead.from_primitive(
            {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {"name": name},
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "volumeMode": "Filesystem",
                    "resources": {"requests": {"storage": storage}},
                    "storageClassName": storage_class,
                },
                "status": {
                    "phase": PersistentVolumeClaimRead.Phase.BOUND,
                    "capacity": {"storage": 2 * storage},
                },
            }
        )
        assert pvc == PersistentVolumeClaimRead(
            name=name,
            storage_class_name=storage_class,
            phase="Bound",
            storage_requested=storage,
            storage_real=2 * storage,
        )
