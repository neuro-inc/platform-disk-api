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
    def test_pvc_with_labels_to_primitive(
        self, name: str, storage_class: str, storage: int
    ) -> None:
        pvc = PersistentVolumeClaimWrite(
            name=name,
            storage_class_name=storage_class,
            storage=storage,
            labels=dict(foo="bar"),
        )
        assert pvc.to_primitive() == {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "labels": {"foo": "bar"}},
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
                "status": {"phase": "Pending"},
            }
        )
        assert pvc == PersistentVolumeClaimRead(
            name=name,
            storage_class_name=storage_class,
            phase=PersistentVolumeClaimRead.Phase.PENDING,
            storage_requested=storage,
            storage_real=None,
            labels=dict(),
            annotations=dict(),
        )

    @pytest.mark.parametrize(
        "storage_str,storage_value",
        [
            ("100", 100),
            ("1e2", 100),
            ("1Ki", 1024),
            ("13Mi", 13 * 1024 * 1024),
            ("22Gi", 22 * (1024 ** 3)),
            ("33Ti", 33 * (1024 ** 4)),
            ("44Pi", 44 * (1024 ** 5)),
            ("55Ei", 55 * (1024 ** 6)),
            ("1k", 1000),
            ("13M", 13 * (1000 ** 2)),
            ("22G", 22 * (1000 ** 3)),
            ("33T", 33 * (1000 ** 4)),
            ("44P", 44 * (1000 ** 5)),
            ("55E", 55 * (1000 ** 6)),
        ],
    )
    def test_pvc_storage_string_parsing(
        self, storage_str: str, storage_value: int
    ) -> None:
        pvc = PersistentVolumeClaimRead.from_primitive(
            {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {"name": "test"},
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "volumeMode": "Filesystem",
                    "resources": {"requests": {"storage": storage_str}},
                    "storageClassName": "test",
                },
                "status": {"phase": "Bound", "capacity": {"storage": storage_str}},
            }
        )
        assert pvc.storage_requested == storage_value
        assert pvc.storage_real == storage_value

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
                "status": {"phase": "Bound", "capacity": {"storage": 2 * storage}},
            }
        )
        assert pvc == PersistentVolumeClaimRead(
            name=name,
            storage_class_name=storage_class,
            phase=PersistentVolumeClaimRead.Phase.BOUND,
            storage_requested=storage,
            storage_real=2 * storage,
            labels=dict(),
            annotations=dict(),
        )

    @pytest.mark.parametrize("name,storage_class,storage", [("test", "test-stor", 100)])
    def test_pvc_from_primitive_with_labels(
        self, name: str, storage_class: str, storage: int
    ) -> None:
        pvc = PersistentVolumeClaimRead.from_primitive(
            {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {"name": name, "labels": {"foo": "bar"}},
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "volumeMode": "Filesystem",
                    "resources": {"requests": {"storage": storage}},
                    "storageClassName": storage_class,
                },
                "status": {"phase": "Bound", "capacity": {"storage": 2 * storage}},
            }
        )
        assert pvc == PersistentVolumeClaimRead(
            name=name,
            storage_class_name=storage_class,
            phase=PersistentVolumeClaimRead.Phase.BOUND,
            storage_requested=storage,
            storage_real=2 * storage,
            labels=dict(foo="bar"),
            annotations=dict(),
        )

    @pytest.mark.parametrize("name,storage_class,storage", [("test", "test-stor", 100)])
    def test_pvc_from_primitive_with_annotations(
        self, name: str, storage_class: str, storage: int
    ) -> None:
        pvc = PersistentVolumeClaimRead.from_primitive(
            {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {"name": name, "annotations": {"foo": "bar"}},
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "volumeMode": "Filesystem",
                    "resources": {"requests": {"storage": storage}},
                    "storageClassName": storage_class,
                },
                "status": {"phase": "Bound", "capacity": {"storage": 2 * storage}},
            }
        )
        assert pvc == PersistentVolumeClaimRead(
            name=name,
            storage_class_name=storage_class,
            phase=PersistentVolumeClaimRead.Phase.BOUND,
            storage_requested=storage,
            storage_real=2 * storage,
            labels=dict(),
            annotations=dict(foo="bar"),
        )
