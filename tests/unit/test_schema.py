import pytest
from marshmallow import ValidationError

from platform_disk_api.schema import DiskRequestSchema, DiskSchema
from platform_disk_api.service import Disk


def test_validate_storage_storage_ok() -> None:
    request = DiskRequestSchema().load({"storage": 2000})
    assert request.storage == 2000


def test_validate_storage_no_storage() -> None:
    with pytest.raises(ValidationError):
        DiskRequestSchema().load({})


def test_validate_storage_storage_is_string() -> None:
    with pytest.raises(ValidationError):
        DiskRequestSchema().load({"storage": "20Gi"})


def test_validate_disk_serialize() -> None:
    disk = Disk(id="test-id", storage=4000, owner="user", status=Disk.Status.READY)
    assert DiskSchema().dump(disk) == {
        "id": "test-id",
        "storage": 4000,
        "owner": "user",
        "status": "Ready",
    }
