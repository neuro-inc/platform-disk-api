from datetime import timedelta

import pytest
from marshmallow import ValidationError

from platform_disk_api.schema import DiskRequestSchema, DiskSchema
from platform_disk_api.service import Disk
from platform_disk_api.utils import utc_now


def test_validate_disk_request_ok() -> None:
    request = DiskRequestSchema().load({"storage": 2000})
    assert request.storage == 2000


def test_validate_disk_request_with_life_span_ok() -> None:
    request = DiskRequestSchema().load({"storage": 2000, "life_span": 3600})
    assert request.storage == 2000
    assert request.life_span == timedelta(hours=1)


def test_validate_disk_request_with_name_ok() -> None:
    request = DiskRequestSchema().load({"storage": 2000, "name": "cool-disk"})
    assert request.storage == 2000
    assert request.name == "cool-disk"


def test_validate_disk_request_with_invalid_name_fail() -> None:
    with pytest.raises(ValidationError):
        DiskRequestSchema().load({"storage": 2000, "name": "cool disk"})


def test_validate_disk_request_no_storage() -> None:
    with pytest.raises(ValidationError):
        DiskRequestSchema().load({})


def test_validate_disk_request_storage_is_string() -> None:
    with pytest.raises(ValidationError):
        DiskRequestSchema().load({"storage": "20Gi"})


def test_validate_disk_serialize() -> None:
    last_usage = utc_now()
    created_at = last_usage - timedelta(days=2)

    disk = Disk(
        id="test-id",
        storage=4000,
        owner="user",
        name=None,
        status=Disk.Status.READY,
        last_usage=last_usage,
        created_at=created_at,
        life_span=timedelta(days=1),
        used_bytes=2000,
    )
    assert DiskSchema().dump(disk) == {
        "id": "test-id",
        "storage": 4000,
        "owner": "user",
        "name": None,
        "status": "Ready",
        "created_at": created_at.isoformat(),
        "last_usage": last_usage.isoformat(),
        "life_span": 86400,
        "used_bytes": 2000,
    }
