from datetime import timedelta

import pytest
from marshmallow import ValidationError

from platform_disk_api.schema import DiskRequestSchema, DiskSchema
from platform_disk_api.service import Disk
from platform_disk_api.utils import utc_now


def test_validate_disk_request_ok() -> None:
    request = DiskRequestSchema().load(
        {"storage": 2000, "project_name": "test-project", "org_name": "test-org"}
    )
    assert request.storage == 2000
    assert request.project_name == "test-project"
    assert request.org_name == "test-org"


def test_validate_disk_request_with_life_span_ok() -> None:
    request = DiskRequestSchema().load(
        {
            "storage": 2000,
            "life_span": 3600,
            "project_name": "test-project",
            "org_name": "test-org",
        }
    )
    assert request.storage == 2000
    assert request.life_span == timedelta(hours=1)


@pytest.mark.parametrize("name", ["cool-disk", "singleword", "word-1-digit"])
def test_validate_disk_request_with_name_ok(name: str) -> None:
    request = DiskRequestSchema().load(
        {
            "storage": 2000,
            "name": name,
            "project_name": "test-project",
            "org_name": "test-org",
        }
    )
    assert request.storage == 2000
    assert request.name == name


@pytest.mark.parametrize("name", ["with space", "1digit", "with-endline\n"])
def test_validate_disk_request_with_invalid_name_fail(name: str) -> None:
    with pytest.raises(ValidationError):
        DiskRequestSchema().load(
            {
                "storage": 2000,
                "name": name,
                "project_name": "test-project",
                "org_name": "test-org",
            }
        )


def test_validate_disk_request_no_storage() -> None:
    with pytest.raises(ValidationError):
        DiskRequestSchema().load({})


def test_validate_disk_request_no_project() -> None:
    with pytest.raises(ValidationError):
        DiskRequestSchema().load({"storage": 2000})


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
        org_name="test-org",
        project_name="test-project",
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
        "org_name": "test-org",
        "project_name": "test-project",
        "status": "Ready",
        "created_at": created_at.isoformat(),
        "last_usage": last_usage.isoformat(),
        "life_span": 86400,
        "used_bytes": 2000,
    }
