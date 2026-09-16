from datetime import UTC, datetime
from typing import Any, cast

from apolo_events_client import EventType, RecvEvent, StreamType, Tag

from platform_disk_api.platform_deleter import ProjectDeleter
from platform_disk_api.service import Service


class FakeService:
    def __init__(self) -> None:
        self.removed: list[str] = []

    async def get_project_disks(
        self, org_name: str, project_name: str, *, ensure_namespace: bool = True
    ) -> list[str]:
        return [f"{org_name}/{project_name}/disk"]

    async def remove_disk(self, disk: Any, *, ensure_namespace: bool = True) -> None:
        self.removed.append(disk)


def make_event(cluster: str) -> RecvEvent:
    return RecvEvent(
        tag=Tag("1"),
        timestamp=datetime.now(tz=UTC),
        sender="platform-admin",
        stream=StreamType("platform-admin"),
        event_type=EventType("project-remove"),
        cluster=cluster,
        org="org",
        project="project",
        user="user",
    )


async def test_removes_disks_of_own_cluster() -> None:
    service = FakeService()
    deleter = ProjectDeleter(cast(Service, service), None, "apolo-main")

    await deleter._on_admin_event(make_event("apolo-main"))

    assert service.removed == ["org/project/disk"]


async def test_ignores_project_of_other_cluster() -> None:
    service = FakeService()
    deleter = ProjectDeleter(cast(Service, service), None, "apolo-main")

    await deleter._on_admin_event(make_event("alfa"))

    assert service.removed == []
