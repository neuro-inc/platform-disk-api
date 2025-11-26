from __future__ import annotations

from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from apolo_events_client import (
    Ack,
    EventType,
    RecvEvent,
    RecvEvents,
    StreamType,
    Tag,
)
from apolo_events_client.pytest import EventsQueues
from apolo_kube_client import V1Namespace

from platform_disk_api.api import create_app
from platform_disk_api.config import Config
from platform_disk_api.service import Disk, DiskRequest, Service

from .conftest import create_local_app_server


@pytest.fixture
async def disk_factory(
    service: Service,
    scoped_namespace: tuple[V1Namespace, str, str],
) -> Callable[[str], Coroutine[Any, Any, Disk]]:
    async def _factory(disk_name: str) -> Disk:
        namespace, org, project = scoped_namespace
        request = DiskRequest(
            storage=1024 * 1024,
            project_name=project,
            org_name=org,
            name=disk_name,
        )
        return await service.create_disk(request, "testuser")

    return _factory


async def test_deleter(
    config: Config,
    events_queues: EventsQueues,
    service: Service,
    disk_factory: Callable[[str], Coroutine[Any, Any, Disk]],
    scoped_namespace: tuple[V1Namespace, str, str],
) -> None:
    app = await create_app(config)
    async with create_local_app_server(app, port=8080):
        _, org, project = scoped_namespace

        await disk_factory("disk1")
        await disk_factory("disk2")

        disks = await service.get_project_disks(org, project, ensure_namespace=False)
        assert len(disks) == 2

        await events_queues.outcome.put(
            RecvEvents(
                subscr_id=uuid4(),
                events=[
                    RecvEvent(
                        tag=Tag("123"),
                        timestamp=datetime.now(tz=UTC),
                        sender="platform-admin",
                        stream=StreamType("platform-admin"),
                        event_type=EventType("project-remove"),
                        org=org,
                        cluster="cluster",
                        project=project,
                        user="testuser",
                    ),
                ],
            )
        )

        ev = await events_queues.income.get()
        assert isinstance(ev, Ack)
        assert ev.events[StreamType("platform-admin")] == ["123"]

        disks = await service.get_project_disks(org, project, ensure_namespace=False)
        assert disks == []
