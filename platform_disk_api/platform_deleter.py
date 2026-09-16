import logging
from typing import Self

from apolo_events_client import (
    EventsClientConfig,
    EventType,
    RecvEvent,
    StreamType,
    from_config,
)

from .service import Service


logger = logging.getLogger(__name__)


class ProjectDeleter:
    ADMIN_STREAM = StreamType("platform-admin")
    PROJECT_REMOVE = EventType("project-remove")

    def __init__(
        self,
        disk_service: Service,
        config: EventsClientConfig | None,
        cluster_name: str,
    ) -> None:
        self._disk_service = disk_service
        self._client = from_config(config)
        self._cluster_name = cluster_name

    async def __aenter__(self) -> Self:
        logger.info("Subscribe for %r", self.ADMIN_STREAM)
        await self._client.subscribe_group(
            self.ADMIN_STREAM, self._on_admin_event, auto_ack=True
        )
        logger.info("Subscribed")
        return self

    async def __aexit__(self, exc_typ: object, exc_val: object, exc_tb: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _on_admin_event(self, ev: RecvEvent) -> None:
        if ev.event_type == self.PROJECT_REMOVE:
            if ev.cluster != self._cluster_name:
                logger.warning(
                    "Skip %s for cluster %r, this is %r",
                    ev.event_type,
                    ev.cluster,
                    self._cluster_name,
                )
                return
            assert ev.org
            assert ev.project

            for disk in await self._disk_service.get_project_disks(
                org_name=ev.org,
                project_name=ev.project,
                ensure_namespace=False,
            ):
                await self._disk_service.remove_disk(disk=disk, ensure_namespace=False)
