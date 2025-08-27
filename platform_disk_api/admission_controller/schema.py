from enum import StrEnum
from functools import cached_property
from pathlib import Path, PurePosixPath

from pydantic import BaseModel, TypeAdapter, field_validator


SCHEMA_DISK = "disk://"


class MountMode(StrEnum):
    READ_ONLY = "r"
    READ_WRITE = "rw"


class MountSchema(BaseModel):
    mount_path: str
    disk_uri: str
    mount_mode: MountMode = MountMode.READ_WRITE  # RW as a default

    @cached_property
    def _uri_parts(self) -> tuple[str, ...]:
        return Path(self.disk_uri).parts

    @cached_property
    def org(self) -> str:
        _, _, org, *_ = self._uri_parts
        return org

    @cached_property
    def project(self) -> str:
        _, _, _, project, *_ = self._uri_parts
        return project

    @cached_property
    def disk_id_or_name(self) -> str:
        *_, disk_id_or_name = self._uri_parts
        return disk_id_or_name

    @field_validator("mount_path", mode="after")
    @classmethod
    def is_mount_path(cls, value: str) -> str:
        if not Path(value).is_absolute():
            err = f"`{value}` is not an absolute path"
            raise ValueError(err)
        return value

    @field_validator("disk_uri", mode="after")
    @classmethod
    def is_disk_uri(cls, value: str) -> str:
        if not value.startswith(SCHEMA_DISK):
            err = f"`{value}` does not follow the {SCHEMA_DISK} schema"
            raise ValueError(err)
        path = PurePosixPath(value)
        if len(path.parts) < 4:
            err = (
                f"`{value}` is invalid. "
                "Cluster, org and project names must be present in the disk URI"
            )
            raise ValueError(err)
        return value


InjectionSchema = TypeAdapter(list[MountSchema])
