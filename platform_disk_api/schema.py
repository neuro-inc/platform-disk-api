from typing import Any

from marshmallow import Schema, fields, post_load, validate

from platform_disk_api.service import Disk, DiskRequest


DISK_NAME_PATTERN = r"[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*"


class DiskRequestSchema(Schema):
    name = fields.String(required=True, validate=validate.Regexp(DISK_NAME_PATTERN))
    storage = fields.Integer(required=True, validate=validate.Range(min=0))

    @post_load
    def make_request(self, data: Any, **kwargs: Any) -> DiskRequest:
        return DiskRequest(**data)


class DiskSchema(Schema):
    name = fields.String(validate=validate.Regexp(DISK_NAME_PATTERN))
    storage = fields.Integer(validate=validate.Range(min=0))
    status = fields.String(validate=validate.OneOf(list(map(str, Disk.Status))))

    @post_load
    def make_disk(self, data: Any, **kwargs: Any) -> Disk:
        return Disk(**data)
