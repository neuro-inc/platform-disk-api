from typing import Any

from marshmallow import Schema, fields, post_load, validate

from platform_disk_api.service import Disk, DiskRequest


class DiskRequestSchema(Schema):
    storage = fields.Integer(required=True, validate=validate.Range(min=0))

    @post_load
    def make_request(self, data: Any, **kwargs: Any) -> DiskRequest:
        return DiskRequest(**data)


class DiskSchema(Schema):
    id = fields.String(required=True)
    storage = fields.Integer(required=True, validate=validate.Range(min=0))
    status = fields.String(
        required=True, validate=validate.OneOf(list(map(str, Disk.Status)))
    )
    owner = fields.String(required=True)

    @post_load
    def make_disk(self, data: Any, **kwargs: Any) -> Disk:
        return Disk(**data)


class ClientErrorSchema(Schema):
    code = fields.String(required=True)
    description = fields.String(required=True)
