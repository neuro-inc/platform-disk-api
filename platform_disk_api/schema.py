from typing import Any

from marshmallow import Schema, fields, post_load, validate

from platform_disk_api.service import Disk, DiskRequest, NO_ORG


class DiskRequestSchema(Schema):
    storage = fields.Integer(required=True, validate=validate.Range(min=0))
    life_span = fields.TimeDelta(required=False, allow_none=True)
    name = fields.String(
        required=False,
        allow_none=True,
        validate=[
            validate.Regexp(r"^[a-z](?:-?[a-z0-9])*(?!\n)$"),
            validate.Length(min=3, max=40),
        ],
    )
    org_name = fields.String(
        required=False,
        allow_none=True,
    )
    project_name = fields.String(required=True)

    @post_load
    def make_request(self, data: Any, **kwargs: Any) -> DiskRequest:
        data["org_name"] = kwargs.get("org_name") or NO_ORG
        return DiskRequest(**data)


class DiskSchema(Schema):
    id = fields.String(required=True)
    storage = fields.Integer(required=True, validate=validate.Range(min=0))
    status = fields.String(
        required=True, validate=validate.OneOf(list(map(str, Disk.Status)))
    )
    owner = fields.String(required=True)
    name = fields.String(required=True, allow_none=True)
    org_name = fields.String(required=True, allow_none=True)
    project_name = fields.String(required=True)
    created_at = fields.DateTime(required=True)
    last_usage = fields.DateTime(required=True, allow_none=True)
    life_span = fields.TimeDelta(required=True, allow_none=True)
    used_bytes = fields.Integer(
        required=True, allow_none=True, validate=validate.Range(min=0)
    )

    @post_load
    def make_disk(self, data: Any, **kwargs: Any) -> Disk:
        return Disk(**data)


class ClientErrorSchema(Schema):
    code = fields.String(required=True)
    description = fields.String(required=True)
