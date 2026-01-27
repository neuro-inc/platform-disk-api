import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack

import aiohttp
import aiohttp.web
import aiohttp_cors
import uvloop
from aiohttp.web import (
    HTTPBadRequest,
    HTTPInternalServerError,
    Request,
    Response,
    StreamResponse,
    json_response,
    middleware,
)
from aiohttp.web_exceptions import (
    HTTPConflict,
    HTTPCreated,
    HTTPForbidden,
    HTTPNoContent,
    HTTPNotFound,
    HTTPOk,
)
from aiohttp.web_urldispatcher import AbstractRoute
from aiohttp_apispec import (
    docs,
    querystring_schema,
    request_schema,
    response_schema,
    setup_aiohttp_apispec,
)
from aiohttp_security import check_authorized
from apolo_kube_client import KubeClientSelector
from marshmallow import Schema, fields
from neuro_admin_client.auth_client import AuthClient, Permission
from neuro_admin_client.security import AuthScheme, check_permissions, setup_security
from neuro_logging import init_logging, setup_sentry

from platform_disk_api import __version__
from platform_disk_api.platform_deleter import ProjectDeleter

from .config import Config, CORSConfig
from .config_factory import EnvironConfigFactory
from .schema import ClientErrorSchema, DiskRequestSchema, DiskSchema
from .service import (
    Disk,
    DiskAlreadyInUse,
    DiskConflict,
    DiskNameUsed,
    DiskNotFound,
    DiskRequest,
    DiskServiceError,
    Service,
)


logger = logging.getLogger(__name__)


ERROR_TO_HTTP_CODE = {
    DiskServiceError: HTTPNotFound.status_code,
    DiskNotFound: HTTPNotFound.status_code,
    DiskConflict: HTTPConflict.status_code,
    DiskNameUsed: HTTPConflict.status_code,
    DiskAlreadyInUse: HTTPConflict.status_code,
}


class ApiHandler:
    def register(self, app: aiohttp.web.Application) -> list[AbstractRoute]:
        return app.add_routes(
            [
                aiohttp.web.get("/ping", self.handle_ping),
                aiohttp.web.get("/secured-ping", self.handle_secured_ping),
            ]
        )

    @docs(
        tags=["ping"],
        summary="Health ping endpoint",
        responses={200: {"description": "Pong"}},
    )
    async def handle_ping(self, request: Request) -> Response:
        return Response(text="Pong")

    @docs(
        tags=["ping"],
        summary="Health ping endpoint with auth check",
        responses={200: {"description": "Secured Pong"}},
    )
    async def handle_secured_ping(self, request: Request) -> Response:
        await check_authorized(request)
        return Response(text="Secured Pong")


class DiskApiHandler:
    def __init__(self, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        # TODO: add routes to handler
        app.add_routes([aiohttp.web.post("", self.handle_create_disk)])
        app.add_routes([aiohttp.web.get("", self.handle_list_disks)])
        app.add_routes([aiohttp.web.get("/{disk_id_or_name}", self.handle_get_disk)])
        app.add_routes(
            [aiohttp.web.delete("/{disk_id_or_name}", self.handle_delete_disk)]
        )

    @property
    def _service(self) -> Service:
        return self._app["service"]

    @property
    def _auth_client(self) -> AuthClient:
        return self._app["auth_client"]

    @property
    def _disk_cluster_uri(self) -> str:
        return f"disk://{self._config.cluster_name}"

    def _get_org_disks_uri(self, org_name: str) -> str:
        return f"{self._disk_cluster_uri}/{org_name}"

    def _get_user_disk_or_project_uri(
        self, username: str, org_name: str, project_name: str
    ) -> str:
        base = self._get_org_disks_uri(org_name)
        if username == project_name:
            return f"{base}/{username}"
        return f"{base}/{project_name}"

    def _get_disk_or_project_uri(self, disk: Disk) -> str:
        base = self._get_org_disks_uri(disk.org_name)
        if disk.owner == disk.project_name:
            return f"{base}/{disk.owner}/{disk.id}"
        return f"{base}/{disk.project_name}"

    def _get_disk_read_perm(self, disk: Disk) -> Permission:
        return Permission(self._get_disk_or_project_uri(disk), "read")

    def _get_disk_write_perm(self, disk: Disk) -> Permission:
        return Permission(self._get_disk_or_project_uri(disk), "write")

    def _get_disks_write_perm(
        self, username: str, org_name: str, project_name: str
    ) -> Permission:
        return Permission(
            self._get_user_disk_or_project_uri(username, org_name, project_name),
            "write",
        )

    def _get_disks_read_perm(self, org_name: str, project_name: str) -> Permission:
        """Permission to read all disks in a project."""
        return Permission(f"{self._get_org_disks_uri(org_name)}/{project_name}", "read")

    async def _get_project_used_storage(
        self,
        disk_request: DiskRequest,
    ) -> int:
        return sum(
            [
                int(d.storage)
                for d in await self._service.get_project_disks(
                    disk_request.org_name, disk_request.project_name
                )
            ]
        )

    async def _resolve_disk_from_request(self, request: Request) -> Disk:
        id_or_name = request.match_info["disk_id_or_name"]
        org_name = request.query["org_name"]
        project_name = request.query["project_name"]
        try:
            return await self._service.resolve_disk(
                disk_id_or_name=id_or_name,
                org_name=org_name,
                project_name=project_name,
            )
        except DiskNotFound:
            exc_txt = f"Disk {id_or_name} not found"
            raise HTTPNotFound(text=exc_txt) from None

    @docs(
        tags=["disks"],
        summary="Create new Disk object",
        responses={
            HTTPCreated.status_code: {
                "description": "Disk created",
                "schema": DiskSchema(),
            },
            HTTPForbidden.status_code: {
                "description": "Disk creation was forbidden",
                "schema": ClientErrorSchema(),
            },
        },
    )
    @request_schema(DiskRequestSchema())
    async def handle_create_disk(self, request: Request) -> Response:
        username = await check_authorized(request)
        payload = await request.json()

        disk_request = DiskRequestSchema().load(payload)
        await check_permissions(
            request,
            [
                self._get_disks_write_perm(
                    username, disk_request.org_name, disk_request.project_name
                )
            ],
        )

        project_used_storage = await self._get_project_used_storage(disk_request)

        if self._config.disk.storage_limit_per_project < (
            project_used_storage + disk_request.storage
        ):
            limit_gb = self._config.disk.storage_limit_per_project / 2**30
            return json_response(
                {
                    "code": "over_limit",
                    "description": f"Project exceeded storage size limit {limit_gb} GB",
                },
                status=HTTPForbidden.status_code,
            )
        disk = await self._service.create_disk(disk_request, username)
        resp_payload = DiskSchema().dump(disk)
        return json_response(resp_payload, status=HTTPCreated.status_code)

    @docs(
        tags=["disks"],
        summary="Get Disk objects by id or name",
        responses={
            HTTPOk.status_code: {"description": "Disk found", "schema": DiskSchema()},
            HTTPNotFound.status_code: {
                "description": "Was unable to found disk with such id"
            },
        },
    )
    @response_schema(DiskSchema(), 200)
    @querystring_schema(
        Schema.from_dict(
            {
                "owner": fields.String(required=False),
                "org_name": fields.String(required=False),
                "project_name": fields.String(required=True),
            }
        )
    )
    async def handle_get_disk(self, request: Request) -> Response:
        disk = await self._resolve_disk_from_request(request)
        await check_permissions(request, [self._get_disk_read_perm(disk)])
        resp_payload = DiskSchema().dump(disk)
        return json_response(resp_payload, status=HTTPOk.status_code)

    @docs(tags=["disks"], summary="List all users Disk objects")
    @response_schema(DiskSchema(many=True), 200)
    async def handle_list_disks(self, request: Request) -> Response:
        await check_authorized(request)
        org_name = request.query["org_name"]
        project_name = request.query["project_name"]
        await check_permissions(
            request,
            [self._get_disks_read_perm(org_name, project_name)],
        )
        disks = await self._service.get_project_disks(
            org_name=org_name,
            project_name=project_name,
        )
        resp_payload = DiskSchema(many=True).dump(disks)
        return json_response(resp_payload, status=HTTPOk.status_code)

    @docs(
        tags=["disks"],
        summary="Delete Disk object by id",
        responses={
            HTTPNoContent.status_code: {"description": "Disk was deleted"},
            HTTPNotFound.status_code: {
                "description": "Was unable to found disk with such id"
            },
        },
    )
    @querystring_schema(
        Schema.from_dict(
            {
                "owner": fields.String(required=False),
                "org_name": fields.String(required=False),
                "project_name": fields.String(required=True),
            }
        )
    )
    async def handle_delete_disk(self, request: Request) -> Response:
        disk = await self._resolve_disk_from_request(request)
        await check_permissions(request, [self._get_disk_write_perm(disk)])
        await self._service.remove_disk(disk)
        raise HTTPNoContent


@middleware
async def handle_exceptions(
    request: Request, handler: Callable[[Request], Awaitable[StreamResponse]]
) -> StreamResponse:
    try:
        return await handler(request)
    except DiskServiceError as e:
        status_code = ERROR_TO_HTTP_CODE.get(
            type(e), HTTPInternalServerError.status_code
        )
        payload = {
            "code": type(e).__name__,
            "description": str(e),
        }
        return json_response(payload, status=status_code)
    except ValueError as e:
        payload = {"error": str(e)}
        return json_response(payload, status=HTTPBadRequest.status_code)
    except RuntimeError as e:
        # check_permissions raises RuntimeError when user lacks permissions
        # (wraps 403 Forbidden from platform-admin)
        error_str = str(e)
        if "403" in error_str or "Forbidden" in error_str:
            raise aiohttp.web.HTTPForbidden() from None
        raise
    except aiohttp.web.HTTPException:
        raise
    except Exception as e:
        msg_str = f"Unexpected exception: {str(e)}. Path with query: {request.path_qs}."
        logging.exception(msg_str)
        payload = {"error": msg_str}
        return json_response(payload, status=HTTPInternalServerError.status_code)


async def add_version_to_header(request: Request, response: StreamResponse) -> None:
    response.headers["X-Service-Version"] = f"platform-disk-api/{__version__}"


async def create_disk_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = DiskApiHandler(app, config)
    handler.register(app)
    return app


def _setup_cors(app: aiohttp.web.Application, config: CORSConfig) -> None:
    if not config.allowed_origins:
        return

    logger.info("Setting up CORS with allowed origins: %s", config.allowed_origins)
    default_options = aiohttp_cors.ResourceOptions(
        allow_credentials=True,
        expose_headers="*",
        allow_headers="*",
    )
    cors = aiohttp_cors.setup(
        app, defaults=dict.fromkeys(config.allowed_origins, default_options)
    )
    for route in app.router.routes():
        logger.debug("Setting up CORS for %s", route)
        cors.add(route)


async def create_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app["config"] = config

    async def _init_app(app: aiohttp.web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:
            logger.info("Initializing Auth client")
            auth_client = await exit_stack.enter_async_context(
                AuthClient(
                    config.platform_auth.url,
                    config.platform_auth.token,
                )
            )

            await setup_security(
                app=app, auth_client=auth_client, auth_scheme=AuthScheme.BEARER
            )
            app["disk_app"]["auth_client"] = auth_client

            logger.info("Initializing Kubernetes client")
            kube_client_selector = await exit_stack.enter_async_context(
                KubeClientSelector(config=config.kube)
            )

            logger.info("Initializing Service")
            disk_service = Service(kube_client_selector, config.disk.k8s_storage_class)
            app["disk_app"]["service"] = disk_service

            await exit_stack.enter_async_context(
                ProjectDeleter(disk_service, config.events)
            )

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)
    app["api_v1_app"] = api_v1_app

    disk_app = await create_disk_app(config)
    app["disk_app"] = disk_app
    api_v1_app.add_subapp("/disk", disk_app)

    app.add_subapp("/api/v1", api_v1_app)

    _setup_cors(app, config.cors)
    if config.enable_docs:
        prefix = "/api/docs/v1/disk"
        setup_aiohttp_apispec(
            app=app,
            title="Disks documentation",
            version="v1",
            url=f"{prefix}/swagger.json",
            static_path=f"{prefix}/static",
            swagger_path=f"{prefix}/ui",
            security=[{"jwt": []}],
            securityDefinitions={
                "jwt": {"type": "apiKey", "name": "Authorization", "in": "header"},
            },
        )

    @docs(
        tags=["ping"],
        summary="Health ping endpoint",
        responses={200: {"description": "Pong"}},
    )
    async def ping(request: Request) -> Response:
        return Response(text="Pong")

    app.router.add_get("/ping", ping)

    app.on_response_prepare.append(add_version_to_header)

    return app


def main() -> None:  # pragma: no coverage
    init_logging(health_check_url_path="/ping")
    setup_sentry(health_check_url_path="/ping")
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)
    loop = uvloop.new_event_loop()
    aiohttp.web.run_app(
        create_app(config),
        host=config.server.host,
        port=config.server.port,
        handler_cancellation=True,
        loop=loop,
    )
