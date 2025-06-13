import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Optional

import aiohttp
import aiohttp.web
import aiohttp_cors
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
from apolo_kube_client.apolo import NO_ORG, normalize_name
from apolo_kube_client.config import KubeConfig
from marshmallow import Schema, fields
from neuro_auth_client import (
    AuthClient,
    ClientSubTreeViewRoot,
    Permission,
    User,
    check_permissions,
)
from neuro_auth_client.security import AuthScheme, setup_security
from neuro_logging import (
    init_logging,
    make_sentry_trace_config,
    make_zipkin_trace_config,
    notrace,
    setup_sentry,
    setup_zipkin,
    setup_zipkin_tracer,
)

from .config import Config, CORSConfig
from .config_factory import EnvironConfigFactory
from .identity import untrusted_user
from .kube_client import KubeClient
from .schema import ClientErrorSchema, DiskRequestSchema, DiskSchema
from .service import Disk, DiskNotFound, DiskRequest, Service, is_no_org
from platform_disk_api import __version__

logger = logging.getLogger(__name__)


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
    @notrace
    async def handle_ping(self, request: Request) -> Response:
        return Response(text="Pong")

    @docs(
        tags=["ping"],
        summary="Health ping endpoint with auth check",
        responses={200: {"description": "Secured Pong"}},
    )
    @notrace
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

    async def _get_untrusted_user(self, request: Request) -> User:
        identity = await untrusted_user(request)
        return User(name=identity.name)

    @property
    def _disk_cluster_uri(self) -> str:
        return f"disk://{self._config.cluster_name}"

    def _get_org_disks_uri(self, org_name: str) -> str:
        return f"{self._disk_cluster_uri}/{org_name}"

    def _get_user_disk_or_project_uri(
        self, user: User, org_name: str, project_name: str
    ) -> str:
        if not is_no_org(org_name):
            base = self._get_org_disks_uri(org_name)
        else:
            base = self._disk_cluster_uri
        if user.name == project_name:
            return f"{base}/{user.name}"
        return f"{base}/{project_name}"

    def _get_disk_or_project_uri(self, disk: Disk) -> str:
        if disk.has_org:
            base = self._get_org_disks_uri(disk.org_name)
        else:
            base = self._disk_cluster_uri
        if disk.owner == disk.project_name:
            return f"{base}/{disk.owner}/{disk.id}"
        return f"{base}/{disk.project_name}"

    def _get_disk_read_perm(self, disk: Disk) -> Permission:
        return Permission(self._get_disk_or_project_uri(disk), "read")

    def _get_disk_write_perm(self, disk: Disk) -> Permission:
        return Permission(self._get_disk_or_project_uri(disk), "write")

    def _get_disks_write_perm(
        self, user: User, org_name: str, project_name: str
    ) -> Permission:
        return Permission(
            self._get_user_disk_or_project_uri(user, org_name, project_name),
            "write",
        )

    async def _get_project_used_storage(
        self,
        disk_request: DiskRequest,
    ) -> int:
        return sum(
            [
                d.storage
                for d in await self._service.get_all_disks(
                    disk_request.org_name, disk_request.project_name
                )
            ]
        )

    async def _resolve_disk(self, request: Request) -> Disk:
        id_or_name = request.match_info["disk_id_or_name"]
        org_name = request.query.get("org_name") or normalize_name(NO_ORG)
        project_name = request.query["project_name"]
        try:
            disk = await self._service.get_disk(org_name, project_name, id_or_name)
        except DiskNotFound:
            try:
                disk = await self._service.get_disk_by_name(
                    id_or_name, org_name, project_name
                )
            except DiskNotFound:
                raise HTTPNotFound(text=f"Disk {id_or_name} not found")
        return disk

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
        user = await self._get_untrusted_user(request)
        payload = await request.json()

        disk_request = DiskRequestSchema().load(payload)
        await check_permissions(
            request,
            [
                self._get_disks_write_perm(
                    user, disk_request.org_name, disk_request.project_name
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
        disk = await self._service.create_disk(disk_request, user.name)
        resp_payload = DiskSchema().dump(disk)
        return json_response(resp_payload, status=HTTPCreated.status_code)

    def _check_disk_read_perm(self, disk: Disk, tree: ClientSubTreeViewRoot) -> bool:
        return tree.allows(self._get_disk_read_perm(disk))

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
        disk = await self._resolve_disk(request)
        await check_permissions(request, [self._get_disk_read_perm(disk)])
        resp_payload = DiskSchema().dump(disk)
        return json_response(resp_payload, status=HTTPOk.status_code)

    @docs(tags=["disks"], summary="List all users Disk objects")
    @response_schema(DiskSchema(many=True), 200)
    async def handle_list_disks(self, request: Request) -> Response:
        username = await check_authorized(request)
        tree = await self._auth_client.get_permissions_tree(
            username, self._disk_cluster_uri
        )
        org_name = request.query.get("org_name") or normalize_name(NO_ORG)
        project_name = request.query["project_name"]
        disks = [
            disk
            for disk in await self._service.get_all_disks(
                org_name=org_name, project_name=project_name
            )
            if self._check_disk_read_perm(disk, tree)
        ]
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
        disk = await self._resolve_disk(request)
        await check_permissions(request, [self._get_disk_write_perm(disk)])
        await self._service.remove_disk(disk)
        raise HTTPNoContent


@middleware
async def handle_exceptions(
    request: Request, handler: Callable[[Request], Awaitable[StreamResponse]]
) -> StreamResponse:
    try:
        return await handler(request)
    except ValueError as e:
        payload = {"error": str(e)}
        return json_response(payload, status=HTTPBadRequest.status_code)
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


@asynccontextmanager
async def create_kube_client(
    config: KubeConfig, trace_configs: Optional[list[aiohttp.TraceConfig]] = None
) -> AsyncIterator[KubeClient]:
    client = KubeClient(
        base_url=config.endpoint_url,
        namespace=config.namespace,
        cert_authority_path=config.cert_authority_path,
        cert_authority_data_pem=config.cert_authority_data_pem,
        auth_type=config.auth_type,
        auth_cert_path=config.auth_cert_path,
        auth_cert_key_path=config.auth_cert_key_path,
        token=config.token,
        token_path=config.token_path,
        conn_timeout_s=config.client_conn_timeout_s,
        read_timeout_s=config.client_read_timeout_s,
        watch_timeout_s=config.client_watch_timeout_s,
        conn_pool_size=config.client_conn_pool_size,
        trace_configs=trace_configs,
    )
    try:
        await client.init()
        yield client
    finally:
        await client.close()


def _setup_cors(app: aiohttp.web.Application, config: CORSConfig) -> None:
    if not config.allowed_origins:
        return

    logger.info(f"Setting up CORS with allowed origins: {config.allowed_origins}")
    default_options = aiohttp_cors.ResourceOptions(
        allow_credentials=True,
        expose_headers="*",
        allow_headers="*",
    )
    cors = aiohttp_cors.setup(
        app, defaults={origin: default_options for origin in config.allowed_origins}
    )
    for route in app.router.routes():
        logger.debug(f"Setting up CORS for {route}")
        cors.add(route)


def make_tracing_trace_configs(config: Config) -> list[aiohttp.TraceConfig]:
    trace_configs = []

    if config.zipkin:
        trace_configs.append(make_zipkin_trace_config())

    if config.sentry:
        trace_configs.append(make_sentry_trace_config())

    return trace_configs


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
                    make_tracing_trace_configs(config),
                )
            )

            await setup_security(
                app=app, auth_client=auth_client, auth_scheme=AuthScheme.BEARER
            )
            app["disk_app"]["auth_client"] = auth_client

            logger.info("Initializing Kubernetes client")
            kube_client = await exit_stack.enter_async_context(
                create_kube_client(config.kube, make_tracing_trace_configs(config))
            )

            logger.info("Initializing Service")
            app["disk_app"]["service"] = Service(
                kube_client, config.disk.k8s_storage_class
            )

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    probes_routes = api_v1_handler.register(api_v1_app)
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
    @notrace
    async def ping(self, request: Request) -> Response:
        return Response(text="Pong")

    app.router.add_get("/ping", ping)

    app.on_response_prepare.append(add_version_to_header)

    if config.zipkin:
        setup_zipkin(app, skip_routes=probes_routes)

    return app


def setup_tracing(config: Config) -> None:
    if config.zipkin:
        setup_zipkin_tracer(
            config.zipkin.app_name,
            config.server.host,
            config.server.port,
            config.zipkin.url,
            config.zipkin.sample_rate,
        )

    if config.sentry:
        setup_sentry(
            config.sentry.dsn,
            app_name=config.sentry.app_name,
            cluster_name=config.sentry.cluster_name,
            sample_rate=config.sentry.sample_rate,
        )


def main() -> None:  # pragma: no coverage
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)
    setup_tracing(config)
    aiohttp.web.run_app(
        create_app(config), host=config.server.host, port=config.server.port
    )
