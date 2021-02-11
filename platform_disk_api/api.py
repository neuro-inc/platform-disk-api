import logging
from contextlib import AsyncExitStack, asynccontextmanager
from typing import AsyncIterator, Awaitable, Callable

import aiohttp
import aiohttp.web
import aiohttp_cors
import pkg_resources
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
from aiohttp_apispec import docs, request_schema, response_schema, setup_aiohttp_apispec
from aiohttp_security import check_authorized
from neuro_auth_client import (
    AuthClient,
    ClientSubTreeViewRoot,
    Permission,
    User,
    check_permissions,
)
from neuro_auth_client.security import AuthScheme, setup_security
from platform_logging import init_logging

from .config import Config, CORSConfig, KubeConfig, PlatformAuthConfig
from .config_factory import EnvironConfigFactory
from .identity import untrusted_user
from .kube_client import KubeClient
from .schema import ClientErrorSchema, DiskRequestSchema, DiskSchema
from .service import Disk, DiskNotFound, Service


logger = logging.getLogger(__name__)


class ApiHandler:
    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes(
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

    async def _get_untrusted_user(self, request: Request) -> User:
        identity = await untrusted_user(request)
        return User(name=identity.name)

    @property
    def _disk_cluster_uri(self) -> str:
        return f"disk://{self._config.cluster_name}"

    def _get_user_disk_uri(self, user: User) -> str:
        return f"{self._disk_cluster_uri}/{user.name}"

    def _get_user_disks_write_perm(self, user: User) -> Permission:
        return Permission(self._get_user_disk_uri(user), "write")

    def _get_disk_read_perm(self, disk: Disk) -> Permission:
        return Permission(f"{self._disk_cluster_uri}/{disk.owner}/{disk.id}", "read")

    def _get_disk_write_perm(self, disk: Disk) -> Permission:
        return Permission(f"{self._disk_cluster_uri}/{disk.owner}/{disk.id}", "write")

    async def _get_user_used_storage(self, user: User) -> int:
        storage_used = 0
        for disk in await self._service.get_all_disks():
            if disk.owner == user.name:
                storage_used += disk.storage
        return storage_used

    async def _resolve_disk(self, request: Request) -> Disk:
        id_or_name = request.match_info["disk_id_or_name"]
        try:
            disk = await self._service.get_disk(id_or_name)
        except DiskNotFound:
            user = await self._get_untrusted_user(request)
            try:
                disk = await self._service.get_disk_by_name(id_or_name, user.name)
            except DiskNotFound:
                raise HTTPNotFound
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
        await check_permissions(request, [self._get_user_disks_write_perm(user)])
        payload = await request.json()
        disk_request = DiskRequestSchema().load(payload)
        if (
            self._config.disk.storage_limit_per_user
            < await self._get_user_used_storage(user) + disk_request.storage
        ):
            return json_response(
                {
                    "code": "over_limit",
                    "description": "User exceeded storage size limit",
                },
                status=HTTPForbidden.status_code,
            )
        disk = await self._service.create_disk(disk_request, user.name)
        resp_payload = DiskSchema().dump(disk)
        return json_response(resp_payload, status=HTTPCreated.status_code)

    def _check_disk_read_perm(self, disk: Disk, tree: ClientSubTreeViewRoot) -> bool:
        node = tree.sub_tree
        if node.can_read():
            return True
        try:
            user_node = node.children[disk.owner]
            if user_node.can_read():
                return True
            disk_node = user_node.children[disk.id]
            return disk_node.can_read()
        except KeyError:
            return False

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
        disks = [
            disk
            for disk in await self._service.get_all_disks()
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
    async def handle_delete_disk(self, request: Request) -> Response:
        disk = await self._resolve_disk(request)
        await check_permissions(request, [self._get_disk_write_perm(disk)])
        await self._service.remove_disk(disk.id)
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


package_version = pkg_resources.get_distribution("platform-disk-api").version


async def add_version_to_header(request: Request, response: StreamResponse) -> None:
    response.headers["X-Service-Version"] = f"platform-disk-api/{package_version}"


async def create_api_v1_app() -> aiohttp.web.Application:
    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)
    return api_v1_app


async def create_disk_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = DiskApiHandler(app, config)
    handler.register(app)
    return app


@asynccontextmanager
async def create_auth_client(config: PlatformAuthConfig) -> AsyncIterator[AuthClient]:
    async with AuthClient(config.url, config.token) as client:
        yield client


@asynccontextmanager
async def create_kube_client(config: KubeConfig) -> AsyncIterator[KubeClient]:
    client = KubeClient(
        base_url=config.endpoint_url,
        namespace=config.namespace,
        cert_authority_path=config.cert_authority_path,
        cert_authority_data_pem=config.cert_authority_data_pem,
        auth_type=config.auth_type,
        auth_cert_path=config.auth_cert_path,
        auth_cert_key_path=config.auth_cert_key_path,
        token=config.token,
        token_path=None,  # TODO (A Yushkovskiy) add support for token_path or drop
        conn_timeout_s=config.client_conn_timeout_s,
        read_timeout_s=config.client_read_timeout_s,
        watch_timeout_s=config.client_watch_timeout_s,
        conn_pool_size=config.client_conn_pool_size,
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


async def create_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app["config"] = config

    async def _init_app(app: aiohttp.web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:
            logger.info("Initializing Auth client")
            auth_client = await exit_stack.enter_async_context(
                create_auth_client(config.platform_auth)
            )

            await setup_security(
                app=app, auth_client=auth_client, auth_scheme=AuthScheme.BEARER
            )
            app["disk_app"]["auth_client"] = auth_client

            logger.info("Initializing Kubernetes client")
            kube_client = await exit_stack.enter_async_context(
                create_kube_client(config.kube)
            )

            logger.info("Initializing Service")
            app["disk_app"]["service"] = Service(
                kube_client, config.disk.k8s_storage_class
            )

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = await create_api_v1_app()
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

    app.on_response_prepare.append(add_version_to_header)

    return app


def main() -> None:  # pragma: no coverage
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)
    aiohttp.web.run_app(
        create_app(config), host=config.server.host, port=config.server.port
    )
