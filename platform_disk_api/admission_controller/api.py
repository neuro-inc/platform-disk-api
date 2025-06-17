import logging

import aiohttp
import aiohttp.web

from ..config import Config

LOGGER = logging.getLogger(__name__)


class AdmissionControllerHandler:
    def __init__(self, app: aiohttp.web.Application) -> None:
        self._app = app

    def register(self) -> None:
        self._app.add_routes(
            [
                aiohttp.web.get("/ping", self.handle_ping),
                aiohttp.web.post("/mutate", self.handle_post_mutate),
            ]
        )

    async def handle_ping(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.Response(text="ok")

    async def handle_post_mutate(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        return aiohttp.web.json_response(
            {
                "apiVersion": "admission.k8s.io/v1",
                "kind": "AdmissionReview",
                "status": {"allowed": True},
            }
        )


async def create_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()

    AdmissionControllerHandler(app=app).register()

    return app
