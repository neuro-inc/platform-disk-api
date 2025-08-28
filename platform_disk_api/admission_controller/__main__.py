import logging
import ssl

import aiohttp
import uvloop
from neuro_logging import init_logging, setup_sentry

from ..config import Config
from ..config_factory import EnvironConfigFactory
from .api import create_app


LOGGER = logging.getLogger(__name__)


async def create_ssl_context(config: Config) -> ssl.SSLContext | None:
    context = None
    if config.server.tls_cert_path and config.server.tls_key_path:
        LOGGER.info("Loading SSL cert chain from http server config")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(
            certfile=config.server.tls_cert_path,
            keyfile=config.server.tls_key_path,
        )
        LOGGER.info("Loaded SSL cert chain")
    return context


def main() -> None:
    init_logging(health_check_url_path="/ping")
    setup_sentry()

    config = EnvironConfigFactory().create()
    LOGGER.info("Loaded config: %s", config)

    loop = uvloop.new_event_loop()
    ssl_context = loop.run_until_complete(create_ssl_context(config))
    app = loop.run_until_complete(create_app(config))
    aiohttp.web.run_app(
        app,
        host=config.server.host,
        port=config.server.port,
        ssl_context=ssl_context,
        handler_cancellation=True,
        loop=loop,
    )


if __name__ == "__main__":
    main()
