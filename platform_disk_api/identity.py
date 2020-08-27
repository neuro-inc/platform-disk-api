import logging
from dataclasses import dataclass, field

from aiohttp.web import HTTPUnauthorized, Request
from aiohttp_security.api import AUTZ_KEY, IDENTITY_KEY


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Identity:
    name: str
    token: str = field(repr=False)


async def untrusted_user(request: Request) -> Identity:
    """Return a non-authorized `User` object based on the token in the request.
    The primary use case is to not perform an extra HTTP request just to
    retrieve the minimal information about the user.
    """
    identity = await _get_identity(request)

    autz_policy = request.config_dict.get(AUTZ_KEY)
    name = autz_policy.get_user_name_from_identity(identity)
    if name is None:
        raise HTTPUnauthorized()

    return Identity(name=name, token=identity)


async def _get_identity(request: Request) -> str:
    identity_policy = request.config_dict.get(IDENTITY_KEY)
    identity = await identity_policy.identify(request)
    if identity is None:
        raise HTTPUnauthorized()
    return identity
