import asyncio
import logging

from . import protocol


@asyncio.coroutine
def get_status(loop, socket_path):
    logger = logging.getLogger("cuteborg.scheduler")

    try:
        endpoint = yield from asyncio.wait_for(
            protocol.test_and_get_socket(
                loop,
                logger,
                socket_path,
            ),
            timeout=5
        )
    except (asyncio.TimeoutError, FileNotFoundError, ConnectionError) as exc:
        msg = str(exc)
        if isinstance(exc, asyncio.TimeoutError):
            msg = "timeout"
        logger.error("failed to connect to scheduler at %r: %s",
                     str(socket_path),
                     msg)
        raise RuntimeError(
            "failed to connect to scheduler"
        )

    response, data = yield from endpoint.request(
        protocol.ToplevelCommand.STATUS
    )

    return data
