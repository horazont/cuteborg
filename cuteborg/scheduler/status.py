########################################################################
# File name: status.py
# This file is part of: cuteborg
#
# LICENSE
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
########################################################################
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
