########################################################################
# File name: protocol.py
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
import enum
import struct
import toml


ExtHeader = struct.Struct(
    # version
    # size (16 bits)
    "<BH",
)


class ProtocolState(enum.Enum):
    TOPLEVEL = 0
    RECV_EXT_HEADER = 1
    RECV_EXT_BODY = 2


class ToplevelCommand(enum.Enum):
    PING = b"AYT?"  # Are You There?
    PONG = b"YSH!"  # Yes, i’m Still Here!

    RESCHEDULE = b"RLS!"  # ReLoad and reSchedule!

    POLL = b"KMP!"  # Keep Me Posted!

    STATUS = b"WIU?"  # What Is Up?

    ERROR = b"ERR!"  # ERRor

    OKAY = b"OKAY"  # OKAY!

    EXTENDED_REQUEST = b"EXT:"  # EXTended

    EXIT = b"EXIT"  # EXIT


class ControlProtocol(asyncio.Protocol):
    def __init__(self, logger, handler):
        super().__init__()

        self.logger = logger
        self.handler = handler

        self.breakdown_future = asyncio.Future()

        self._buffer = []
        self._buffer_len = 0

        self._ext_size = 0
        self._ext_cmd = None

        self._state = ProtocolState.TOPLEVEL

    def _squash_buffer(self, extract_bytes=0):
        buf = b"".join(self._buffer)
        assert len(buf) == self._buffer_len
        assert len(buf) >= extract_bytes
        result = buf[:extract_bytes]
        self._buffer = [buf[extract_bytes:]]
        self._buffer_len -= extract_bytes
        return result

    def _process(self, state):
        if state == ProtocolState.TOPLEVEL:
            if self._buffer_len < 5:
                return state

            cmd = self._squash_buffer(5)

            self.logger.debug("received command bytes: %r", cmd)

            if cmd[4] != b"\n"[0]:
                raise ValueError(
                    "commands must be terminated with a newline"
                )

            actual_cmd = ToplevelCommand(cmd[:4])

            self.logger.debug("received command: %r", actual_cmd)

            if actual_cmd.value.endswith(b":"):
                self.logger.debug("command is extended, "
                                  "waiting for more data")
                self._ext_cmd = actual_cmd
                return ProtocolState.RECV_EXT_HEADER

            self._process_command(actual_cmd)
            return state

        elif state == ProtocolState.RECV_EXT_HEADER:
            if self._buffer_len < ExtHeader.size:
                return state

            header = self._squash_buffer(ExtHeader.size)
            version, size = ExtHeader.unpack(header)

            if version != 1:
                raise ValueError("unknown version: {!r}".format(version))

            self._ext_size = size

            return ProtocolState.RECV_EXT_BODY

        elif state == ProtocolState.RECV_EXT_BODY:
            if self._buffer_len < self._ext_size:
                return state

            raw_data = self._squash_buffer(self._ext_size).decode("utf-8")
            try:
                data = toml.loads(raw_data)
            except:
                raise ValueError("failed to decode message: {!r}".format(
                    raw_data
                ))

            self._process_command(
                self._ext_cmd,
                data,
            )

            return ProtocolState.TOPLEVEL

    def _error(self):
        if hasattr(self, "_transport"):
            self.send(
                ToplevelCommand.ERROR,
            )
            self._transport.close()

    def _process_command(self, cmd, extra_data=None):
        self.logger.debug("%r (extra_data=%r)", cmd, extra_data)
        if cmd == ToplevelCommand.PING:
            self._transport.writelines([
                ToplevelCommand.PONG.value,
                b"\n"
            ])
        else:
            result = self.handler(self, cmd, extra_data)
            if result is not None:
                toplevel, extra_data = result
                self.send(toplevel, extra_data)

    def send(self, cmd, extra_data=None):
        if not hasattr(self, "_transport"):
            raise ConnectionError("disconnected")

        self.logger.debug("sending %r (extra_data=%r)", cmd, extra_data)

        parts = [
            cmd.value,
            b"\n",
        ]

        if extra_data is not None:
            if not cmd.value.endswith(b":"):
                raise ValueError(
                    "that command does not support extended data"
                )
            serialised = toml.dumps(extra_data).encode("utf-8")
            parts.extend([
                ExtHeader.pack(1, len(serialised)),
                serialised,
            ])

        elif cmd.value.endswith(b":") and extra_data is None:
            raise ValueError(
                "that command requires extended data"
            )

        self._transport.writelines(parts)

    def connection_made(self, transport):
        self._transport = transport

    def connection_lost(self, exc):
        if exc is not None:
            self.breakdown_future.set_exception(exc)
        else:
            self.breakdown_future.set_result(None)
        del self._transport

    def data_received(self, data):
        self.logger.debug("recv: %r", data)

        self._buffer.append(data)
        self._buffer_len += len(data)

        try:
            new_state = self._process(self._state)
            try:
                while new_state != self._state:
                    self._state = new_state
                    new_state = self._process(new_state)
            finally:
                self._state = new_state
        except:
            self.logger.error(
                "while processing input",
                exc_info=True,
            )
            self._error()

    def eof_received(self):
        pass

    def close(self):
        self._transport.close()


class ClientProtocolEndpoint:
    protocol = None

    def __init__(self):
        super().__init__()
        self.response_futs = []
        self.poll_future = None

    def close(self):
        if self.protocol is not None:
            self.protocol.close()

    @asyncio.coroutine
    def request(self, cmd, extra_data=None, timeout=None):
        if self.response_futs:
            raise RuntimeError("there is a command pending")
        if self.poll_future is not None:
            raise RuntimeError("there is a poll pending")

        fut = asyncio.Future()
        self.response_futs.append(fut)
        self.protocol.send(cmd, extra_data)

        done, pending = yield from asyncio.wait(
            [
                fut,
                self.protocol.breakdown_future,
            ],
            return_when=asyncio.FIRST_COMPLETED,
            timeout=timeout
        )

        if fut in done:
            return fut.result()

        self.protocol.breakdown_future.result()
        raise ConnectionError("unknown connection error")

    @asyncio.coroutine
    def poll_start(self):
        if self.response_futs:
            raise RuntimeError("there is a command pending")
        if self.poll_future is not None:
            raise RuntimeError("there is a poll pending")

        fut1 = asyncio.Future()
        self.poll_future = asyncio.Future()

        self.response_futs.append(fut1)
        self.response_futs.append(self.poll_future)

        self.protocol.send(ToplevelCommand.POLL)

        done, pending = yield from asyncio.wait(
            [
                fut1,
                self.protocol.breakdown_future,
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if fut1 in done:
            return fut1.result()

        self.protocol.breakdown_future.result()
        raise ConnectionError("unknown connection error")

    @asyncio.coroutine
    def poll_finalise(self):
        if self.poll_future is None:
            raise RuntimeError("there is no poll pending")

        done, pending = yield from asyncio.wait(
            [
                self.poll_future,
                self.protocol.breakdown_future,
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if self.poll_future in done:
            result = self.poll_future.result()
            self.poll_future = None
            return result

        self.poll_future = None

        self.protocol.breakdown_future.result()
        raise ConnectionError("unknown connection error")

    def handle_request(self, protocol, cmd, extra_data):
        assert self.protocol == protocol
        if not self.response_futs:
            return protocol.ToplevelCommand.ERROR, None

        fut = self.response_futs.pop(0)
        fut.set_result((cmd, extra_data))


@asyncio.coroutine
def test_and_get_socket(loop, logger, socket_path):
    endpoint = ClientProtocolEndpoint()

    def protocol_factory():
        nonlocal endpoint

        proto = ControlProtocol(
            logger,
            endpoint.handle_request,
        )
        endpoint.protocol = proto

        return proto

    try:
        _, protocol = yield from loop.create_unix_connection(
            protocol_factory,
            str(socket_path))
    except ConnectionRefusedError:
        # we can unlink the socket
        socket_path.unlink()
        raise FileNotFoundError()

    cmd, _ = yield from endpoint.request(ToplevelCommand.PING)
    if cmd != ToplevelCommand.PONG:
        raise ConnectionError("unexpected response to PING command")

    return endpoint
