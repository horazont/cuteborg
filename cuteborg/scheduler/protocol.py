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

    STATUS = b"WIU?"  # What Is Up?

    ERROR = b"ERR!"  # ERRor

    OKAY = b"OKAY"  # OKAY!

    EXTENDED_REQUEST = b"EXT:"  # EXTended


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

            data = toml.loads(
                self._squash_buffer(self._ext_size).decode("utf-8")
            )

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


class ClientProtocolEndpoint:
    protocol = None
    response_fut = None

    def close(self):
        if self.protocol is not None:
            self.protocol.close()

    @asyncio.coroutine
    def request(self, cmd, extra_data=None, timeout=None):
        if self.response_fut is not None:
            raise RuntimeError("there is a command pending")

        fut = asyncio.Future()
        self.response_fut = fut
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

    def handle_request(self, protocol, cmd, extra_data):
        assert self.protocol == protocol
        if self.response_fut is None:
            return protocol.ToplevelCommand.ERROR, None

        self.response_fut.set_result((cmd, extra_data))
        self.response_fut = None


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
