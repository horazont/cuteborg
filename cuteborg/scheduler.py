import argparse
import asyncio
import enum
import logging
import pathlib
import pprint
import signal
import struct
import sys

import xdg.BaseDirectory

from . import config


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

    WHEN_NEXT = b"WIN?"  # When Is Next?

    ERROR = b"ERR!"  # ERRor

    OKAY = b"OKAY"  # OKAY!

    EXTENDED_REQUEST = b"EXT:"  # EXTended


class ControlProtocol(asyncio.Protocol):
    def __init__(self, logger, scheduler):
        super().__init__()

        self.logger = logger
        self.scheduler = scheduler

        self._buffer = []
        self._buffer_len = 0

        self._ext_size = 0

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

            if actual_cmd == ToplevelCommand.EXTENDED_REQUEST:
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

        elif state == ProtocolState.RECV_EXT_DATA:
            if self._buffer_len < self._ext_size:
                return state

            raw_data = self._squash_buffer(self._ext_size)

            self._process_command(
                ToplevelCommand.EXTENDED_REQUEST,
                raw_data,
            )

            return ProtocolState.TOPLEVEL

    def _error(self):
        if hasattr(self, "_transport"):
            self._transport.writelines([
                ToplevelCommand.ERROR.value,
                b"\n",
            ])
            self._transport.close()

    def _process_command(self, cmd, ext_data=None):
        self.logger.debug("%r (ext_data=%r)", cmd, ext_data)
        if cmd == ToplevelCommand.PING:
            self._transport.writelines([
                ToplevelCommand.PONG.value,
                b"\n"
            ])
        else:
            self.logger.warning(
                "no way to handle %r",
                cmd,
            )
            self._error()

    def connection_made(self, transport):
        self._transport = transport

    def connection_lost(self, exc):
        del self._transport

    def data_received(self, data):
        self.logger.debug("recv: %r", data)

        self._buffer.append(data)
        self._buffer_len += len(data)

        try:
            new_state = self._process(self._state)
            try:
                while new_state != self._state:
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


class Scheduler:
    def __init__(self, loop, socket_path):
        super().__init__()
        self.loop = loop
        self.logger = logging.getLogger("cuteborg.scheduler")
        self.socket_path = socket_path

        self._last_runs = {}

        self._reload_and_reschedule()
        self._rls_pending = False

    def _create_protocol(self):
        self.logger.debug("new connection")
        proto = ControlProtocol(
            self.logger.getChild("control"),
            self,
        )
        return proto

    def _reload_and_reschedule(self):
        self._rls_pending = False
        self.logger.debug("reloading configuration")
        self.config = config.Config.from_raw(config.load_config())

        self.logger.debug(
            "found %d repositories with %d jobs in total",
            len(self.config.repositories),
            len(self.config.jobs),
        )

        for name, job in self.config.jobs.items():
            self.logger.debug("scheduling job %r", name)

            last_run = self._last_runs.get(name)

            if last_run is None:
                self.logger.debug(
                    "job was not run yet",
                )
            else:
                self.logger.debug(
                    "previous run was at %r",
                    last_run,
                )

    def _scheduled_reload_and_reschedule(self):
        if not self._rls_pending:
            return
        self._reload_and_reschedule()

    def _trigger_reload_and_reschedule(self):
        if self._rls_pending:
            return

        self._rls_pending = True
        self.loop.call_later(
            1,
            self._scheduled_reload_and_reschedule
        )

    @asyncio.coroutine
    def main(self):
        try:
            self.socket_path.parent.mkdir(mode=0o700, parents=True)
        except FileExistsError:
            if not self.socket_path.parent.is_dir():
                raise

        if self.socket_path.exists():
            self.logger.error(
                "socket %s exists! please remove it.",
                self.socket_path,
            )
            return 1

        stop_signal = asyncio.Event(loop=self.loop)
        self.loop.add_signal_handler(signal.SIGINT, stop_signal.set)
        self.loop.add_signal_handler(signal.SIGTERM, stop_signal.set)

        server = yield from self.loop.create_unix_server(
            self._create_protocol,
            str(self.socket_path),
        )

        try:
            yield from stop_signal.wait()
        finally:
            server.close()
            stop_signal.clear()
            done, pending = yield from asyncio.wait(
                [
                    server.wait_closed(),
                    stop_signal.wait(),
                ],
                return_when=asyncio.FIRST_COMPLETED)

            for fut in pending:
                fut.cancel()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-v",
        dest="verbosity",
        action="count",
        default=0,
        help="Increase verbosity (up to -vvv)"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level={
            0: logging.ERROR,
            1: logging.WARNING,
            2: logging.INFO,
        }.get(args.verbosity, logging.DEBUG),
    )

    socket_path = pathlib.Path(
        xdg.BaseDirectory.get_runtime_dir(strict=False)
    ) / "cuteborg" / "scheduler.sock"

    loop = asyncio.get_event_loop()
    try:
        scheduler = Scheduler(loop, socket_path)
        sys.exit(loop.run_until_complete(scheduler.main()))
    finally:
        try:
            socket_path.unlink()
        except OSError:
            pass
        loop.close()
