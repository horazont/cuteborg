import argparse
import asyncio
import logging
import pathlib
import sys

import xdg.BaseDirectory

import PyQt5.Qt as Qt

import quamash

from . import Main


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-v",
        dest="verbosity",
        action="count",
        default=0,
        help="Increase verbosity (up to -vvv)"
    )

    parser.add_argument(
        "--socket-path",
        default=None,
        type=pathlib.Path,
        help="Override the socket path "
        "(defaults to a user-specific socket)"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level={
            0: logging.ERROR,
            1: logging.WARNING,
            2: logging.INFO,
        }.get(args.verbosity, logging.DEBUG),
    )

    if args.socket_path is None:
        args.socket_path = pathlib.Path(
            xdg.BaseDirectory.get_runtime_dir(strict=False)
        ) / "cuteborg" / "scheduler.sock"

    logger = logging.getLogger("qtborg.trayicon")

    logging.getLogger("quamash").setLevel(logging.INFO)

    app = Qt.QApplication(sys.argv[:1])
    app.setQuitOnLastWindowClosed(False)
    asyncio.set_event_loop(quamash.QEventLoop(app=app))

    loop = asyncio.get_event_loop()
    try:
        iconapp = Main(loop, logger, args.socket_path)
        rc = loop.run_until_complete(iconapp.run())
        del iconapp
        del app
        import gc
        gc.collect()
        sys.exit(rc)
    finally:
        loop.close()
