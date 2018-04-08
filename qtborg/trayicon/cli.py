########################################################################
# File name: cli.py
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

    Qt.QApplication.setAttribute(Qt.Qt.AA_UseHighDpiPixmaps)
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
