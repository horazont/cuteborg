import argparse
import asyncio
import logging
import pathlib
import sys

import xdg.BaseDirectory

from .. import utils
from . import core, status


@asyncio.coroutine
def cmd_run(loop, args):
    scheduler = core.Scheduler(loop, args.socket_path)
    return (yield from scheduler.main())


@asyncio.coroutine
def cmd_status(loop, args):
    data = yield from status.get_status(loop, args.socket_path)

    headers = [
        "run at",
        "action",
        "args",
    ]
    rows = [
        (
            str(item["at"]),
            item["action"],
            " ".join(
                "{}={!r}".format(key, value)
                for key, value
                in sorted(item[item["action"]].items())
            )
        )
        for item in data["schedule"]
    ]

    print("upcoming jobs: {}".format(len(data["schedule"])))
    utils.print_table(headers, rows)

    print()
    print("running jobs: {}".format(len(data["running"])))

    headers = [
        "run_id",
        "action",
        "args",
        "progress",
    ]
    rows = [
        (
            item["run_id"],
            item["action"],
            str(item["args"]),
            " ".join(
                "{}={!r}".format(key, value)
                for key, value
                in sorted(item["progress"].items())
            ) if "progress" in item else ""
        )
        for item in data["running"]
    ]
    utils.print_table(headers, rows)


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

    subparsers = parser.add_subparsers()

    subparser = subparsers.add_parser(
        "run",
        help="Execute the scheduler (runs in foreground)"
    )
    subparser.set_defaults(func=cmd_run)

    subparser = subparsers.add_parser(
        "status",
        help="Connect to a running scheduler and show the status"
    )
    subparser.set_defaults(func=cmd_status)

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

    loop = asyncio.get_event_loop()
    try:
        sys.exit(loop.run_until_complete(args.func(loop, args)) or 0)
    finally:
        loop.close()
