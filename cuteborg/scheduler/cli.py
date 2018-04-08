import argparse
import asyncio
import inspect
import logging
import os
import pathlib
import pprint
import sys

from datetime import datetime

import xdg.BaseDirectory

from .. import utils
from . import core, status, protocol


def cmd_run(loop, args):
    logger = logging.getLogger("cuteborg.scheduler")

    scheduler = core.Scheduler(loop, args.socket_path)

    server = loop.run_until_complete(scheduler.check_server_socket())
    sock = scheduler.create_server_socket()

    if args.daemonize:
        logger.debug("forking into background")

        try:
            pid = os.fork()
        except OSError:
            logger.error("failed to fork")
            return

        if pid != 0:
            # parent
            return 0

        # child
        # start new session
        os.setsid()
        try:
            pid = os.fork()
        except OSError:
            logger.error("second fork failed")
            return 1

        if pid != 0:
            # parent -> exit
            return 0

        logger.debug("double-forked into background")

    asyncio.get_event_loop().close()
    loop = asyncio.get_event_loop_policy().new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(scheduler.main(sock=sock, loop=loop))

    sys.stdout.flush()
    sys.stderr.flush()


def _format_value(value):
    if isinstance(value, datetime):
        return str(value)
    return repr(value)


def _format_dict(d):
    return " ".join(
        "{}={}".format(key, _format_value(value))
        for key, value
        in sorted(d.items())
    )


def _format_status_row(item):
    reason_map = {
        "waiting_for_repository_lock": "repolock",
        "removable_device": "device",
        "sleep_until": "scheduled",
    }

    try:
        args = item["args"]
    except KeyError:
        args = str(item["raw_args"])
    else:
        args = _format_dict(args)

    state = "??"
    if item["running"] and not item.get("blocking"):
        state = "run"
    elif item.get("error"):
        state = "error"
    elif item.get("blocking"):
        state = "wait"

    status_parts = []
    try:
        blocking_info = item["blocking"]
    except KeyError:
        pass
    else:
        status_parts.append(
            reason_map.get(blocking_info["reason"],
                           blocking_info["reason"])
        )

        try:
            struct_info = blocking_info["struct_info"]
        except KeyError:
            status_parts.append(
                str(blocking_info["raw_info"])
            )
        else:
            status_parts.append(
                _format_dict(struct_info)
            )

    try:
        error_info = item["error"]
    except KeyError:
        pass
    else:
        status_parts.append("error since {}: {}".format(
            error_info["since"],
            error_info["message"],
        ))

    try:
        progress_info = item["progress"]
    except KeyError:
        pass
    else:
        status_parts.append("progress")
        status_parts.append(
            _format_dict(progress_info)
        )

    return (
        item["type"],
        args,
        state,
        " ".join(status_parts),
    )


@asyncio.coroutine
def cmd_status(loop, args):
    data = yield from status.get_status(loop, args.socket_path)

    logging.getLogger("cuteborg.scheduler").debug(
        "\n%s",
        pprint.pformat(data),
    )

    headers = [
        "type",
        "args",
        "state",
        "status",
    ]

    rows = list(map(_format_status_row, data["jobs"]))

    print("jobs: {}".format(len(data["jobs"])))
    utils.print_table(headers, rows)


@asyncio.coroutine
def cmd_reload(loop, args):
    logger = logging.getLogger("cuteborg.scheduler")

    try:
        endpoint = yield from asyncio.wait_for(
            protocol.test_and_get_socket(
                loop,
                logger,
                args.socket_path,
            ),
            timeout=5
        )
    except (asyncio.TimeoutError, FileNotFoundError, ConnectionError) as exc:
        msg = str(exc)
        if isinstance(exc, asyncio.TimeoutError):
            msg = "timeout"
        logger.error("failed to connect to scheduler at %r: %s",
                     str(args.socket_path),
                     msg)
        raise RuntimeError(
            "failed to connect to scheduler"
        )

    response, data = yield from endpoint.request(
        protocol.ToplevelCommand.RESCHEDULE
    )

    if response == protocol.ToplevelCommand.OKAY:
        logger.info("reload triggered successfully")
        return 0
    else:
        logger.error("could not trigger reload")
        return 2


@asyncio.coroutine
def cmd_stop(loop, args):
    logger = logging.getLogger("cuteborg.scheduler")

    try:
        endpoint = yield from asyncio.wait_for(
            protocol.test_and_get_socket(
                loop,
                logger,
                args.socket_path,
            ),
            timeout=5
        )
    except (asyncio.TimeoutError, FileNotFoundError, ConnectionError) as exc:
        msg = str(exc)
        if isinstance(exc, asyncio.TimeoutError):
            msg = "timeout"
        logger.error("failed to connect to scheduler at %r: %s",
                     str(args.socket_path),
                     msg)
        raise RuntimeError(
            "failed to connect to scheduler"
        )

    response, data = yield from endpoint.request(
        protocol.ToplevelCommand.EXIT
    )

    if response != protocol.ToplevelCommand.OKAY:
        logger.error("could not stop scheduler")
        return 2

    logger.info("scheduler stopping")

    try:
        yield from endpoint.protocol.breakdown_future
    except OSError:
        pass
    logger.info("scheduler stopped")


@asyncio.coroutine
def cmd_force_archive(loop, args):
    logger = logging.getLogger("cuteborg.scheduler")

    try:
        endpoint = yield from asyncio.wait_for(
            protocol.test_and_get_socket(
                loop,
                logger,
                args.socket_path,
            ),
            timeout=5
        )
    except (asyncio.TimeoutError, FileNotFoundError, ConnectionError) as exc:
        msg = str(exc)
        if isinstance(exc, asyncio.TimeoutError):
            msg = "timeout"
        logger.error("failed to connect to scheduler at %r: %s",
                     str(args.socket_path),
                     msg)
        raise RuntimeError(
            "failed to connect to scheduler"
        )

    request = {
        "command": "force-archive",
        "force-archive": {
            "name": args.job,
            "only": list(set(args.only)),
        }
    }

    response, data = yield from endpoint.request(
        protocol.ToplevelCommand.EXTENDED_REQUEST,
        request,
    )

    print(response, data)


@asyncio.coroutine
def cmd_force_prune(loop, args):
    logger = logging.getLogger("cuteborg.scheduler")

    try:
        endpoint = yield from asyncio.wait_for(
            protocol.test_and_get_socket(
                loop,
                logger,
                args.socket_path,
            ),
            timeout=5
        )
    except (asyncio.TimeoutError, FileNotFoundError, ConnectionError) as exc:
        msg = str(exc)
        if isinstance(exc, asyncio.TimeoutError):
            msg = "timeout"
        logger.error("failed to connect to scheduler at %r: %s",
                     str(args.socket_path),
                     msg)
        raise RuntimeError(
            "failed to connect to scheduler"
        )

    request = {
        "command": "force-prune",
        "force-prune": {
            "repositories": list(set(args.repositories)),
        }
    }

    response, data = yield from endpoint.request(
        protocol.ToplevelCommand.EXTENDED_REQUEST,
        request,
    )

    print(response, data)


def autoloop(loop, f, *args, **kwargs):
    if asyncio.iscoroutinefunction(f):
        return loop.run_until_complete(f(*args, **kwargs))
    else:
        return f(*args, **kwargs)


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
    subparser.add_argument(
        "-d", "--daemonize",
        action="store_true",
        default=False,
        help="Daemonize after setup instead of running in foreground",
    )

    subparser = subparsers.add_parser(
        "status",
        help="Connect to a running scheduler and show the status"
    )
    subparser.set_defaults(func=cmd_status)

    subparser = subparsers.add_parser(
        "reload",
        help="Connect to a running scheduler and ask it to schedule a "
        "reload and reschedule"
    )
    subparser.set_defaults(func=cmd_reload)

    subparser = subparsers.add_parser(
        "stop",
        help="Connect to a running scheduler and ask it to stop"
    )
    subparser.set_defaults(func=cmd_stop)

    subparser = subparsers.add_parser(
        "force-archive",
        help="Force an archive job to run",
    )
    subparser.add_argument(
        "job",
        help="Name of the job",
    )
    subparser.add_argument(
        "--only",
        action="append",
        default=[],
        help="Only run the job on this repository. May be given multiple times."
    )
    subparser.set_defaults(func=cmd_force_archive)

    subparser = subparsers.add_parser(
        "force-prune",
        help="Force a prune job to run",
    )
    subparser.add_argument(
        "repositories",
        metavar="REPO-ID",
        nargs="+",
        help="ID of the repositories to prune",
    )
    subparser.set_defaults(func=cmd_force_prune)

    args = parser.parse_args()

    if not hasattr(args, "func"):
        print("select a subcommand (use --help)", file=sys.stderr)
        sys.exit(1)

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
        sys.exit(autoloop(loop, args.func, loop, args) or 0)
    finally:
        loop.close()
