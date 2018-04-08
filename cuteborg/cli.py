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
import logging
import pathlib
import pprint
import uuid
import sys

from . import config, utils


def cmd_repository_add_local_directory(logger, cfg, state, args):
    for repo in cfg.setdefault("repository", []):
        if repo["type"] != "local":
            continue
        if repo["path"] == str(args.path):
            logger.error(
                "a repository with path %r already exists as id %r",
                str(args.path),
                repo["id"],
            )
            return False, False, 1

    uid = str(uuid.uuid4())

    repository = {
        "id": uid,
        "type": "local_directory",
        "path": str(args.path),
    }

    cfg["repository"].append(repository)

    print(uid)

    return True, False, 0


def cmd_repository_remove(logger, cfg, state, args):
    # TODO: check that no job uses that repository
    ifound = config.find_repository_by_id(
        cfg.get("repository", []),
        args.id,
    )

    if ifound is None:
        logger.error(
            "no such repository: %r",
            args.id,
        )
        return False, False, 1

    del cfg["repository"][ifound]

    return True, False, 0


def cmd_repository_list(logger, cfg, state, args):
    headers = [
        "id",
        "type",
        "path",
        "extra",
    ]

    rows = [
        (
            repo["id"],
            repo["type"],

            repo["local"]["path"] if repo["type"] == "local" else
            repo["remote"]["path"] if repo["type"] == "remote" else "",

            "dev={!r}".format(
                repo["local"].get("removable", {}).get("device_uuid", None),
            ) if repo["type"] == "local" else
            "user={!r} host={!r}".format(
                    repo["remote"].get("user"),
                    repo["remote"].get("host"),
            ) if repo["type"] == "remote" else ""
        )
        for repo in cfg.get("repository", [])
    ]

    utils.print_table(headers, rows)

    return False, False, 0


def cmd_job_add(logger, cfg, state, args):
    pass


def path(s):
    p = pathlib.Path(s).absolute()
    return p


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-v",
        dest="verbosity",
        action="count",
        default=0,
        help="Increase verbosity (up to -vvv)",
    )

    subparsers = parser.add_subparsers()

    subparser = subparsers.add_parser(
        "repository-add-local-directory",
        help="Add a new directory repository",
    )
    subparser.set_defaults(func=cmd_repository_add_local_directory)
    subparser.add_argument(
        "path",
        type=path,
        help="Path to the local directory to use as repository"
    )

    subparser = subparsers.add_parser(
        "repository-remove",
        help="Remove a repository",
    )
    subparser.set_defaults(func=cmd_repository_remove)
    subparser.add_argument(
        "id",
        help="ID of the repository to remove"
    )

    subparser = subparsers.add_parser(
        "repository-list",
        help="List known repositories",
    )
    subparser.set_defaults(func=cmd_repository_list)

    subparser = subparsers.add_parser(
        "job-add-local",
        help="Add local sourced job"
    )
    subparser.add_argument(
        "--repository",
        action="append",
        help="Add repository to run the job into"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level={
            0: logging.ERROR,
            1: logging.WARNING,
            2: logging.INFO,
        }.get(args.verbosity, logging.DEBUG),
    )

    logger = logging.getLogger("cuteborg")

    cfg = config.load_config()
    state = config.load_state()

    assert hasattr(args, "func")

    cfg_changed, state_changed, rc = args.func(
        logger,
        cfg,
        state,
        args,
    )

    if cfg_changed:
        config.save_config(cfg)
    if state_changed:
        config.save_state(state)

    sys.exit(rc)
