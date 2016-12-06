import ast
import asyncio
import functools
import os
import re
import subprocess

from datetime import datetime

from . import backend


VERSION_RE = re.compile(
    r"^\s*borg\s+"
    r"(?P<major>[0-9]+)\."
    r"(?P<minor>[0-9]+)\."
    r"(?P<release>[0-9]+)\s*$",
)


SIMPLE_ATTR_RE = re.compile(
    r"^(?P<name>[\w \(\)]+):\s+(?P<value>.+)$",
    re.MULTILINE,
)

SIZE_RE = re.compile(
    "[0-9.]+\s+[TGMK]B",
    re.I,
)

CREATE_PROGRESS_RE = re.compile(
    r"^(?P<uncompressed>[0-9.]+\s+[TGMK]?B)\s+O\s+"
    r"(?P<compressed>[0-9.]+\s+[TGMK]?B)\s+C\s+"
    r"(?P<deduplicated>[0-9.]+\s+[TGMK]?B)\s+D\s+"
    r"(?P<nfiles>[0-9]+)\s+N\s+"
    r".*$",
    re.I,
)


DELETE_PROGRESS_RE = re.compile(
    r"^Decrementing references\s+(?P<progress>\d+)%\s*$",
    re.I,
)


def parse_version(s):
    m = VERSION_RE.match(s)
    if m is None:
        raise ValueError("invalid borg version: {!r}".format(s))

    info = m.groupdict()
    return (
        int(info["major"]),
        int(info["minor"]),
        int(info["release"]),
    )


def parse_timestamp(s):
    return datetime.strptime(
        s,
        "%a, %Y-%m-%d %H:%M:%S"
    )


def parse_sizes(s):
    sizes = tuple(SIZE_RE.finditer(s))
    if len(sizes) != 3:
        raise ValueError(
            "failed to parse exactly three sizes "
            "from {!r}".format(s)
        )

    return tuple(
        match.group(0)
        for match in sizes
    )


def format_version(v):
    return ".".join(map(str, v))


def forward_create_progress(cb, s):
    parsed = CREATE_PROGRESS_RE.match(s)
    if parsed is None:
        cb(None)
        return

    info = parsed.groupdict()
    info["nfiles"] = int(info["nfiles"])
    info["uncompressed"] = tuple(info["uncompressed"].split())
    info["compressed"] = tuple(info["compressed"].split())
    info["deduplicated"] = tuple(info["deduplicated"].split())
    cb(info)


def forward_delete_progress(cb, s):
    parsed = DELETE_PROGRESS_RE.match(s)
    if parsed is None:
        cb(None)
        return

    info = parsed.groupdict()
    info["progress"] = int(info["progress"]) / 100
    cb(info)


class LocalSubprocessBackend(backend.Backend):
    SUPPORTED_VERSION_RANGES = [
        ((1, 0, 8), (1, 1, 0)),
    ]

    LIST_FORMAT = (
        "{{"
        "'mode': {mode!r}, "
        "'user': {user!r}, "
        "'group': {group!r}, "
        "'size': {size!r}, "
        "'mtime': {isomtime!r}, "
        "'path': {path!r}, "
        "'uid': {uid!r}, "
        "'gid': {gid!r}, "
        "'type': {type!r}, "
        "'extra': {extra!r}, "
        "'linktarget': {linktarget!r}, "
        "'source': {source!r}, "
        "}}, "
    )

    def __init__(self,
                 logger=None, *,  # NOQA
                 path_to_borg="borg"):
        super().__init__(logger=logger)
        self.path_to_borg = path_to_borg

        self._check_version()

    def _get_version(self):
        try:
            s = subprocess.check_output(
                [self.path_to_borg, "--version"],
            ).decode("ascii")
            version = parse_version(s)
        except ValueError as exc:
            raise RuntimeError(
                "could not determine version of borg",
            ) from exc
        return version

    def _check_version(self):
        version = self._get_version()

        self.logger.debug(
            "detected Borg version %s", ".".join(
                map(str, version)
            )
        )

        for ge_version, lt_version in self.SUPPORTED_VERSION_RANGES:
            if ge_version <= version < lt_version:
                return

        raise RuntimeError(
            "cannot use this version of Borg {}. "
            "supported versions are: {}".format(
                format_version(version),
                ", ".join(
                    "{} <= v < {}".format(
                        format_version(v1),
                        format_version(v2),
                    )
                    for v1, v2 in self.SUPPORTED_VERSION_RANGES
                )
            )
        )

    def _prep_call(self, args, env, context):
        args = [self.path_to_borg] + args

        if context.dry_run:
            args.insert(2, "-n")

        env = dict(os.environ)
        env.update(env)
        env["LANG"] = "C"

        self.logger.debug(
            "prepared call: %r, with env=%r (passphrase omitted from env)",
            args,
            env,
        )

        if context.passphrase is not None:
            env["BORG_PASSPHRASE"] = context.passphrase

        if context.borg_remote_path is not None:
            env["BORG_REMOTE_PATH"] = context.borg_remote_path

        return args, env

    # def _call_and_wait(self, args, env, context):
    #     args, env = self._prep_call(args, env, context)
    #     return subprocess.check_output(
    #         args,
    #         env=env,
    #     )

    # def _call_lines(self, args, env, context, line_callback):
    #     args, env = self._prep_call(args, env, context)

    #     proc = subprocess.Popen(
    #         args,
    #         env=env,
    #         stderr=subprocess.PIPE,
    #         universal_newlines=True,
    #     )

    #     for line in proc.stderr:
    #         if not line:
    #             break
    #         line_callback(line)

    #     proc.wait()

    @asyncio.coroutine
    def _call(self, args, env):
        proc = yield from asyncio.create_subprocess_exec(
            *args,
            env=env,
        )

        yield from proc.wait()
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(
                proc.returncode,
                args,
            )

    @asyncio.coroutine
    def _call_lines(self, args, env, line_callback):
        proc = yield from asyncio.create_subprocess_exec(
            *args,
            stderr=subprocess.PIPE,
            env=env,
        )

        potential_information = bytearray()
        try:
            while True:
                line = yield from proc.stderr.readuntil(b"\r")
                potential_information.extend(line)
                line_callback(line.decode())
        except asyncio.streams.IncompleteReadError as exc:
            potential_information.extend(exc.partial)

        yield from proc.wait()
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(
                proc.returncode,
                args,
                stderr=potential_information
            )

    def init_repository(self, path, mode, context):
        args = [
            "init",
            "-e", mode.value,
            path,
        ]

        return self._call(args, {}, context)

    def list_archives(self, path, context):
        args = [
            "list",
            path,
        ]

        return self._call(args, {}, context)

    @asyncio.coroutine
    def prune_repository(self, path,
                         context,
                         prefix,
                         *,
                         hourly=None,
                         daily=None,
                         weekly=None,
                         monthly=None,
                         yearly=None,
                         keep_within=None):
        args = [
            "prune",
            path,
        ]

        if prefix is not None:
            args.extend([
                "--prefix", prefix,
            ])

        for key, value in [
                ("hourly", hourly),
                ("daily", daily),
                ("weekly", weekly),
                ("monthly", monthly),
                ("yearly", yearly)]:
            if value is not None:
                args.extend([
                    "--keep-{}".format(key),
                    str(value),
                ])

        if keep_within is not None:
            args.extend([
                "--keep-within", str(keep_within)
            ])

        args, env = self._prep_call(args, {}, context)

        return (yield from self._call(args, env))

    @asyncio.coroutine
    def create_archive(self, path, name, source_paths, context):
        args = [
            "create",
            "::".join([path, name])
        ]
        env = {}

        if context.progress_callback:
            args.append("--progress")

        if context.compression != backend.CompressionMethod.NONE:
            compression_name = context.compression.value
            if context.compression_level is not None:
                compression_name += ","+str(context.compression_level)
            args.extend([
                "-C", compression_name,
            ])

        if context.one_file_system:
            args.append("-x")

        if (context.network_limit_upstream is not None
                or context.network_limit_downstream is not None):
            self.logger.warning(
                "using network limiting is experimental and may make borg hang"
            )
            parts = ["-s"]
            if context.network_limit_upstream is not None:
                parts.extend([
                    "-u", str(context.network_limit_upstream),
                ])
            if context.network_limit_downstream is not None:
                parts.extend([
                    "-d", str(context.network_limit_downstream),
                ])

            env["BORG_RSH"] = "trickle {} ssh".format(" ".join(parts))

        args.append("--")
        args.extend(map(str, source_paths))

        args, env = self._prep_call(args, env, context)

        line_proc = functools.partial(
            forward_create_progress,
            context.progress_callback or (lambda x: None),
        )

        try:
            yield from self._call_lines(args, env, line_proc)
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                "create_archive operation failed: {}".format(
                    exc.stderr.decode()
                )
            ) from None

    def get_archive_info(self, path, name, context):
        args = [
            "info",
            "::".join([path, name])
        ]
        env = {}

        info = self._call(args, env, context).decode("utf-8")

        attrs = {}
        for attr in SIMPLE_ATTR_RE.finditer(info):
            attr_info = attr.groupdict()
            attrs[attr_info["name"]] = attr_info["value"]

        try:
            start_time = parse_timestamp(attrs["Time (start)"])
        except (ValueError, KeyError):
            self.logger.warning("failed to parse timestamp", exc_info=True)
            start_time = None

        try:
            end_time = parse_timestamp(attrs["Time (end)"])
        except (KeyError, ValueError):
            self.logger.warning("failed to parse timestamp", exc_info=True)
            end_time = None

        try:
            nfiles = int(attrs["Number of files"])
        except (KeyError, ValueError):
            self.logger.warning("failed to parse number of files",
                                exc_info=True)
            nfiles = None

        try:
            uncompressed_size, compressed_size, deduplicated_size = \
                parse_sizes(attrs["This archive"])
        except (KeyError, ValueError):
            self.logger.warning("failed to parse sizes",
                                exc_info=True)
            uncompressed_size = None
            compressed_size = None
            deduplicated_size = None

        return backend.ArchiveInfo(
            name=attrs.get("Name"),
            fingerprint=attrs.get("Fingerprint"),
            hostname=attrs.get("Hostname"),
            username=attrs.get("Username"),
            start_time=start_time,
            end_time=end_time,
            commandline=attrs.get("Command line"),
            nfiles=nfiles,
            uncompressed_size=uncompressed_size,
            compressed_size=compressed_size,
            deduplicated_size=deduplicated_size,
        )

    def list_archive_contents(self, path, name, context, *, prefix=None):
        args = [
            "list",
            "::".join([path, name]),
            "--list-format", self.LIST_FORMAT,
        ]

        if prefix is not None:
            args.extend([
                "--prefix", prefix,
            ])

        env = {}

        items = ast.literal_eval(
            "[{}]".format(self._call(args, env, context).decode())
        )

        for item in items:
            try:
                item["mtime"] = parse_timestamp(item["mtime"])
            except ValueError:
                self.logger.warning("failed to parse timestamp", exc_info=True)
                item["mtime"] = None

        return items

    @asyncio.coroutine
    def delete_archive(self, path, name, context):
        args = [
            "delete",
            "::".join([path, name]),
        ]

        env = {}

        if context.progress_callback:
            args.append("--progress")

        args, env = self._prep_call(args, env, context)

        line_proc = functools.partial(
            forward_delete_progress,
            context.progress_callback or (lambda x: None),
        )

        yield from self._call_lines(
            args,
            env,
            functools.partial(
                forward_delete_progress,
                context.progress_callback,
            )
        )

    def delete_archive_cache(self, path, name, context):
        args = [
            "delete",
            "::".join([path, name]),
            "--cache-only",
        ]

        env = {}

        if context.progress_callback:
            args.append("--progress")

        if context.progress_callback:
            return self._call_lines(
                args,
                env,
                context,
                functools.partial(
                    forward_delete_progress,
                    context.progress_callback,
                )
            )
        else:
            return self._call(args, env, context)
