########################################################################
# File name: backend.py
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
import abc
import collections
import enum
import logging


class RepositoryLocked(Exception):
    def __init__(self):
        super().__init__(
            "the repository is locked",
        )


class RepositoryUnreachable(ConnectionError):
    def __init__(self, extra=None):
        msg = "the repository is unreachable"
        if extra is not None:
            msg = "{}: {}".format(msg, extra)
        super().__init__(msg)


class EncryptionMode(enum.Enum):
    NONE = "none"
    KEYFILE = "keyfile"
    REPOKEY = "repokey"


class CompressionMethod(enum.Enum):
    NONE = "none"
    LZ4 = "lz4"
    LZMA = "lzma"
    ZLIB = "zlib"


ArchiveInfo = collections.namedtuple(
    "ArchiveInfo",
    [
        "name",
        "fingerprint",
        "hostname",
        "username",
        "start_time",
        "end_time",
        "commandline",
        "nfiles",
        "uncompressed_size",
        "compressed_size",
        "deduplicated_size",
    ]
)


class Context:
    network_limit_upstream = None
    network_limit_downstream = None
    network_limit_builtin = None
    one_file_system = False
    compression = CompressionMethod.NONE
    compression_level = None
    passphrase = None
    dry_run = False
    progress_callback = None
    borg_remote_path = None


class Backend(metaclass=abc.ABCMeta):
    """
    Backend which interfaces with Borg.
    """

    def __init__(self, logger=None, **kwargs):
        super().__init__(**kwargs)
        if logger is None:
            logger = logging.getLogger(".".join([type(self).__module__,
                                                 type(self).__qualname__]))
        self.logger = logger

    @abc.abstractmethod
    def init_repository(self, path, mode, context):
        pass

    @abc.abstractmethod
    def list_archives(self, path, context):
        pass

    @abc.abstractmethod
    def prune_repository(self, path,
                         context,
                         prefix=None,
                         *,
                         hourly=None,
                         daily=None,
                         weekly=None,
                         monthly=None,
                         yearly=None,
                         keep_within=None):
        pass

    @abc.abstractmethod
    def create_archive(self, path, name, source_paths, context):
        pass

    @abc.abstractmethod
    def get_archive_info(self, path, name, context):
        pass

    @abc.abstractmethod
    def list_archive_contents(self, path, name, context):
        pass

    @abc.abstractmethod
    def delete_archive(self, path, name, context):
        pass
