import abc
import collections
import enum
import logging


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
    one_file_system = False
    compression = CompressionMethod.NONE
    compression_level = None
    passphrase = None
    dry_run = False
    progress_callback = None


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
