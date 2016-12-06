import abc
import enum
import pathlib
import tempfile

import pytz

import toml

import xdg.BaseDirectory

from .backend import EncryptionMode, CompressionMethod


class IntervalUnit(enum.Enum):
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"


JOB_DEFAULTS = {
    "one_file_system": True,
}


def load_config():
    cfg_path = pathlib.Path(
        xdg.BaseDirectory.xdg_config_home
    ) / "cuteborg" / "config.toml"
    try:
        with cfg_path.open("r") as f:
            return toml.load(f)
    except OSError:
        return {}


def load_state():
    cfg_path = pathlib.Path(
        xdg.BaseDirectory.xdg_config_home
    ) / "cuteborg" / "state.toml"
    try:
        with cfg_path.open("r") as f:
            return toml.load(f)
    except OSError:
        return {}


class ConfigElementBase(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def load_from_raw(self, raw):
        pass

    @abc.abstractmethod
    def to_raw(self):
        return {}


class PruneConfig(ConfigElementBase):
    hourly = None
    daily = None
    weekly = None
    monthly = None
    yearly = None
    keep_within = None

    schedule = None

    def load_from_raw(self, raw):
        self.hourly = raw.get("hourly")
        self.daily = raw.get("daily")
        self.weekly = raw.get("weekly")
        self.monthly = raw.get("monthly")
        self.yearly = raw.get("yearly")
        self.keep_within = raw.get("keep_within")

        try:
            schedule_cfg = raw["schedule"]
        except KeyError:
            pass
        else:
            self.schedule = ScheduleConfig.from_raw(schedule_cfg)

    def _set_raw_attr(self, raw, attr):
        value = getattr(self, attr)
        if value is not None:
            raw[attr] = value

    def to_raw(self):
        raw = super().to_raw()
        self._set_raw_attr(raw, "hourly")
        self._set_raw_attr(raw, "daily")
        self._set_raw_attr(raw, "weekly")
        self._set_raw_attr(raw, "monthly")
        self._set_raw_attr(raw, "yearly")
        self._set_raw_attr(raw, "keep_within")

        if self.schedule is not None:
            raw["schedule"] = self.schedule.to_raw()

        return raw

    @classmethod
    def from_raw(cls, raw):
        self = cls()
        self.load_from_raw(raw)
        return self

    def to_kwargs(self):
        return {
            "hourly": self.hourly,
            "daily": self.daily,
            "weekly": self.weekly,
            "monthly": self.monthly,
            "yearly": self.yearly,
            "keep_within": self.keep_within,
        }


class RepositoryConfig(ConfigElementBase):
    id_ = None
    encryption_mode = None
    encryption_passphrase = None
    prune = None

    @abc.abstractmethod
    def load_from_raw(self, raw):
        self.id_ = raw["id"]

        encryption_cfg = raw["encryption"]

        self.encryption_mode = EncryptionMode(encryption_cfg["mode"])
        self.encryption_passphrase = encryption_cfg.get("passphrase")

        try:
            prune_cfg = raw["prune"]
        except KeyError:
            self.prune = None
        else:
            self.prune = PruneConfig.from_raw(prune_cfg)

    @abc.abstractmethod
    def to_raw(self):
        raw = super().to_raw()
        raw["id"] = self.id_

        encryption_cfg = raw.setdefault("encryption", {})
        encryption_cfg["mode"] = self.encryption_mode.value
        if self.encryption_passphrase is not None:
            encryption_cfg["passphrase"] = self.encryption_passphrase

        if self.prune is not None:
            prune_cfg = self.prune.to_raw()
            raw["prune"] = prune_cfg

        return raw

    @classmethod
    def from_raw(self, raw):
        try:
            type_ = raw["type"]
        except KeyError:
            raise ValueError("repository definition lacks type")

        if type_ == "local":
            result = LocalRepositoryConfig()
        elif type_ == "remote":
            result = RemoteRepositoryConfig()
        else:
            raise ValueError("unknown repository type: {!r}".format(
                type_,
            ))

        result.load_from_raw(raw)
        return result


class LocalRepositoryConfig(RepositoryConfig):
    path = None
    removable_device_uuid = None
    crypto_device_uuid = None
    crypto_passphrase = None

    def load_from_raw(self, raw):
        if raw["type"] != "local":
            raise ValueError("not a local repository config")

        super().load_from_raw(raw)
        local_cfg = raw["local"]

        self.path = local_cfg["path"]

        try:
            removable_cfg = local_cfg["removable"]
        except KeyError:
            pass
        else:
            self.removable_device_uuid = removable_cfg["device_uuid"]

        try:
            crypto_cfg = local_cfg["crypto"]
        except KeyError:
            pass
        else:
            self.crypto_device_uuid = crypto_cfg["device_uuid"]
            self.crypto_passphrase = crypto_cfg.get("passphrase")

    def to_raw(self):
        raw = super().to_raw()
        raw.setdefault("local", {})
        raw["local"]["path"] = self.path

        if self.removable_device_uuid is not None:
            removable_cfg = raw["local"].setdefault("removable")
            removable_cfg["device_uuid"] = self.removable_device_uuid

        if self.crypto_device_uuid is not None:
            crypto_cfg = raw["local"].setdefault("crypto")
            crypto_cfg["device_uuid"] = self.crypto_device_uuid
            if self.crypto_passphrase is not None:
                crypto_cfg["passphrase"] = self.crypto_passphrase

        return raw

    def make_repository_path(self):
        return self.path

    def setup_context(self, ctx):
        pass


class RemoteRepositoryConfig(RepositoryConfig):
    path = None
    borg_path = None
    host = None
    user = None

    def load_from_raw(self, raw):
        if raw["type"] != "remote":
            raise ValueError("not a remote repository config")

        super().load_from_raw(raw)
        remote_cfg = raw["remote"]

        self.path = remote_cfg["path"]
        self.host = remote_cfg["host"]
        self.user = remote_cfg.get("user")
        self.borg_path = remote_cfg.get("borg_path")

    def to_raw(self):
        raw = super().to_raw()

        remote_cfg = raw.setdefault("remote", {})
        remote_cfg["path"] = self.path
        remote_cfg["host"] = self.host

        if self.user is not None:
            remote_cfg["user"] = self.user

        if self.borg_path is not None:
            remote_cfg["borg_path"] = self.borg_path

        return raw

    def make_repository_path(self):
        parts = ["ssh://"]
        if self.user is not None:
            parts.append(self.user)
            parts.append("@")
        parts.append(self.host)
        parts.append(self.path)

        return "".join(parts)

    def setup_context(self, ctx):
        ctx.borg_remote_path = self.borg_path


class ScheduleConfig(ConfigElementBase):
    interval_unit = None
    interval_step = None
    timezone = None

    def load_from_raw(self, raw):
        super().load_from_raw(raw)
        self.interval_unit = IntervalUnit(raw["interval_unit"])
        self.interval_step = raw["interval_step"]
        self.timezone = raw.get("timezone", None)
        if self.timezone is not None:
            self.timezone = pytz.timezone(self.timezone)

    def to_raw(self):
        raw = super().to_raw()
        raw["interval_unit"] = self.interval_unit.value
        raw["interval_level"] = self.interval_level
        if self.timezone is not None:
            raw["timezone"] = self.timezone.zone

        return raw

    @classmethod
    def from_raw(cls, raw):
        self = cls()
        self.load_from_raw(raw)
        return self

    def __repr__(self):
        return (
            "<{}.{} "
            "interval_unit={} "
            "interval_step={} "
            "timezone={!r} "
            "at 0x{:x}>".format(
                type(self).__module__,
                type(self).__qualname__,
                self.interval_unit,
                self.interval_step,
                self.timezone,
                id(self),
            )
        )


class JobConfig(ConfigElementBase):
    name = None
    compression_method = None
    compression_level = None
    schedule = None

    def __init__(self):
        super().__init__()
        self.repositories = []

    def load_from_raw(self, raw, repositories):
        super().load_from_raw(raw)
        self.name = raw["name"]
        self.repositories = [
            repositories[repo_id]
            for repo_id in raw["repositories"]
        ]
        self.sources = list(raw["sources"])

        try:
            schedule_cfg = raw["schedule"]
        except KeyError:
            pass
        else:
            self.schedule = ScheduleConfig.from_raw(schedule_cfg)

    def to_raw(self):
        raw = super().to_raw()
        raw["name"] = self.name
        raw["repositories"] = [
            repo.id_
            for repo in self.repositories
        ]
        raw["sources"] = list(self.sources)

        if self.compression_method is not None:
            compression_cfg = raw.setdefault("compression", {})
            compression_cfg["method"] = self.compression_method.value
            if self.compression_level is None:
                compression_cfg["level"] = self.compression_level

        if self.schedule is not None:
            raw["schedule"] = self.schedule.to_raw()

    @classmethod
    def from_raw(self, raw, repositories):
        try:
            type_ = raw["type"]
        except KeyError:
            raise ValueError("job definition lacks type")

        if type_ == "simple":
            result = JobConfig()
        else:
            raise ValueError("unknown job type: {!r}".format(
                type_
            ))

        result.load_from_raw(raw, repositories)
        return result

    def setup_context(self, ctx):
        ctx.compression_method = self.compression_method
        ctx.compression_level = self.compression_level


class Config(ConfigElementBase):
    def __init__(self):
        super().__init__()
        self.repositories = {}
        self.jobs = {}

    def load_from_raw(self, raw):
        try:
            schedule_cfg = raw["schedule"]
        except KeyError:
            self.schedule = None
        else:
            self.schedule = ScheduleConfig.from_raw(schedule_cfg)

        for i, repo_raw in enumerate(raw["repository"]):
            if "id" not in repo_raw:
                raise ValueError(
                    "repository #{} (0-based) has no id".format(i)
                )

            try:
                repo = RepositoryConfig.from_raw(repo_raw)
            except (KeyError, ValueError) as exc:
                raise ValueError(
                    "failed to load repository #{} (0-based)".format(i)
                ) from exc

            self.repositories[repo.id_] = repo

        for i, job_raw in enumerate(raw["job"]):
            if "name" not in job_raw:
                raise ValueError(
                    "job #{} (0-based) has no name".format(i)
                )

            try:
                job = JobConfig.from_raw(job_raw, self.repositories)
            except (KeyError, ValueError) as exc:
                raise ValueError(
                    "failed to load job #{} (0-based)".format(i)
                ) from exc

            self.jobs[job.name] = job

    def to_raw(self):
        raw = super().to_raw()
        raw["repository"] = [
            repo.to_raw()
            for repo in self.repositories.values()
        ]

        raw["job"] = [
            job.to_raw()
            for job in self.jobs.values()
        ]

    @classmethod
    def from_raw(cls, raw):
        self = cls()
        self.load_from_raw(raw)
        return self


def save_config(cfg):
    cfg_path = pathlib.Path(
        xdg.BaseDirectory.xdg_config_home
    ) / "cuteborg" / "config.toml"
    with tempfile.NamedTemporaryFile(
            "w",
            dir=str(cfg_path.parent),
            delete=False) as temp:
        tempfile_path = cfg_path.parent / temp.name
        try:
            toml.dump(cfg, temp)
        except:
            try:
                tempfile_path.unlink()
            except OSError:
                # ignore, we want to re-raise the original problem instead
                pass
            raise

        tempfile_path.rename(cfg_path)


def save_state(cfg):
    cfg_path = pathlib.Path(
        xdg.BaseDirectory.xdg_config_home
    ) / "cuteborg" / "state.toml"
    with tempfile.NamedTemporaryFile(
            "w",
            dir=str(cfg_path.parent),
            delete=False) as temp:
        tempfile_path = cfg_path.parent / temp.name
        try:
            toml.dump(cfg, temp)
        except:
            try:
                tempfile_path.unlink()
            except OSError:
                # ignore, we want to re-raise the original problem instead
                pass
            raise

        tempfile_path.rename(cfg_path)


def find_repository_by_id(repositories, id_):
    for i, repo in enumerate(repositories):
        if repo["id"] == id_:
            return i
    return None
