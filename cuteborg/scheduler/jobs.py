import abc
import asyncio

from datetime import datetime

import cuteborg.backend

from . import utils


def _schedule_next(now, schedule, last_run):
    start_of_current_interval = utils.align_to_interval(
        now,
        schedule.interval_step,
        schedule.interval_unit,
    )

    if (last_run is not None and
            last_run >= start_of_current_interval):
        start_of_current_interval = utils.step_interval(
            start_of_current_interval,
            schedule.interval_step,
            schedule.interval_unit,
        )

    # end_of_current_interval = utils.step_interval(
    #     start_of_current_interval,
    #     schedule.interval_step,
    #     schedule.interval_unit,
    # )

    if now > start_of_current_interval:
        return now

    # TODO: honour scheduling preferences
    return start_of_current_interval


class Job(metaclass=abc.ABCMeta):
    def __init__(self, logger, scheduler):
        super().__init__()
        self.logger = logger
        self.scheduler = scheduler

    @abc.abstractproperty
    def key(self):
        pass

    @asyncio.coroutine
    def retry_locked(self, action, *args, **kwargs):
        while True:
            try:
                yield from action(*args, **kwargs)
            except cuteborg.backend.RepositoryLocked:
                self.logger.warning(
                    "repository is locked, will retry after 30 seconds"
                )
                self.scheduler.set_job_error(
                    self.key,
                    "repository is locked by an external process",
                )
                yield from asyncio.sleep(30)
                continue
            break

    @abc.abstractmethod
    def exec(self):
        """
        Execute a single instance of the job.

        This coroutine is cancelled when the scheduler shuts down, but not when
        re-scheduling takes place.
        """

    @abc.abstractmethod
    def run(self):
        """
        Schedule the job and run it as needed.

        This coroutine is cancelled when rescheduling takes place. Running jobs
        are protected if ran through :meth:`.core.Scheduler.execute_job`.
        """


class PeriodicRepositoryJob(Job):
    def __init__(self, logger, scheduler, repository_cfg, schedule):
        super().__init__(logger, scheduler)
        self.repository_cfg = repository_cfg
        self.schedule = schedule

    @asyncio.coroutine
    def exec(self, **kwargs):
        self.logger.debug("locking repository %r", self.repository_cfg.id_)
        with (yield from self.scheduler.lock_repository(
                self,
                self.repository_cfg.id_)):
            self.logger.debug("locked repository %r", self.repository_cfg.id_)
            yield from self._exec(**kwargs)

    @asyncio.coroutine
    def run(self):
        while True:
            last_run = self.scheduler.get_last_run(self.key)
            if last_run is None:
                self.logger.debug("job was not run yet")
            else:
                self.logger.debug("last run was at %r", last_run)

            next_run = _schedule_next(
                datetime.utcnow(),
                self.schedule,
                last_run,
            )

            self.logger.debug("next run is at %s", next_run)
            yield from self.scheduler.sleep_until(self, next_run)

            # waits until the storage of the repository is reachable
            repo_path = yield from self.scheduler.wait_for_repository_storage(
                self,
                self.repository_cfg
            )

            self.logger.debug(
                "repository is at %r",
                repo_path,
            )

            yield from self.scheduler.execute_job(self, repo_path=repo_path)


class PruneRepository(PeriodicRepositoryJob):
    def __init__(self, logger, scheduler, prune_cfg, repository_cfg, schedule):
        super().__init__(logger, scheduler, repository_cfg, schedule)
        self.prune_cfg = prune_cfg

    @property
    def key(self):
        return ("prune",
                self.repository_cfg.id_)

    @asyncio.coroutine
    def _exec(self, repo_path):
        job_names = []
        for name, job in self.scheduler.config.jobs.items():
            if self.repository_cfg in job.repositories:
                job_names.append(name)

        settings = self.repository_cfg.prune.to_kwargs()

        context = cuteborg.backend.Context()
        self.repository_cfg.setup_context(context)

        for name in job_names:
            prefix = name + "-"

            self.logger.debug(
                "pruning archives starting with %r", prefix
            )

            self.scheduler.set_job_progress(
                self,
                {
                    "job": name,
                }
            )

            yield from self.retry_locked(
                self.scheduler.backend.prune_repository,
                repo_path,
                context,
                prefix,
                **settings,
            )
            self.scheduler.set_job_error(self.key, None)

        self.scheduler.set_job_progress(self, None)


class CreateArchive(PeriodicRepositoryJob):
    def __init__(self, logger, scheduler, job_cfg, repository_cfg, schedule):
        super().__init__(logger, scheduler, repository_cfg, schedule)
        self.job_cfg = job_cfg

    @property
    def key(self):
        return ("create_archive",
                self.job_cfg.name,
                self.repository_cfg.id_)

    def _update_progress(self, progress):
        self.scheduler.set_job_error(self.key, None)
        if progress is not None:
            self.scheduler.set_job_progress(
                self,
                {
                    key: (" ".join(value)
                          if not isinstance(value, int)
                          else value)
                    for key, value in progress.items()
                }
            )
        else:
            self.scheduler.set_job_progress(self, None)

    @asyncio.coroutine
    def _exec(self, repo_path):
        self.logger.debug("starting job")
        now = datetime.utcnow()

        archive_name = self.job_cfg.name + "-" + now.isoformat()

        context = cuteborg.backend.Context()
        context.progress_callback = self._update_progress
        self.repository_cfg.setup_context(context)
        self.job_cfg.setup_context(context)

        yield from self.retry_locked(
            self.scheduler.backend.create_archive,
            repo_path,
            archive_name,
            self.job_cfg.sources,
            context,
        )
