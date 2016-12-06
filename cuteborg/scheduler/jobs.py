import abc
import asyncio

from datetime import datetime

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


class CreateArchive(Job):
    def __init__(self, logger, scheduler, job_cfg, repository_cfg):
        super().__init__(logger, scheduler)
        self.job_cfg = job_cfg
        self.repository_cfg = repository_cfg
        self.schedule = job_cfg.schedule or scheduler.config.schedule

    @property
    def key(self):
        return ("create_archive",
                self.job_cfg.name,
                self.repository_cfg.id_)

    @asyncio.coroutine
    def exec(self, logger):
        pass

    @asyncio.coroutine
    def run(self):
        last_run = self.scheduler.get_last_run(self.key)

        while True:
            next_run = _schedule_next(
                datetime.utcnow(),
                self.schedule,
                last_run,
            )

            yield from self.scheduler.sleep_until(next_run)

            last_run = next_run
            yield from self.scheduler.execute_job(self)
