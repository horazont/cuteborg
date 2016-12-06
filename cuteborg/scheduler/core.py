import asyncio
import contextlib
import functools
import logging
import signal
import uuid

from datetime import datetime

import cuteborg.backend
import cuteborg.subprocess_backend
import cuteborg.config as config

from . import utils, protocol, wctsleep


def _schedule_in_interval(now, interval_start, interval_end):
    # TODO: honour scheduling preferences
    if now > interval_start:
        return now
    return interval_start


def _schedule(now, interval_step, interval_unit, last_run):
    start_of_current_interval = utils.align_to_interval(
        now,
        interval_step,
        interval_unit,
    )

    if (last_run is not None and
            last_run >= start_of_current_interval):
        start_of_current_interval = utils.step_interval(
            start_of_current_interval,
            interval_step,
            interval_unit,
        )

    end_of_current_interval = utils.step_interval(
        start_of_current_interval,
        interval_step,
        interval_unit,
    )

    return _schedule_in_interval(
        now,
        start_of_current_interval,
        end_of_current_interval,
    )


@contextlib.contextmanager
def cancelling(fut):
    try:
        yield fut
    finally:
        if not fut.done():
            fut.cancel()


class ServerProtocolEndpoint:
    def __init__(self, logger, scheduler):
        super().__init__()
        self.logger = logger
        self.scheduler = scheduler

    def _handle_status_request(self):
        self.logger.debug("got status request")
        result = {
            "schedule": [],
            "running": [],
        }

        for ts, (action, *args) in self.scheduler._schedule:
            item = {
                "at": ts,
                "action": action,
            }

            if action == "create_archive":
                job, repository = args
                action_info = {
                    "job_name": job.name,
                    "repository_id": repository.id_,
                }
                item["create_archive"] = action_info
            elif action == "prune":
                repository, = args
                action_info = {
                    "repository_id": repository.id_,
                }
                item["prune"] = action_info

            result["schedule"].append(item)

        for (action, *args), (run_id, progress) in self.scheduler._active_runs.items():
            item = {
                "run_id": str(run_id),
                "action": action,
                "args": list(args),
            }

            if progress is not None:
                item["progress"] = progress

            result["running"].append(item)

        return protocol.ToplevelCommand.EXTENDED_REQUEST, result

    def handle_request(self, proto, cmd, extra_data):
        if cmd == protocol.ToplevelCommand.STATUS:
            return self._handle_status_request()

        self.logger.debug(
            "no way to handle request %r (extra=%r)",
            cmd, extra_data,
        )
        return protocol.ToplevelCommand.ERROR, None


class Scheduler:
    DEFAULT_PRUNE_SCHEDULE = {
        "interval_step": 1,
        "interval_unit": "day",
    }

    def __init__(self, loop, socket_path):
        super().__init__()
        self.loop = loop
        self.logger = logging.getLogger("cuteborg.scheduler")
        self.socket_path = socket_path
        self.backend = cuteborg.subprocess_backend.LocalSubprocessBackend(
            logger=self.logger.getChild("backend")
        )

        self.wctsleep = wctsleep.WallClockTimeSleepImpl()

        self._default_prune_schedule = config.ScheduleConfig.from_raw(
            self.DEFAULT_PRUNE_SCHEDULE
        )

        self._wakeup_event = asyncio.Event(loop=self.loop)
        self._stop_event = asyncio.Event(loop=self.loop)

        self._last_runs = {}
        self._jobs = []

        self._rls_pending = False
        self._reload_and_reschedule()

        self._endpoint = ServerProtocolEndpoint(
            self.logger.getChild("control"),
            self
        )

    def _create_protocol(self):
        self.logger.debug("new connection")
        proto = protocol.ControlProtocol(
            self.logger.getChild("control"),
            self._endpoint.handle_request,
        )
        return proto

    def _note_issue(self, fmt, *args, logger=None, **kwargs):
        msg = fmt.format(*args, **kwargs)
        self._issues.append(msg)
        if logger is None:
            logger = self.logger
        logger.error(msg)

    def _reschedule(self, now, schedule, run_key):
        last_run = self._last_runs.get(run_key)

        if last_run is None:
            self.logger.debug(
                "%r: was not run yet",
                run_key,
            )
        else:
            self.logger.debug(
                "%r: previous run was at %r",
                run_key,
                last_run,
            )

        next_run = _schedule(
            now,
            schedule.interval_step,
            schedule.interval_unit,
            last_run,
        )

        self.logger.debug(
            "%r: next run at %s",
            run_key,
            next_run,
        )

        return next_run

    def _reload_and_reschedule(self):
        self._rls_pending = False
        self.logger.debug("reloading configuration")
        self.config = config.Config.from_raw(config.load_config())

        self.logger.debug(
            "found %d repositories with %d jobs in total",
            len(self.config.repositories),
            len(self.config.jobs),
        )

        now = datetime.utcnow()

        repository_locks = {}
        schedule = []

        for id_, repository in self.config.repositories.items():
            self.logger.debug("repository %r: preparing", id_)
            repository_locks[id_] = asyncio.Lock(loop=self.loop)

            # if repository.prune is not None:
            #     prune = repository.prune
            #     self.logger.debug("repository %r: uses pruning", id_)

            #     prune_schedule = prune.schedule or self._default_prune_schedule

            #     self.logger.debug("repository %r: uses prune schedule %r",
            #                       id_, prune_schedule)

            #     next_run = self._reschedule(
            #         now, prune_schedule,
            #         ("prune", repository.id_),
            #     )

            #     schedule.append(
            #         (next_run, ("prune", repository))
            #     )

        for name, job in self.config.jobs.items():
            self.logger.debug("scheduling job %r", name)

            job_schedule = (job.schedule or self.config.schedule)

            if job_schedule is None:
                self._note_issue(
                    "job {!r}: has no schedule!",
                    name
                )
                continue

            self.logger.debug("job %r: uses schedule %r",
                              name, job_schedule)

            for repository in job.repositories:
                next_run = self._reschedule(
                    now,
                    job_schedule,
                    ("create_archive", job.name, repository.id_),
                )

                schedule.append(
                    (next_run, ("create_archive", job, repository))
                )

        schedule.sort(key=lambda x: x[0])
        self._repository_locks = repository_locks
        self._schedule = schedule
        self.logger.debug("schedule: %s", self._schedule)

        self._wakeup_event.set()

    def _scheduled_reload_and_reschedule(self):
        if not self._rls_pending:
            return
        self._reload_and_reschedule()

    def _trigger_reload_and_reschedule(self):
        if self._rls_pending:
            return

        self._rls_pending = True
        self.loop.call_later(
            1,
            self._scheduled_reload_and_reschedule
        )

    def _signalled_reload_and_reschedule(self):
        self.logger.info(
            "reload and reschedule requested via SIGUSR1/SIGHUP -- "
            "scheduling reload"
        )
        self._trigger_reload_and_reschedule()

    def _spawn_done(self, id_, task):
        try:
            result = task.result()
        except asyncio.CancelledError:
            self.logger.info("task %s: cancelled", id_)
        except:
            self.logger.exception("task %s: failed", id_)
        else:
            if result is not None:
                self.logger.info("task %s: returned value: %r", id_, result)

    def _spawn(self, coro, id_=None):
        task = asyncio.async(coro, loop=self.loop)
        task.add_done_callback(functools.partial(self._spawn_done, id_))
        self._running_jobs.append(task)
        return task

    @asyncio.coroutine
    def _job_create_archive(self, run_id, job, repository):
        logger = self.logger.getChild("run").getChild(str(run_id))

        def show_progress(progress):
            nonlocal state
            state.clear()
            if progress is not None:
                state.update({
                    key: " ".join(value) if not isinstance(value, int) else value
                    for key, value in progress.items()
                })
            logger.debug("in progress ... %s", state)

        run_key = "create_archive", job.name, repository.id_
        now = datetime.utcnow()

        logger.debug("running job %r on repository %r",
                     job.name,
                     repository.id_)

        logger.debug("locking. now is %s", now)
        state = {}
        self._active_runs[run_key] = run_id, state
        try:
            archive_name = job.name + "-" + now.isoformat()

            logger.debug("acquiring lock on repository %r", repository.id_)
            with (yield from self._repository_locks[repository.id_]):
                logger.debug("lock on repository acquired")

                repo_path = repository.make_repository_path()

                logger.debug(
                    "job info: "
                    "\nrepository_path=%r"
                    "\narchive_name=%r",
                    repo_path,
                    archive_name,
                )

                context = cuteborg.backend.Context()
                context.progress_callback = show_progress
                repository.setup_context(context)
                job.setup_context(context)

                yield from self.backend.create_archive(
                    repo_path,
                    archive_name,
                    job.sources,
                    context,
                )

            logger.info("run successful! "
                        "marking done and rescheduling")
            self._last_runs[run_key] = now
            job_schedule = job.schedule or self.config.schedule
            self._schedule.append(
                (
                    self._reschedule(
                        now,
                        job_schedule,
                        ("create_archive", job.name, repository.id_),
                    ),
                    ("create_archive", job, repository),
                )
            )
            self._schedule.sort(key=lambda x: x[0])
            self._wakeup_event.set()
        finally:
            logger.debug("releasing lock")
            del self._active_runs[run_key]

    @asyncio.coroutine
    def _job_prune(self, run_id, repository):
        logger = self.logger.getChild("run").getChild(str(run_id))

        run_key = "prune", repository.id_
        now = datetime.utcnow()

        logger.debug("pruning repository %r",
                     repository.id_)

        logger.debug("locking. now is %s", now)
        self._active_runs[run_key] = run_id, None
        try:
            logger.debug("acquiring lock on repository %r", repository.id_)
            with (yield from self._repository_locks[repository.id_]):
                logger.debug("lock on repository acquired")

                job_names = []
                for name, job in self.config.jobs.items():
                    if repository in job.repositories:
                        job_names.append(name)

                repo_path = repository.make_repository_path()
                settings = repository.prune.to_kwargs()

                logger.debug(
                    "job info: "
                    "\nrepository_path=%r"
                    "\njobs=%r"
                    "\nintervals=%r",
                    repo_path,
                    job_names,
                    settings,
                )

                context = cuteborg.backend.Context()
                repository.setup_context(context)

                for name in job_names:
                    prefix = name + "-"

                    yield from self.backend.prune_repository(
                        repo_path,
                        context,
                        prefix,
                        **settings,
                    )

            logger.info("run successful! "
                        "marking done and rescheduling")
            self._last_runs[run_key] = now
            prune_schedule = (repository.prune.schedule or
                              self._default_prune_schedule)
            self._schedule.append(
                (
                    self._reschedule(
                        now,
                        prune_schedule,
                        ("prune", repository.id_),
                    ),
                    ("prune", repository),
                )
            )
            self._schedule.sort(key=lambda x: x[0])
            self._wakeup_event.set()
        finally:
            logger.debug("releasing lock")
            del self._active_runs[run_key]

    @asyncio.coroutine
    def _main_loop(self):
        with contextlib.ExitStack() as stack:
            stop_future = stack.enter_context(
                cancelling(
                    asyncio.async(
                        self._stop_event.wait(),
                        loop=self.loop)
                )
            )

            self._wakeup_event.clear()
            wakeup_future = stack.enter_context(
                cancelling(
                    asyncio.async(
                        self._wakeup_event.wait(),
                        loop=self.loop)
                )
            )

            self.logger.debug("determining wait time")

            if not self._schedule:
                self.logger.warning(
                    "schedule is empty"
                )
                timeout = None
            else:
                now = datetime.utcnow()
                next_event_at, _ = self._schedule[0]
                next_event_in = (next_event_at - now).total_seconds()

                # wake up every five minutes
                # that’s a workaround for not handling suspend/resume
                timeout = min(max(next_event_in, 0), 300)

            if timeout is None or timeout > 0:
                if timeout is None:
                    self.logger.debug("sleeping until signal")
                else:
                    self.logger.debug("sleeping for %.1f seconds", timeout)
                # wait for timeout
                done, pending = yield from asyncio.wait(
                    [stop_future, wakeup_future],
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                    loop=self.loop,
                )

                if stop_future in done:
                    self.logger.debug("received stop signal")
                    return False

                return True

            jobs_to_run = []
            now = datetime.utcnow()
            for i, (ts, job) in enumerate(self._schedule):
                if now >= ts:
                    jobs_to_run.append(job)
                else:
                    self._schedule = self._schedule[i:]
                    break
            else:
                self._schedule.clear()

            self.logger.info("%d jobs ready to run", len(jobs_to_run))

            for action, *args in jobs_to_run:
                run_id = uuid.uuid4()

                if action == "create_archive":
                    job, repository = args
                    run_key = "create_archive", job.name, repository.id_
                    impl = self._job_create_archive
                elif action == "prune":
                    repository, = args
                    run_key = "prune", repository.id_
                    impl = self._job_prune

                try:
                    existing_run_id, _ = self._active_runs[run_key]
                except KeyError:
                    pass
                else:
                    self.logger.warning(
                        "SKIPPING job %r on repository %r, as a job (%s) "
                        "is currently running",
                        job.name,
                        repository.id_,
                        existing_run_id,
                    )
                    continue

                self.logger.info("running job %r as %s",
                                 run_key,
                                 run_id)
                self._spawn(
                    impl(
                        run_id,
                        *args,
                    ),
                    run_id,
                )

            return True

    @asyncio.coroutine
    def main(self):
        try:
            endpoint = yield from asyncio.wait_for(
                protocol.test_and_get_socket(
                    self.loop,
                    self.logger,
                    self.socket_path,
                ),
                timeout=5
            )
        except (FileNotFoundError):
            pass
        except ConnectionError:
            self.logger.error(
                "socket %r exists, but it doesn’t conform to scheduler "
                "protocol. aborting.",
                str(self.socket_path)
            )
            return 1
        except asyncio.TimeoutError:
            self.logger.error(
                "socket %r exists, but no reply received there in time."
                " is there a running scheduler which is stuck?",
                str(self.socket_path)
            )
            return 1
        else:
            self.logger.error(
                "socket %r exists, and a scheduler is replying there! "
                "aborting.",
                str(self.socket_path),
            )
            endpoint.close()
            return 1

        try:
            self.socket_path.parent.mkdir(mode=0o700, parents=True)
        except FileExistsError:
            if not self.socket_path.parent.is_dir():
                raise

        if self.socket_path.exists():
            self.logger.error(
                "socket %s exists! please remove it.",
                self.socket_path,
            )
            return 1

        self.loop.add_signal_handler(signal.SIGINT, self._stop_event.set)
        self.loop.add_signal_handler(signal.SIGTERM, self._stop_event.set)

        self.loop.add_signal_handler(
            signal.SIGHUP,
            self._signalled_reload_and_reschedule,
        )
        self.loop.add_signal_handler(
            signal.SIGUSR1,
            self._signalled_reload_and_reschedule,
        )

        server = yield from self.loop.create_unix_server(
            self._create_protocol,
            str(self.socket_path),
        )

        self.logger.info("server started up")

        try:
            # main_loop returns True while it should keep looping
            # and False otherwise
            while (yield from self._main_loop()):
                pass
        finally:
            self.logger.info("shutting down server")
            server.close()
            self._stop_event.clear()
            self.logger.warning(
                "waiting for server shutdown -- "
                "if it hangs, send SIGINT/SIGTERM again"
            )
            done, pending = yield from asyncio.wait(
                [
                    server.wait_closed(),
                    self._stop_event.wait(),
                ],
                return_when=asyncio.FIRST_COMPLETED)

            for fut in pending:
                fut.cancel()
