########################################################################
# File name: core.py
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
import ast
import asyncio
import contextlib
import functools
import logging
import pathlib
import signal
import socket
import tempfile
import uuid

from datetime import datetime

import toml

import pytz

import xdg.BaseDirectory

import cuteborg.backend
import cuteborg.subprocess_backend
import cuteborg.config as config

from . import utils, protocol, wctsleep, jobs, devices

if not hasattr(asyncio, "ensure_future"):
    asyncio.ensure_future = asyncio.async


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


@contextlib.contextmanager
def note_job_block(mapping, key, blocker, notifier):
    unset = object()
    prev = mapping.get(key, unset)
    mapping[key] = blocker
    notifier()
    try:
        yield
    finally:
        if prev is unset:
            try:
                del mapping[key]
            except KeyError:
                pass
        else:
            mapping[key] = prev
        notifier()


class ServerProtocolEndpoint:
    def __init__(self, logger, scheduler):
        super().__init__()
        self.logger = logger
        self.scheduler = scheduler
        self.poll_pending = False
        self.poll_futures = []

    def _delayed_notify_poll(self):
        self.poll_pending = False
        self.logger.debug("notifying pollers")

        for fut in self.poll_futures:
            if fut.done():
                continue

            fut.set_result(None)
        self.poll_futures.clear()

    def notify_poll(self):
        if self.poll_pending:
            return

        self.poll_pending = True
        asyncio.get_event_loop().call_later(
            0.1,
            self._delayed_notify_poll,
        )

    def _handle_status_request(self):
        result = {
            "jobs": [],
        }

        for raw in self.scheduler.get_job_status():
            ((type_, *info),
             job_obj,
             task_fut,
             blocker,
             progress,
             error,
             last_run) = raw

            item = {
                "type": type_,
                "last_run": last_run,
            }

            if type_ == "create_archive":
                job_name, repo_id = info
                item["args"] = {}
                item["args"]["job"] = job_name
                item["args"]["repo"] = {
                    "id": repo_id,
                    "name": self.scheduler.config.repositories[repo_id].name
                }

            elif type_ == "prune":
                repo_id, = info
                item["args"] = {}
                item["args"]["repo"] = {
                    "id": repo_id,
                    "name": self.scheduler.config.repositories[repo_id].name
                }

            item["raw_args"] = list(info)

            if blocker is not None:
                reason, args = blocker
                item["blocking"] = {}
                item["blocking"]["reason"] = reason

                if reason == "waiting_for_repository_lock":
                    repo_id, = args
                    item["blocking"]["struct_info"] = {}
                    item["blocking"]["struct_info"]["repo"] = {
                        "id": repo_id,
                        "name": self.scheduler.config.repositories[repo_id].name
                    }

                elif reason == "sleep_until":
                    dt, = args
                    item["blocking"]["struct_info"] = {}
                    item["blocking"]["struct_info"]["wakeup_at"] = dt

                elif reason == "removable_device":
                    dev_uuid, = args
                    item["blocking"]["struct_info"] = {}
                    item["blocking"]["struct_info"]["device_uuid"] = dev_uuid

                elif reason == "network_host":
                    host, extra = args
                    item["blocking"]["struct_info"] = {}
                    item["blocking"]["struct_info"]["host"] = host
                    if extra is not None:
                        item["blocking"]["struct_info"]["extra"] = extra

                else:
                    item["blocking"]["raw_info"] = list(args)

            if progress is not None:
                item["progress"] = progress

            if error is not None:
                msg, timestamp = error
                item["error"] = {}
                item["error"]["message"] = msg
                item["error"]["since"] = timestamp

            item["running"] = task_fut is not None and not task_fut.done()

            result["jobs"].append(item)

        return protocol.ToplevelCommand.EXTENDED_REQUEST, result

    def _send_status(self, protocol, _):
        try:
            protocol.send(*self._handle_status_request())
        except ConnectionError:
            pass

    def handle_request(self, proto, cmd, extra_data):
        if cmd == protocol.ToplevelCommand.STATUS:
            return self._handle_status_request()
        elif cmd == protocol.ToplevelCommand.RESCHEDULE:
            self.scheduler._trigger_reload_and_reschedule()
            return protocol.ToplevelCommand.OKAY, None
        elif cmd == protocol.ToplevelCommand.POLL:
            self.logger.debug("received poll request")
            fut = asyncio.Future()
            fut.add_done_callback(functools.partial(self._send_status, proto))
            self.poll_futures.append(fut)
            return self._handle_status_request()
        elif cmd == protocol.ToplevelCommand.EXIT:
            self.scheduler._trigger_exit()
            return protocol.ToplevelCommand.OKAY, None
        elif cmd == protocol.ToplevelCommand.EXTENDED_REQUEST:
            try:
                if extra_data["command"] == "force-archive":
                    job = extra_data["force-archive"]["name"]
                    only = extra_data["force-archive"].get("repositories")
                    self.scheduler._force_archive(
                        job,
                        only=only,
                    )
                    return protocol.ToplevelCommand.OKAY, None
                elif extra_data["command"] == "force-prune":
                    self.scheduler._force_prune(
                        extra_data["force-prune"]["repositories"]
                    )
                    return protocol.ToplevelCommand.OKAY, None
            except KeyError as exc:
                self.logger.debug(
                    "missing required data for extended request (%r)",
                    exc,
                )

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

        self.wctsleep = None

        self._default_prune_schedule = config.ScheduleConfig.from_raw(
            self.DEFAULT_PRUNE_SCHEDULE
        )

        self._endpoint = None
        self._wakeup_event = None
        self._stop_event = None

        self._last_runs = {}
        self._jobs = {}
        self._job_tasks = {}
        self._job_blockers = {}
        self._job_progress = {}
        self._job_errors = {}
        self._sleep_tasks = {}
        self._repository_locks = {}
        self._deferred_jobs = set()

        self._load_state()

        self._rls_pending = False
        self._shutting_down = False
        self._reload_and_reschedule(defer=True)

    def _save_state(self):
        cfg_path = pathlib.Path(
            xdg.BaseDirectory.xdg_config_home
        ) / "cuteborg" / "scheduler-state.toml"

        self.logger.debug("saving state to %s", cfg_path)

        state = {
            "last_runs": [
                {
                    "key": list(key),
                    "timestamp": ts,
                }
                for key, ts in self._last_runs.items()
            ]
        }

        config.toml_to_file(state, cfg_path)

    def _load_state(self):
        cfg_path = pathlib.Path(
            xdg.BaseDirectory.xdg_config_home
        ) / "cuteborg" / "scheduler-state.toml"

        self.logger.debug("loading state from %s", cfg_path)
        try:
            with cfg_path.open("r") as f:
                state = toml.load(f)
        except OSError as exc:
            self.logger.warning("failed to load state: %s")
            return

        self._last_runs = {
            tuple(item["key"]): item["timestamp"]
            for item in state.get("last_runs", [])
        }

    def _create_protocol(self):
        self.logger.debug("new connection")
        proto = protocol.ControlProtocol(
            self.logger.getChild("control"),
            self._endpoint.handle_request,
        )
        return proto

    def _job_main_task_done(self, job_obj, task):
        try:
            task.result()
        except asyncio.CancelledError:
            try:
                ex_job_obj, ex_main_task = self._jobs[job_obj.key]
            except KeyError:
                # task was cancelled because the job was erased
                pass
            else:
                if ex_job_obj is job_obj and not self._shutting_down:
                    # task was cancelled for unknown reasons
                    self.logger.warning(
                        "unexpected cancellation of main task for job %r",
                        job_obj,
                    )
                # task was cancelled because it was replaced
        except Exception as exc:
            self.logger.exception(
                "main task for job %r failed",
                job_obj
            )
            self.set_job_error(job_obj.key, str(exc))
        else:
            self.logger.warning(
                "main task for job %r terminated",
                job_obj
            )

    def _job_task_done(self, job_obj, start_time, task):
        try:
            task.result()
        except asyncio.CancelledError:
            if not self._shutting_down:
                self.logger.warning(
                    "unexpected cancellation of task for job %r",
                    job_obj,
                )
        except:
            self.logger.exception(
                "task for job %r failed",
                job_obj
            )
        else:
            # clear errors
            self._job_errors.pop(job_obj.key, None)

        if self._job_tasks[job_obj.key] != task:
            self.logger.warning(
                "wtf? inconsistent internal state: "
                "self._job_tasks[job_obj.key] != task ... "
                "unsure what to do, continuing for now"
            )

        del self._job_tasks[job_obj.key]

        try:
            del self._job_progress[job_obj.key]
        except KeyError:
            pass

        self.logger.debug(
            "recording last run of %r as %s",
            job_obj,
            start_time
        )
        self._last_runs[job_obj.key] = start_time

        job_obj, main_task = self._jobs[job_obj.key]
        if main_task is None:
            self.logger.debug(
                "starting main task for %r now after it has been deferred "
                "earlier",
                job_obj
            )
            main_task = asyncio.ensure_future(
                job_obj.run()
            )
            self._jobs[job_obj.key] = job_obj, main_task

        self._endpoint.notify_poll()

    def _add_job(self, job_obj, defer):
        key = job_obj.key

        try:
            job_obj, main_task = self._jobs[key]
        except KeyError:
            if defer:
                self.logger.debug("deferring startup of task for job %r",
                                  job_obj)
                self._deferred_jobs.add(key)
                main_task = None
            else:
                self.logger.debug("starting main task for job %r", job_obj)
                main_task = asyncio.ensure_future(job_obj.run())
                main_task.add_done_callback(
                    functools.partial(
                        self._job_main_task_done,
                        job_obj,
                    )
                )
        else:
            self.logger.debug(
                "deferring start of main task for job %r, as a task "
                "is currently running",
                job_obj
            )
            main_task = None

        self._jobs[key] = (
            job_obj, main_task
        )

    def _reload_and_reschedule(self, defer=False):
        self._rls_pending = False
        self.logger.debug("reloading configuration")
        self.config = config.Config.from_raw(config.load_config())

        self.logger.debug(
            "found %d repositories with %d jobs in total",
            len(self.config.repositories),
            len(self.config.jobs),
        )

        for job_obj, job_main_task in self._jobs.values():
            if job_main_task is not None:
                self.logger.debug("stopping main task of job %s", job_obj.key)
                job_main_task.cancel()

        self._jobs.clear()
        self._deferred_jobs.clear()
        self._job_errors.clear()

        new_repositories = set(self.config.repositories)
        old_repositories = set(self._repository_locks)

        self.logger.debug("old repositories = %r, new repositories = %r",
                          old_repositories, new_repositories)

        if not defer:
            for id_ in (new_repositories - old_repositories):
                # create locks for added repositories
                self.logger.debug("repository %r: preparing", id_)
                self._repository_locks[id_] = asyncio.Lock(loop=self.loop)

            for id_ in (old_repositories - new_repositories):
                self.logger.debug("repository %r removed -- tearing down", id_)
                del self._repository_locks[id_]

        # At this point, last_run information needs to be available

        for id_, repository in self.config.repositories.items():
            if repository.prune is not None:
                self.logger.debug(
                    "repository %r: setting up prune job",
                    id_
                )

                instance_logger = self.logger.getChild(
                    "prune",
                ).getChild(
                    str(id_)
                )

                job_obj = jobs.PruneRepository(
                    instance_logger,
                    self,
                    repository.prune,
                    repository,
                    repository.prune.schedule or self._default_prune_schedule,
                )

                self._add_job(job_obj, defer)

        for name, job in self.config.jobs.items():
            for repository in job.repositories:
                self.logger.debug("job %r, repository %r: preparing",
                                  name, repository.id_)
                instance_logger = self.logger.getChild(
                    "create_archive"
                ).getChild(
                    job.name
                ).getChild(
                    str(repository.id_)
                )

                job_obj = jobs.CreateArchive(
                    instance_logger,
                    self,
                    job,
                    repository,
                    job.schedule or self.config.schedule,
                )

                self._add_job(job_obj, defer)

        self.logger.debug("repository_locks = %r", self._repository_locks)
        self.logger.debug("jobs = %r", self._jobs)
        self.logger.debug("job_tasks = %r", self._job_tasks)
        self.logger.debug("max_slack = %r", self.config.max_slack)
        self.logger.debug("poll_interval = %r", self.config.poll_interval)

        if self.wctsleep is not None:
            self.wctsleep.max_slack = self.config.max_slack
        if self._wakeup_event is not None:
            self._wakeup_event.set()
        if self._endpoint is not None:
            self._endpoint.notify_poll()

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

    def _trigger_exit(self):
        self._stop_event.set()

    def _signalled_reload_and_reschedule(self):
        self.logger.info(
            "reload and reschedule requested via SIGUSR1/SIGHUP -- "
            "scheduling reload"
        )
        self._trigger_reload_and_reschedule()

    @asyncio.coroutine
    def _main_loop(self):
        for key in self._deferred_jobs:
            try:
                job_obj, main_task = self._jobs.pop(key)
            except KeyError:
                continue

            if main_task is None:
                self._add_job(job_obj, False)
            else:
                self._jobs[key] = job_obj, main_task

        self._deferred_jobs.clear()

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

            done, pending = yield from asyncio.wait(
                [
                    stop_future,
                    wakeup_future,
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if stop_future in done:
                self.logger.info("received stop signal (SIGTERM or SIGINT)")
                return False

            return True

    @asyncio.coroutine
    def check_server_socket(self):
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

    def create_server_socket(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
        sock.bind(str(self.socket_path))
        return sock

    @asyncio.coroutine
    def bind_server_socket(self, sock):
        return (yield from self.loop.create_unix_server(
            self._create_protocol,
            sock=sock,
        ))

    @asyncio.coroutine
    def main(self, sock=None, loop=None):
        if sock is None:
            yield from self.check_server_socket()
            sock = self.create_server_socket()

        if loop is not None and loop != self.loop:
            self.loop = loop

        self._endpoint = ServerProtocolEndpoint(
            self.logger.getChild("control"),
            self
        )
        self._wakeup_event = asyncio.Event(loop=loop)
        self._stop_event = asyncio.Event(loop=loop)
        self.wctsleep = wctsleep.WallClockTimeSleepImpl()
        self._repository_locks.clear()
        self._reload_and_reschedule()

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

        server = yield from self.bind_server_socket(sock)

        self.logger.info("server started up")

        try:
            # main_loop returns True while it should keep looping
            # and False otherwise
            while (yield from self._main_loop()):
                pass
        finally:
            self.logger.info("saving state")
            self._save_state()
            self.logger.info("shutting down server")
            server.close()
            self._stop_event.clear()
            self.logger.warning(
                "stopping server and tasks -- "
                "if it hangs, send SIGINT/SIGTERM again"
            )

            self._shutting_down = True

            for job_task in self._job_tasks.values():
                job_task.cancel()

            for _, job_main_task in self._jobs.values():
                job_main_task.cancel()

            other_futures = list(self._job_tasks.values())
            other_futures.extend(
                job_main_task
                for _, job_main_task in self._jobs.values()
            )
            other_futures.append(
                server.wait_closed()
            )
            other_futures.append(
                self.wctsleep.cancel_and_wait()
            )

            other_futures = [
                asyncio.ensure_future(fut)
                for fut in other_futures
            ]

            other_futures_fut = asyncio.ensure_future(
                asyncio.wait(
                    other_futures,
                ),
            )

            def debug_done(fut):
                self.logger.debug("shutdown: %r finished", fut)

            for fut in other_futures:
                fut.add_done_callback(debug_done)
                self.logger.debug("waiting for: %r", fut)

            self.logger.warning(
                "waiting for server and tasks -- "
                "if it hangs, send SIGINT/SIGTERM again"
            )

            stop_fut = asyncio.ensure_future(self._stop_event.wait())

            done, pending = yield from asyncio.wait(
                [
                    stop_fut,
                    other_futures_fut,
                ],
                return_when=asyncio.FIRST_COMPLETED
            )

            for fut in pending:
                if fut != stop_fut:
                    self.logger.warning("not finished: %r", fut)
                fut.cancel()

            self.logger.debug(
                "shutdown completed"
            )

    def get_last_run(self, key):
        return self._last_runs.get(key)

    @asyncio.coroutine
    def sleep_until(self, job_obj, dt):
        with note_job_block(
                self._job_blockers,
                job_obj.key,
                (
                    "sleep_until",
                    (dt, ),
                ),
                self._endpoint.notify_poll):
            wait_task = asyncio.ensure_future(
                self.wctsleep.sleep_until(dt)
            )
            self._sleep_tasks[job_obj.key] = wait_task, False
            try:
                yield from wait_task
            except asyncio.CancelledError:
                _, awoken = self._sleep_tasks.get(job_obj.key, (None, False))
                if awoken:
                    return
                raise
            finally:
                self._sleep_tasks.pop(job_obj.key, None)

    def execute_job(self, job_obj, **kwargs):
        key = job_obj.key
        task = asyncio.ensure_future(job_obj.exec(**kwargs))
        self._job_tasks[key] = task
        task.add_done_callback(
            functools.partial(
                self._job_task_done,
                job_obj,
                datetime.utcnow()
            )
        )
        return asyncio.ensure_future(asyncio.shield(task))

    def _force_archive(self, job, only=None):
        if only is not None:
            keys = [
                ("create_archive", job, repo)
                for repo in only
            ]
        else:
            keys = [
                key
                for key in self._sleep_tasks.keys()
                if key[0] == "create_archive" and key[1] == job
            ]

        tasks = []
        for key in keys:
            sleeper, _ = self._sleep_tasks[key]
            self._sleep_tasks[key] = sleeper, True
            sleeper.cancel()

    def _force_prune(self, repositories):
        keys = [
            ("prune", repo)
            for repo in repositories
        ]

        tasks = []
        for key in keys:
            sleeper, _ = self._sleep_tasks[key]
            self._sleep_tasks[key] = sleeper, True
            sleeper.cancel()

    @asyncio.coroutine
    def _wait_for_remote_repository(self, logger, job_key, remote_repo,
                                    update_status):
        context = cuteborg.backend.Context()
        remote_repo.setup_context(context)

        path = remote_repo.make_repository_path()
        while True:
            try:
                yield from self.backend.ping(path, context)
            except cuteborg.backend.RepositoryUnreachable as exc:
                update_status(str(exc))
            else:
                break

            yield from asyncio.sleep(self.config.poll_interval)

        return path

    @asyncio.coroutine
    def _wait_for_removable_device(self, logger, job_key, local_repo):
        problem_known = None
        while True:
            try:
                mount_path = yield from devices.wait_for_mounted(
                    logger,
                    local_repo.removable_device_uuid,
                    self.config.poll_interval,
                    crypto_passphrase=local_repo.crypto_passphrase
                )
                break
            except devices.EncryptedDeviceWithoutPassphrase as exc:
                if problem_known is not type(exc):
                    self.set_job_error(job_key, str(exc))
                problem_known = type(exc)

            yield from asyncio.sleep(self.config.poll_interval)

        # clear errors
        self._job_errors.pop(job_key, None)

        logger.debug(
            "device is mounted at %r",
            mount_path
        )
        return mount_path + local_repo.make_repository_path()

    @asyncio.coroutine
    def wait_for_repository_storage(self, job_obj, repository_cfg):
        if isinstance(repository_cfg, config.RemoteRepositoryConfig):
            with contextlib.ExitStack() as stack:
                has_block = False

                def update_status(msg):
                    nonlocal has_block
                    argv = (
                        "network_host",
                        (repository_cfg.host, msg),
                    )
                    if not has_block:
                        stack.enter_context(
                            note_job_block(
                                self._job_blockers,
                                job_obj.key,
                                (
                                    "network_host",
                                    (repository_cfg.host, msg),
                                ),
                                self._endpoint.notify_poll)
                        )
                        has_block = True
                    else:
                        self._job_blockers[job_obj.key] = argv
                        self._endpoint.notify_poll()

                return (yield from self._wait_for_remote_repository(
                    job_obj.logger,
                    job_obj.key,
                    repository_cfg,
                    update_status,
                ))
        elif isinstance(repository_cfg, config.LocalRepositoryConfig):
            if repository_cfg.removable_device_uuid is not None:
                with note_job_block(
                        self._job_blockers,
                        job_obj.key,
                        (
                            "removable_device",
                            (repository_cfg.removable_device_uuid, ),
                        ),
                        self._endpoint.notify_poll):
                    return (yield from self._wait_for_removable_device(
                        job_obj.logger,
                        job_obj.key,
                        repository_cfg
                    ))
            else:
                return repository_cfg.make_repository_path()

    @asyncio.coroutine
    def lock_repository(self, job_obj, id_):
        with note_job_block(
                self._job_blockers,
                job_obj.key,
                (
                    "waiting_for_repository_lock",
                    (id_,)
                ),
                self._endpoint.notify_poll):
            return (yield from self._repository_locks[id_])

    def get_job_status(self):
        for key, (job_obj, job_main_task) in self._jobs.items():
            yield (
                key,
                job_obj,
                self._job_tasks.get(key),
                self._job_blockers.get(key),
                self._job_progress.get(key),
                self._job_errors.get(key),
                self._last_runs.get(key),
            )

    def set_job_progress(self, job_obj, progress):
        self._job_progress[job_obj.key] = progress
        self._endpoint.notify_poll()

    def set_job_error(self, job_key, message, timestamp=None):
        if message is None:
            try:
                del self._job_errors[job_key]
            except KeyError:
                pass
            else:
                self._endpoint.notify_poll()
            return

        timestamp = timestamp or datetime.utcnow()
        self._job_errors[job_key] = (
            message,
            timestamp,
        )
        self._endpoint.notify_poll()
