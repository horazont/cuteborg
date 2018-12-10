########################################################################
# File name: __init__.py
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
import asyncio
import enum
import itertools
import pathlib
import signal
import subprocess

from datetime import datetime

import babel.dates

import PyQt5.Qt as Qt

import cuteborg.scheduler.protocol

from qtborg import utils

from .ui_main import Ui_StatusWindow


SortRole = Qt.Qt.UserRole


@asyncio.coroutine
def try_start():
    try:
        proc = yield from asyncio.create_subprocess_exec(
            "cuteborg-scheduler",
            "-vv",
            "run",
            "-d",
            stdin=subprocess.DEVNULL,
        )
    except OSError:
        return False

    yield from proc.wait()

    return proc.returncode == 0


@asyncio.coroutine
def try_stop():
    proc = yield from asyncio.create_subprocess_exec(
        "cuteborg-scheduler",
        "-vvv",
        "stop",
        stdin=subprocess.DEVNULL,
    )

    yield from proc.wait()

    return proc.returncode == 0


def _load_pixmap(name):
    for p in __path__:
        filename = pathlib.Path(p) / "data" / name
        pixmap = Qt.QPixmap(str(filename))
        if pixmap.isNull():
            continue
        return pixmap
    raise FileNotFoundError("icon {!r} not found".format(name))


def _load_icon(name):
    return Qt.QIcon(_load_pixmap(name))


def _compose_icon(bottom_left=None, top_right=None):
    base = _load_pixmap("base.png")
    if bottom_left is None and top_right is None:
        return Qt.QIcon(base)

    if bottom_left is not None:
        bottom_left = _load_pixmap(bottom_left).toImage()
    if top_right is not None:
        top_right = _load_pixmap(top_right).toImage()

    base = base.toImage()
    painter = Qt.QPainter()
    painter.begin(base)

    if bottom_left is not None:
        y = base.height() - bottom_left.height()
        painter.drawImage(
            Qt.QPoint(0, y),
            bottom_left
        )

    if top_right is not None:
        x = base.width() - top_right.width()
        painter.drawImage(
            Qt.QPoint(x, 0),
            top_right
        )

    painter.end()

    return Qt.QIcon(Qt.QPixmap.fromImage(base))


CREATE_ARCHIVE_BASE_TEMPLATE = (
    "<tr><td>archive</td>"
    "<td>{job}</td>"
    "<td>{repo[name]}</td>"
    "<td>{status}</td></tr>"
)

CREATE_ARCHIVE_PROGRESS_TEMPLATE = (
    "{progress[uncompressed]} archived"
)

PRUNE_RUNNING_TEMPLATE = (
    "<tr><td>prune</td>"
    "<td>{progress[job]}</td>"
    "<td>{repo[name]}</td>"
    "<td>in progress</td></tr>"
)

PRUNE_NONRUNNING_TEMPLATE = (
    "<tr><td>prune</td>"
    "<td>—</td>"
    "<td>{repo[name]}</td>"
    "<td>{status}</td></tr>"
)


def format_job_state_table(job, now, parts=None):
    parts = parts or []

    if "progress" in job:
        progress = job["progress"]
        if progress.get("synced", 1) < 1:
            parts.append("synchronizing")
        else:
            parts.append(CREATE_ARCHIVE_PROGRESS_TEMPLATE.format(**job))

    if "blocking" in job:
        blocking_info = job["blocking"]
        if blocking_info["reason"] == "sleep_until":
            parts.append(
                babel.dates.format_timedelta(
                    blocking_info["struct_info"]["wakeup_at"] - now,
                    add_direction=True,
                )
            )

        elif blocking_info["reason"] == "waiting_for_repository_lock":
            parts.append(
                "waiting for other job to finish",
            )

        elif blocking_info["reason"] == "removable_device":
            parts.append(
                "waiting for removable device",
            )

        elif blocking_info["reason"] == "network_host":
            parts.append(
                "waiting for network connectivity"
            )

        else:
            parts.append(blocking_info["reason"])

    if "error" in job:
        parts.append(
            "<strong>Error:</strong> {}".format(
                job["error"]["message"]
            )
        )

    return "<br/>".join(parts)


def format_job_state_view(job):
    parts = []

    if "blocking" in job:
        blocking_info = job["blocking"]
        if blocking_info["reason"] == "sleep_until":
            # shown at other place in table
            pass

        elif blocking_info["reason"] == "waiting_for_repository_lock":
            parts.append(
                "waiting for other job to finish",
            )

        elif blocking_info["reason"] == "removable_device":
            parts.append(
                "waiting for removable device",
            )

        elif blocking_info["reason"] == "network_host":
            parts.append(
                "waiting for network connectivity"
            )

        else:
            parts.append(blocking_info["reason"])

    if "error" in job:
        parts.append(
            "error: {}".format(
                job["error"]["message"]
            )
        )

    return "; ".join(parts)


class JobsModel(Qt.QAbstractTableModel):
    COLUMN_TYPE = 0
    COLUMN_JOB = 1
    COLUMN_REPOSITORY = 2
    COLUMN_STATE = 3
    COLUMN_LAST_RUN = 4
    COLUMN_NEXT_RUN = 5
    COLUMN_PROGRESS = 6
    COLUMN_COUNT = 7

    REFERENCE_TIME = datetime(1970, 1, 1, 1, 0)

    def __init__(self):
        super().__init__()
        self.jobs = []
        self.known_keys = set()

    def update_jobs(self, jobs):
        update_tmp = {}
        abandoned_keys = set(self.known_keys)

        for job in jobs:
            key = (job["type"], ) + tuple(job["raw_args"])

            abandoned_keys.discard(key)

            if key in self.known_keys:
                update_tmp[key] = job
                continue
            self.known_keys.add(key)

            self.beginInsertRows(Qt.QModelIndex(),
                                 len(self.jobs),
                                 len(self.jobs))
            self.jobs.append(job)
            self.endInsertRows()

        indices_to_delete = []

        for i, job in enumerate(self.jobs):
            key = (job["type"], ) + tuple(job["raw_args"])

            if key in abandoned_keys:
                indices_to_delete.append(i)
                self.known_keys.discard(key)
                continue

            try:
                update = update_tmp.pop(key)
            except KeyError:
                continue

            job.clear()
            job.update(update)

            self.dataChanged.emit(
                self.index(i, 0),
                self.index(i, self.COLUMN_COUNT),
            )

        for index in reversed(indices_to_delete):
            self.beginRemoveRows(Qt.QModelIndex(), index, index)
            del self.jobs[index]
            self.endRemoveRows()

    def rowCount(self, parent):
        if parent.isValid():
            return 0
        return len(self.jobs)

    def columnCount(self, parent):
        return self.COLUMN_COUNT

    def index(self, row, column, parent=None):
        return self.createIndex(
            row,
            column,
            self.jobs[row],
        )

    def headerData(self, section, orientation, role):
        if orientation == Qt.Qt.Horizontal:
            if role == Qt.Qt.DisplayRole:
                return [
                    "Type",
                    "Job",
                    "Repository",
                    "State",
                    "Last run",
                    "Next run",
                    "Progress",
                ][section]
            elif role == Qt.Qt.InitialSortOrderRole:
                return [
                    Qt.Qt.AscendingOrder,
                    Qt.Qt.AscendingOrder,
                    Qt.Qt.AscendingOrder,
                    Qt.Qt.AscendingOrder,
                    Qt.Qt.AscendingOrder,
                    Qt.Qt.DescendingOrder,
                    Qt.Qt.AscendingOrder,
                ][section]

    def data(self, index, role):
        if role != Qt.Qt.DisplayRole and role != SortRole:
            return

        if not index.isValid():
            return

        job = index.internalPointer()

        if index.column() == self.COLUMN_TYPE:
            if job["type"] == "create_archive":
                return "archive"
            elif job["type"] == "prune":
                return "prune"
            else:
                return job["type"]

        elif index.column() == self.COLUMN_JOB:
            try:
                return job["args"]["job"]
            except KeyError:
                if job["type"] == "prune":
                    try:
                        return job["progress"]["job"]
                    except KeyError:
                        pass

        elif index.column() == self.COLUMN_REPOSITORY:
            try:
                return job["args"]["repo"]["name"]
            except KeyError:
                pass

        elif index.column() == self.COLUMN_STATE:
            if job["running"] and not job.get("blocking"):
                state = "running"
            elif job.get("error"):
                state = "failed"
            elif job.get("blocking"):
                if job["blocking"]["reason"] == "sleep_until":
                    state = "scheduled"
                else:
                    state = "waiting"
            else:
                state = "rescheduling"

            return state

        elif index.column() == self.COLUMN_LAST_RUN:
            try:
                last_run = job["last_run"]
            except KeyError:
                if role == SortRole:
                    return float("-inf")
                return "never"
            if role == SortRole:
                if last_run is None:
                    last_run = datetime.utcnow()
                return (last_run - self.REFERENCE_TIME).total_seconds()
            elif role == Qt.Qt.ToolTipRole:
                return str(last_run)
            else:
                return babel.dates.format_timedelta(
                    last_run - datetime.utcnow(),
                    add_direction=True,
                    granularity='minute',
                )

        elif index.column() == self.COLUMN_NEXT_RUN:
            try:
                wakeup_at = job["blocking"]["struct_info"]["wakeup_at"]
            except KeyError:
                if role == SortRole:
                    return float("-inf")
                return "now"
            if role == SortRole:
                return (wakeup_at - self.REFERENCE_TIME).total_seconds()
            elif role == Qt.Qt.ToolTipRole:
                return str(wakeup_at)
            else:
                return babel.dates.format_timedelta(
                    wakeup_at - datetime.utcnow(),
                    granularity='minute',
                    add_direction=True,
                )

        elif index.column() == self.COLUMN_PROGRESS:
            if job["type"] == "create_archive" and "progress" in job:
                progress = job["progress"]
                if progress["synced"] < 1:
                    return "synchronizing ({:.0f}%)".format(
                        progress["synced"] * 100,
                    )
                return "archived {} from {} files".format(
                    job["progress"]["uncompressed"],
                    job["progress"]["nfiles"],
                )
            elif job["type"] == "prune" and "progress" in job:
                return "pruning archives from job {!r}".format(
                    job["progress"]["job"]
                )
            else:
                return format_job_state_view(job)

    def flags(self, index):
        return Qt.Qt.ItemIsEnabled | Qt.Qt.ItemIsSelectable


class ErrorCondition(enum.Enum):
    SCHEDULER_NOT_RUNNING = "The scheduler is not running!"
    REMOVABLE_DEVICE_MISSING = \
        "A removable device ({}) must be inserted for backup jobs to continue."
    OTHER_ERROR = "Error: {}"
    NETWORK_HOST = \
        "Cannot access borg server at {!r}{}."


class Main(Qt.QMainWindow):
    def __init__(self, loop, logger, socket_path):
        super().__init__()
        self.logger = logger

        self.ui = Ui_StatusWindow()
        self.ui.setupUi(self)

        self.trayicon = Qt.QSystemTrayIcon()
        self.trayicon.activated.connect(
            self._tray_icon_activated
        )
        self.trayicon.installEventFilter(self)

        self.icons = {
            (colour, flag): _compose_icon(
                flag and "bullet_{}.png".format(flag),
                colour and "bullet_{}.png".format(colour),
            )
            for colour, flag in itertools.product(
                (None, "blue", "orange", "red"),
                (None, "error"))
        }

        self._connected = False
        self._stop = False
        self._quit = False
        self._jobs = []
        self._error_conditions = set()
        self._curr_icon = None, "error"
        self.trayicon.setIcon(
            self.icons[self._curr_icon],
        )
        self.socket_path = socket_path
        self.loop = loop

        self.stop_event = asyncio.Event()

        self.blink_task = None

        self.jobs_model = JobsModel()
        proxy_model = Qt.QSortFilterProxyModel(self.ui.jobs_view)
        proxy_model.setSourceModel(self.jobs_model)
        proxy_model.setSortRole(SortRole)
        proxy_model.setSortCaseSensitivity(Qt.Qt.CaseInsensitive)
        proxy_model.setSortLocaleAware(True)
        proxy_model.sort(
            JobsModel.COLUMN_NEXT_RUN,
            Qt.Qt.DescendingOrder,
        )

        self.ui.jobs_view.setModel(proxy_model)
        self.ui.jobs_view.horizontalHeader().setSectionResizeMode(
            Qt.QHeaderView.ResizeToContents,
        )
        self.ui.jobs_view.horizontalHeader().setSortIndicator(
            JobsModel.COLUMN_NEXT_RUN,
            Qt.Qt.DescendingOrder,
        )

        self.ui.action_close.triggered.connect(
            self._action_close,
        )

        self.ui.action_quit.triggered.connect(
            self._action_quit,
        )

        self.ui.action_quit_and_stop.triggered.connect(
            self._action_quit_and_stop,
        )

        signal.signal(
            signal.SIGINT,
            signal.SIG_DFL,
        )

        signal.signal(
            signal.SIGTERM,
            signal.SIG_DFL,
        )

    def _action_close(self):
        self.hide()

    def _action_quit(self, _):
        result = Qt.QMessageBox.warning(
            self,
            "Exit CuteBorg Status",
            "Exiting CuteBorg status will not stop the CuteBorg "
            "scheduler which runs your backups, but will remove "
            "the indicator icon which shows you the status.",
            Qt.QMessageBox.Ok | Qt.QMessageBox.Cancel,
        )

        self._quit = True
        if result == Qt.QMessageBox.Ok:
            self.stop_event.set()

    def _action_quit_and_stop(self, _):
        result = Qt.QMessageBox.warning(
            self,
            "Exit CuteBorg Status and stop CuteBorg",
            "If you proceed, your backups will not continue to be "
            "run until you re-start CuteBorg manually.",
            Qt.QMessageBox.Ok | Qt.QMessageBox.Cancel,
        )

        self._quit = True
        self._stop = True
        if result == Qt.QMessageBox.Ok:
            self.stop_event.set()

    def _tray_icon_activated(self, reason):
        if reason == Qt.QSystemTrayIcon.Trigger:
            self.show()
            self.raise_()
            return

    @asyncio.coroutine
    def _blink_task(self):
        try:
            while True:
                yield from asyncio.sleep(1.5)
                self.trayicon.setIcon(self.icons[None, None])
                yield from asyncio.sleep(0.5)
                self.trayicon.setIcon(self.icons[self._curr_icon])
        except asyncio.CancelledError:
            self.trayicon.setIcon(self.icons[self._curr_icon])

    def _update_jobs(self, jobs):
        self._jobs = jobs
        self.jobs_model.update_jobs(jobs)

    def _set_error_condition(self, condition, data=(), *,
                             suppress_notify=False):
        key = condition, data
        if key in self._error_conditions:
            return

        self._error_conditions.add(key)
        self._update_status_label()

        if not suppress_notify and Qt.QSystemTrayIcon.supportsMessages():
            self.trayicon.showMessage(
                "CuteBorg error",
                condition.value.format(*data),
                Qt.QSystemTrayIcon.Warning,
            )

    def _update_status_label(self):
        if self._error_conditions:
            self.ui.status_box.show()
            self.ui.status_icon.setPixmap(
                Qt.QApplication.style().standardIcon(
                    Qt.QStyle.SP_MessageBoxWarning,
                ).pixmap(48)
                # self.icons[None, "error"].pixmap(48),
            )
            text = []
            for condition, data in self._error_conditions:
                text.append(
                    "<li>{}</li>".format(
                        condition.value.format(*data)
                    )
                )

            self.ui.status_label.setText(
                "<h6>Problems:</h6><ul>{}</ul>".format("".join(text))
            )
        else:
            self.ui.status_box.hide()

    def _clear_error_condition(self, condition, data=None):
        self._error_conditions.discard((condition, data))
        self._update_status_label()

    def _update_error_conditions_from_jobs(self):
        current_conditions = set()

        for job in self._jobs:
            try:
                error_info = job["error"]
            except KeyError:
                pass
            else:
                current_conditions.add(
                    (ErrorCondition.OTHER_ERROR,
                     (error_info["message"],))
                )
                continue

            try:
                blocking_info = job["blocking"]
            except KeyError:
                pass
            else:
                if blocking_info["reason"] == "removable_device":
                    current_conditions.add(
                        (ErrorCondition.REMOVABLE_DEVICE_MISSING,
                         (blocking_info["struct_info"]["device_uuid"],))
                    )
                elif blocking_info["reason"] == "network_host":
                    extra = blocking_info["struct_info"].get("extra")
                    if extra is not None:
                        extra = " ({})".format(extra)
                    current_conditions.add(
                        (ErrorCondition.NETWORK_HOST,
                         (blocking_info["struct_info"]["host"],
                          extra))
                    )

        for key in set(self._error_conditions):
            if key not in current_conditions:
                self._clear_error_condition(*key)

        for key in current_conditions:
            self._set_error_condition(*key)

    def _generate_tooltip_from_jobs(self):
        job_table = []
        nrunning = 0
        now = datetime.utcnow()

        for job in self._jobs:
            if job["running"]:
                nrunning += 1

            if job["type"] == "create_archive":
                job_table.append(
                    CREATE_ARCHIVE_BASE_TEMPLATE.format(
                        job=job["args"]["job"],
                        repo=job["args"]["repo"],
                        status=(
                            format_job_state_table(job, now)
                        ),
                    )
                )

            elif job["type"] == "prune":
                if "progress" in job:
                    job_table.append(
                        PRUNE_RUNNING_TEMPLATE.format(
                            repo=job["args"]["repo"],
                            progress=job["progress"],
                        )
                    )
                else:
                    job_table.append(
                        PRUNE_NONRUNNING_TEMPLATE.format(
                            repo=job["args"]["repo"],
                            status=format_job_state_table(job, now),
                        )
                    )

        return "<p>running jobs: {}</p>{}".format(
            nrunning,
            ("<table><tr><th>Task</th><th>Job name</th><th>Repository</th><th>Status</th></tr>{}</table>".format("".join(job_table)))
            if job_table else ""
        )

    def eventFilter(self, object_, ev):
        if object_ == self.trayicon:
            if ev.type() == Qt.QEvent.ToolTip:
                if self._connected:
                    Qt.QToolTip.showText(
                        ev.globalPos(),
                        self._generate_tooltip_from_jobs(),
                    )
                else:
                    Qt.QToolTip.showText(
                        ev.globalPos(),
                        "<strong>Scheduler not running!</strong>"
                    )
                return True
            return False
        else:
            return False

    def _update_trayicon_from_jobs(self):
        nrunning = False
        any_failed = False
        any_warning = False

        for job in self._jobs:
            if job["running"]:
                nrunning += 1

            any_failed = (
                any_failed or
                "error" in job and "blocking" not in job
            )

            any_warning = (
                any_warning or
                "error" in job or
                job.get("blocking", {}).get("reason") == "removable_device"
            )

        if any_failed:
            self._curr_icon = "red", None
        elif any_warning:
            self._curr_icon = "orange", None
        else:
            self._curr_icon = "blue", None

        if nrunning and self.blink_task is None:
            self.trayicon.setIcon(self.icons[self._curr_icon])

            self.blink_task = asyncio.ensure_future(
                self._blink_task()
            )

            def clear_task(fut):
                self.blink_task = None
                fut.result()

            self.blink_task.add_done_callback(
                clear_task
            )
        elif not nrunning and self.blink_task is not None:
            self.blink_task.cancel()
            self.blink_task = None

        if not nrunning:
            self.trayicon.setIcon(self.icons[self._curr_icon])

    @asyncio.coroutine
    def _get_connection(self):
        while True:
            try:
                endpoint = yield from asyncio.wait_for(
                    cuteborg.scheduler.protocol.test_and_get_socket(
                        self.loop,
                        self.logger,
                        self.socket_path,
                    ),
                    timeout=5
                )
                break
            except (asyncio.TimeoutError,
                    FileNotFoundError,
                    ConnectionError) as exc:
                msg = str(exc)
                if isinstance(exc, asyncio.TimeoutError):
                    msg = "timeout"
                self.logger.error("failed to connect to scheduler at %r: %s",
                                  str(self.socket_path),
                                  msg)
            self._update_jobs({})
            self._set_error_condition(
                ErrorCondition.SCHEDULER_NOT_RUNNING,
                suppress_notify=not self._connected
            )
            yield from asyncio.sleep(10)

        self._connected = True

        response, data = yield from endpoint.request(
            cuteborg.scheduler.protocol.ToplevelCommand.STATUS,
        )

        self._update_jobs(data["jobs"])

        return endpoint

    @asyncio.coroutine
    def _update_loop(self, stop_future):
        self._curr_icon = None, "error"
        self._set_error_condition(
            ErrorCondition.SCHEDULER_NOT_RUNNING,
            suppress_notify=not self._connected
        )
        self._connected = False
        self.trayicon.setIcon(self.icons[self._curr_icon])

        self.logger.debug("attempting connection ...")

        conn_future = asyncio.ensure_future(self._get_connection())
        done, pending = yield from asyncio.wait(
            [
                stop_future,
                conn_future,
            ],
            return_when=asyncio.FIRST_COMPLETED
        )

        for fut in pending:
            if fut != stop_future:
                fut.cancel()

        if conn_future in done:
            endpoint = conn_future.result()
        else:
            return

        self.logger.debug("connection established!")

        while True:
            try:
                conn_future = asyncio.ensure_future(
                    endpoint.poll_start()
                )

                done, pending = yield from asyncio.wait(
                    [
                        conn_future,
                        stop_future,
                    ],
                    return_when=asyncio.FIRST_COMPLETED
                )

                for fut in pending:
                    if fut != stop_future:
                        fut.cancel()

                if conn_future in done:
                    response, data = conn_future.result()
                else:
                    return

                self._update_jobs(data["jobs"])
                self._update_trayicon_from_jobs()
                self._update_error_conditions_from_jobs()

                conn_future = asyncio.ensure_future(
                    endpoint.poll_finalise()
                )

                done, pending = yield from asyncio.wait(
                    [
                        conn_future,
                        stop_future,
                    ],
                    return_when=asyncio.FIRST_COMPLETED
                )

                for fut in pending:
                    if fut != stop_future:
                        fut.cancel()

                if conn_future in done:
                    conn_future.result()
                else:
                    return
            except ConnectionError as exc:
                self.logger.error(
                    "connection to scheduler broke: %s",
                    exc
                )
                break

    @asyncio.coroutine
    def run(self):
        self.trayicon.show()
        stop_future = asyncio.ensure_future(self.stop_event.wait())

        yield from try_start()

        while not self._quit:
            yield from self._update_loop(stop_future)

        if self._stop:
            yield from try_stop()
