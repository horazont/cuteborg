import asyncio
import pathlib
import signal

from datetime import datetime

import babel.dates

import PyQt5.Qt as Qt

import cuteborg.scheduler.protocol

from qtborg import utils

from .ui_main import Ui_StatusWindow


SortRole = Qt.Qt.UserRole


def _load_icon(name):
    for p in __path__:
        filename = pathlib.Path(p) / "data" / name
        return Qt.QIcon(Qt.QPixmap(str(filename)))


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
            if role == SortRole:
                return (job["last_run"] - self.REFERENCE_TIME).total_seconds()
            else:
                return str(job["last_run"])

        elif index.column() == self.COLUMN_NEXT_RUN:
            try:
                wakeup_at = job["blocking"]["struct_info"]["wakeup_at"]
            except KeyError:
                if role == SortRole:
                    return float("inf")
                return "now"
            if role == SortRole:
                return (wakeup_at - self.REFERENCE_TIME).total_seconds()
            else:
                return str(wakeup_at)

        elif index.column() == self.COLUMN_PROGRESS:
            if job["type"] == "create_archive" and "progress" in job:
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

        self.icons = {
            "blue": _load_icon("bullet_blue.png"),
            "orange": _load_icon("bullet_orange.png"),
            "red": _load_icon("bullet_red.png"),
            "": Qt.QIcon(Qt.QPixmap()),
        }
        self.trayicon.setIcon(
            self.icons["blue"],
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
                yield from asyncio.sleep(1)
                self.trayicon.setIcon(self.icons[""])
                yield from asyncio.sleep(1)
                self.trayicon.setIcon(self.icons[self._curr_icon])
        except asyncio.CancelledError:
            self.trayicon.setIcon(self.icons[self._curr_icon])

    def _update_jobs(self, jobs):
        next_scheduling_event = None
        nrunning = 0
        any_failed = False
        any_warning = False

        job_table = []

        now = datetime.utcnow()

        for job in jobs:
            if job["running"]:
                nrunning += 1

            if job["type"] == "create_archive":
                job_table.append(
                    CREATE_ARCHIVE_BASE_TEMPLATE.format(
                        job=job["args"]["job"],
                        repo=job["args"]["repo"],
                        status=(
                            format_job_state_table(job, now)
                            if "progress" not in job
                            else CREATE_ARCHIVE_PROGRESS_TEMPLATE.format(
                                    progress=job["progress"]
                            )
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

            try:
                blocking_info = job["blocking"]
            except KeyError:
                pass
            else:
                try:
                    wakeup_at = blocking_info["struct_info"]["wakeup_at"]
                except KeyError:
                    pass
                else:
                    if next_scheduling_event is not None:
                        next_scheduling_event = min(
                            wakeup_at,
                            next_scheduling_event,
                        )
                    else:
                        next_scheduling_event = wakeup_at

            any_failed = (
                any_failed or
                "error" in job and "blocking" not in job
            )

            any_warning = (
                any_warning or
                "error" in job or
                job.get("blocking", {}).get("reason") == "removable_device"
            )

        self.trayicon.setToolTip(
            "<p>running jobs: {}</p>{}".format(
                nrunning,
                ("<table><tr><th>Task</th><th>Job name</th><th>Repository</th><th>Status</th></tr>{}</table>".format("".join(job_table)))
                if job_table else ""
            )
        )

        if any_failed:
            self._curr_icon = "red"
        elif any_warning:
            self._curr_icon = "orange"
        else:
            self._curr_icon = "blue"

        if nrunning and self.blink_task is None:
            self.blink_task = asyncio.ensure_future(
                self._blink_task()
            )
        elif not nrunning and self.blink_task is not None:
            self.blink_task.cancel()
            self.blink_task = None

        if not nrunning:
            self.trayicon.setIcon(self.icons[self._curr_icon])

        self.jobs_model.update_jobs(jobs)

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
            yield from asyncio.sleep(30)

        response, data = yield from endpoint.request(
            cuteborg.scheduler.protocol.ToplevelCommand.STATUS,
        )

        self._update_jobs(data["jobs"])

        return endpoint

    @asyncio.coroutine
    def run(self):
        self.trayicon.show()
        stop_future = asyncio.ensure_future(self.stop_event.wait())

        while True:
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

                except ConnectionError as exc:
                    self.logger.error(
                        "connection to scheduler broke: %s",
                        exc
                    )
                    break

                self._update_jobs(data["jobs"])

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
