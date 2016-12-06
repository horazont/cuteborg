import asyncio
import logging

from datetime import datetime

if not hasattr(asyncio, "ensure_future"):
    asyncio.ensure_future = asyncio.async


MAX_SLACK = 60


logger = logging.getLogger(__name__)


class WallClockTimeSleepImpl:
    def __init__(self, max_slack=MAX_SLACK):
        super().__init__()
        self._futures = []
        self._wakeup_event = asyncio.Event()
        self._main_task = None
        self._max_slack = max_slack

    @property
    def max_slack(self):
        return self._max_slack

    @max_slack.setter
    def max_slack(self, new_value):
        self._max_slack = new_value
        self._wakeup_event.set()

    @asyncio.coroutine
    def cancel_and_wait(self):
        if self._main_task is None:
            return

        for _, fut in self._futures:
            fut.cancel()
        self._wakeup_event.set()
        yield from self._main_task

    def _main_done(self, task):
        pass

    def _cleanup(self):
        self._futures = sorted(
            (
                (ts, fut)
                for ts, fut in self._futures
                if not fut.done()
            ),
            key=lambda x: x[0],
        )

    @asyncio.coroutine
    def _main(self):
        # self._cleanup always returns None
        while self._cleanup() or self._futures:
            now = datetime.utcnow()
            next_event, _ = self._futures[0]
            wait_time = (next_event - now).total_seconds()
            if wait_time >= 0:
                wait_time = min(wait_time, self._max_slack)
                logger.debug("sleeping for %.1f seconds",
                             wait_time)
                try:
                    yield from asyncio.wait_for(
                        self._wakeup_event.wait(),
                        timeout=wait_time
                    )
                except asyncio.TimeoutError:
                    logger.debug("timeout passed")
                else:
                    self._wakeup_event.clear()
                    logger.debug("woke up from event")
                continue

            for i, (timestamp, fut) in enumerate(self._futures):
                if timestamp > now:
                    break
                fut.set_result(None)

    def _notify(self):
        # start task if its not running
        if self._main_task is None or self._main_task.done():
            self._main_task = asyncio.ensure_future(
                self._main()
            )
            self._main_task.add_done_callback(self._main_done)
            self._wakeup_event.clear()
        else:
            self._wakeup_event.set()

    def sleep_until(self, dt):
        fut = asyncio.Future()
        self._futures.append((dt, fut))
        self._notify()
        return fut
