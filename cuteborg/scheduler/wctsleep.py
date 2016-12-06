import asyncio
import threading

from datetime import datetime


_locals = threading.local()

MAX_SLACK = 60


class WallClockTimeSleepImpl:
    def __init__(self):
        super().__init__()
        self._futures = []
        self._wakeup_event = asyncio.Event()
        self._main_task = None

    def _main_done(self, task):
        pass

    def _cleanup(self):
        self._futures = [
            (ts, fut)
            for ts, fut in self._futures
            if not fut.done()
        ]

    @asyncio.coroutine
    def _main(self):
        while self._futures:
            self._futures.sort()
            now = datetime.utcnow()
            next_event, _ = self._futures[0]
            wait_time = (next_event - now).total_seconds()
            if wait_time >= 0:
                wait_time = min(wait_time, MAX_SLACK)
                try:
                    yield from asyncio.wait_for(
                        self._wakeup_event.wait(),
                        timeout=wait_time
                    )
                except asyncio.TimeoutError:
                    pass
                self._cleanup()
                continue

            for i, (timestamp, fut) in enumerate(self._futures):
                if timestamp > now:
                    break
                fut.set_result(None)

            self._cleanup()

    def _notify(self):
        # start task if its not running
        if self._main_task is None or self._main_task.done():
            self._main_task = asyncio.async(
                self._main
            )
            self._main_task.add_done_callback(self._main_done)
        else:
            self._wakeup_event.set()

    def sleep_until(self, dt):
        fut = asyncio.Future()
        self._futures.append((dt, fut))
        self._notify()
        yield from fut


def _get_impl():
    try:
        return _locals.impl
    except AttributeError:
        impl = WallClockTimeSleepImpl()
        _locals.impl = impl
        return impl


@asyncio.coroutine
def sleep_until(dt):
    yield from _get_impl().sleep_until(dt)
