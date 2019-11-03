# MIT License
#
# Copyright (c) [2019] [andrew-ld]
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import asyncio
import abc
import typing
import time
import operator


class AsyncTask(abc.ABC):
    _next: int
    _locked: bool

    __slots__ = [
        "_next",
        "_locked"
    ]

    def __init__(self):
        self._next = 0
        self._locked = False

    @abc.abstractmethod
    async def process(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def setup(self, *args, **kwargs):
        raise NotImplementedError

    def set_next(self, _next: int):
        self._next = time.time() + _next

    def get_next(self) -> int:
        return self._next

    def get_delay(self) -> int:
        return self.get_next() - time.time()

    def lock(self):
        self._locked = True

    def unlock(self):
        self._locked = False

    def is_locked(self):
        return self._locked


class AsyncTaskDelay:
    _task: asyncio.Task
    _delay_end: int

    __slots__ = [
        "_task",
        "_delay_end",
    ]

    def __init__(self):
        self._task = asyncio.ensure_future(asyncio.Future())
        self._delay_end = 0

    async def sleep(self, _time) -> bool:
        self._delay_end = _time + time.time()

        self._task = asyncio.ensure_future(
            asyncio.sleep(_time)
        )

        try:

            await self._task

        except asyncio.CancelledError:
            return False

        return True

    def is_sleeping(self) -> bool:
        return not (self._task.done() or self._task.cancelled())

    def cancel(self):
        self._task.cancel()

    def __gt__(self, other: 'AsyncTaskDelay'):
        return self._delay_end > other._delay_end


class AsyncMultipleEvent:
    _events: typing.List[asyncio.Event]

    __slots__ = [
        "_events"
    ]

    def __init__(self):
        self._events = []

    async def lock(self):
        event = asyncio.Event()
        self._events.append(event)
        await event.wait()

    def unlock_first(self):
        if self._events:
            self._events.pop(0).set()


class AsyncTaskScheduler:
    _queue: typing.List[AsyncTask]
    _wait_enqueue: AsyncMultipleEvent
    _wait_unlock: AsyncMultipleEvent
    _sleep_tasks: typing.List[AsyncTaskDelay]

    __slots__ = [
        "_queue",
        "_sleep_tasks",
        "_wait_enqueue",
        "_wait_unlock"
    ]

    def __init__(self):
        self._queue = []
        self._sleep_tasks = []

        self._wait_enqueue = AsyncMultipleEvent()
        self._wait_unlock = AsyncMultipleEvent()

    async def submit(self, task: AsyncTask):
        self._queue.append(task)
        self._wait_enqueue.unlock_first()
        self._wait_unlock.unlock_first()

        cancellable_tasks = [*filter(lambda x: x.is_sleeping(), self._sleep_tasks)]

        if cancellable_tasks:
            max(cancellable_tasks).cancel()

    async def loop(self):
        sleeper = AsyncTaskDelay()
        self._sleep_tasks.append(sleeper)

        while True:
            if not self._queue:
                await self._wait_enqueue.lock()

            while self._queue:
                runnable_tasks = [*filter(lambda x: not x.is_locked(), self._queue)]

                if not runnable_tasks:
                    await self._wait_unlock.lock()
                    continue

                task, delay = min(
                    (
                        (task, task.get_next())
                        for task in runnable_tasks
                    ),

                    key=operator.itemgetter(1)
                )

                delay -= time.time()
                task.lock()

                if delay > 0 and not await sleeper.sleep(delay):
                    task.unlock()
                    continue

                next_delay = await task.process()

                if next_delay is False:
                    self._queue.remove(task)

                else:
                    task.set_next(next_delay)
                    task.unlock()
                    self._wait_unlock.unlock_first()
