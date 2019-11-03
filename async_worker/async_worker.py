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

    def __init__(self):
        self._next = 0
        self._locked = False

    @abc.abstractmethod
    async def process(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def setup(self, *args, **kwargs):
        raise NotImplementedError

    def __gt__(self, other: 'AsyncTask') -> bool:
        return self._next > other._next

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
    _task: asyncio.Task = None
    _delay_end: int = 0
    _sleeping: bool = False

    async def sleep(self, _time):
        self._delay_end = _time + time.time()

        self._task = asyncio.ensure_future(
            asyncio.sleep(_time)
        )

        self._sleeping = True

        try:
            await self._task
        except asyncio.CancelledError as e:
            self._sleeping = False
            raise e

        self._sleeping = False

    def is_sleeping(self) -> bool:
        return self._sleeping

    def cancel(self):
        self._task.cancel()

    def __gt__(self, other: 'AsyncTaskDelay'):
        return self._delay_end > other._delay_end


class AsyncMultipleEvent:
    _events: typing.List[asyncio.Event]

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
                        (task, task.get_delay())
                        for task in runnable_tasks
                    ),

                    key=operator.itemgetter(1)
                )

                task.lock()

                if delay > 0:
                    try:
                        await sleeper.sleep(delay)
                    except asyncio.CancelledError:
                        task.unlock()
                        continue

                next_delay = await task.process()

                if next_delay is False:
                    self._queue.remove(task)
                else:
                    task.set_next(next_delay)
                    task.unlock()
                    self._wait_unlock.unlock_first()
