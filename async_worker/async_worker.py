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
import functools
import typing
import time
import operator


class AsyncTask(abc.ABC):
    _next: int
    _locked: bool
    _scheduler: 'AsyncTaskScheduler'

    __slots__ = [
        "_next",
        "_locked",
        "_scheduler",
        "_ready"
    ]

    def __init__(self):
        self._next = 0
        self._locked = False
        self._ready = False

    def set_scheduler(self, scheduler: 'AsyncTaskScheduler'):
        self._scheduler = scheduler

    async def future(self, task: 'AsyncTask'):
        await self._scheduler.submit(task)

    async def _process(self) -> typing.Union[bool, int]:
        result = await self.process()

        if result is False:
            return False

        if isinstance(result, int):
            return result * 1e9

        raise ValueError

    @abc.abstractmethod
    async def process(self) -> typing.Union[bool, int]:
        raise NotImplementedError

    async def _setup(self) -> bool:
        result = await self.setup()
        self._ready = True

        if result is None:
            return True

        return result

    async def setup(self) -> bool:
        pass

    def set_next(self, _next: int):
        self._next = time.time_ns() + _next

    def get_next(self) -> int:
        return self._next

    def get_delay(self) -> int:
        return self.get_next() - time.time_ns()

    def is_ready(self) -> bool:
        return self._ready

    def lock(self):
        self._locked = True

    def unlock(self):
        self._locked = False

    def is_locked(self):
        return self._locked


class OneLoopAsyncTask(AsyncTask, abc.ABC):
    async def _process(self) -> bool:
        await self.process()
        return False

    @abc.abstractmethod
    async def process(self) -> typing.NoReturn:
        raise NotImplementedError


class AsyncTaskDelay:
    _task: asyncio.Task
    _delay_end: int

    __slots__ = [
        "_task",
        "_delay_end",
    ]

    def __init__(self):
        self._task = asyncio.Future()
        self._delay_end = 0

    async def sleep(self, _time) -> bool:
        self._delay_end = _time + time.time_ns()
        self._task = asyncio.ensure_future(asyncio.sleep(_time // 1e9))

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


class SchedulerConfig:
    imprecise_delay: int
    skippable_delay: int
    max_fast_submit_tasks: int

    __slots__ = [
        "imprecise_delay",
        "skippable_delay",
        "max_fast_submit_tasks"
    ]

    def __init__(self,
        imprecise_delay: int = 2 * 1e8,
        skippable_delay: int = 3 * 1e8,
        max_fast_submit_tasks: int = 50
    ):
        self.imprecise_delay = imprecise_delay
        self.skippable_delay = skippable_delay
        self.max_fast_submit_tasks = max_fast_submit_tasks


def on_complete(
    the_task: AsyncTask,
    the_queue: list,
    the_lock: AsyncMultipleEvent,
    the_future: asyncio.Future,
):
    result = the_future.result()

    if result is False:
        the_queue.remove(the_task)

    else:
        the_task.unlock()
        the_task.set_next(result)
        the_lock.unlock_first()


class AsyncTaskScheduler:
    _queue: typing.List[AsyncTask]
    _wait_enqueue: AsyncMultipleEvent
    _wait_unlock: AsyncMultipleEvent
    _sleep_tasks: typing.List[AsyncTaskDelay]
    _config: SchedulerConfig

    __slots__ = [
        "_queue",
        "_sleep_tasks",
        "_wait_enqueue",
        "_wait_unlock",
        "_config"
    ]

    def __init__(self, config: SchedulerConfig = SchedulerConfig()):
        self._queue = []
        self._sleep_tasks = []
        self._config = config

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
        task: AsyncTask

        sleeper = AsyncTaskDelay()
        self._sleep_tasks.append(sleeper)

        while True:
            if not self._queue:
                await self._wait_enqueue.lock()
                await asyncio.sleep(0)

            while self._queue:
                runnable_tasks = [*filter(lambda x: not x.is_locked(), self._queue)]

                if not runnable_tasks:
                    await self._wait_unlock.lock()
                    continue

                submittable = [*filter(lambda x: x.get_delay() <= self._config.imprecise_delay, runnable_tasks)]

                if submittable:

                    for task in submittable:
                        task.lock()
                        task.set_scheduler(self)

                    while submittable:
                        futures = []

                        for task in submittable[:self._config.max_fast_submit_tasks]:
                            on_done = functools.partial(on_complete, task, self._queue, self._wait_unlock)
                            future = asyncio.ensure_future(task._process() if task.is_ready() else task._setup())

                            future.add_done_callback(on_done)
                            futures.append(future)

                        await asyncio.gather(*futures)
                        submittable = submittable[self._config.max_fast_submit_tasks:]

                    continue

                task, delay = min(
                    (
                        (task, task.get_next())
                        for task in runnable_tasks
                    ),

                    key=operator.itemgetter(1)
                )

                delay -= time.time_ns()
                task.lock()

                if delay > self._config.skippable_delay and not await sleeper.sleep(delay):
                    task.unlock()
                    continue

                task.set_scheduler(self)
                next_delay = await task._process()

                if next_delay is False:
                    self._queue.remove(task)

                else:
                    task.set_next(next_delay)
                    task.unlock()
                    self._wait_unlock.unlock_first()
