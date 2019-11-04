# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# For more information, please refer to <http://unlicense.org/>

import asyncio
from multiprocessing import cpu_count

from async_worker import AsyncTaskScheduler, AsyncTask, OneLoopTask


class Test1(AsyncTask):
    _pause: int

    def setup(self, pause: int):
        self._pause = pause

    async def process(self) -> int:
        print(id(self), self.__class__.__name__, self._pause)
        return self._pause


class Test2(AsyncTask):
    _after: int
    _bootstrapped = False

    def setup(self, after: int):
        self._after = after

    async def process(self) -> int:
        if not self._bootstrapped:
            self._bootstrapped = True
            return self._after

        for i in range(50):
            task = Test3()
            task.setup(i)

            await self.future(task)

        return False


class Test3(OneLoopTask):
    _i: int

    async def process(self):
        await asyncio.sleep(self._i)
        print("sleep", self._i)

    def setup(self, i: int):
        self._i = i


async def main():
    scheduler = AsyncTaskScheduler()

    for i in range(6, 12):
        task = Test1()
        task.setup(i)

        await scheduler.submit(task)

    task = Test2()
    task.setup(6)
    await scheduler.submit(task)

    await asyncio.gather(*(scheduler.loop() for _ in range(cpu_count())))

if __name__ == "__main__":
    _loop = asyncio.get_event_loop()
    _loop.run_until_complete(main())
