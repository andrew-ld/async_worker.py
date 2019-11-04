# async_worker.py
Hi!, this software permit to schedule loop oriented asynchronous tasks

# example
    import asyncio
    
    from multiprocessing import cpu_count
    from async_worker import AsyncTaskScheduler, AsyncTask
    
    
    class ExampleTask(AsyncTask):
        _pause: int
    
        def setup(self, pause: int):
            self._pause = pause
    
        async def process(self) -> int:
            print(id(self), self._pause)
            # the return value is the time to sleep
            # if return False the task is cancelled
            return self._pause
    
    
    async def main():
        scheduler = AsyncTaskScheduler()
    
        for i in range(1, 5):
            task = ExampleTask()
            task.setup(i)
            await scheduler.submit(task)
    
        await asyncio.gather(*(scheduler.loop() for _ in range(cpu_count())))
    
    
    if __name__ == "__main__":
        _loop = asyncio.get_event_loop()
        _loop.run_until_complete(main())
