import asyncio

async def _is_async_callable():
    pass

async def _invoke(coroutine_func):
    return await coroutine_func()

async def execute_record(coroutine_func):
    return await _invoke(coroutine_func)

async def _run_task_body(coroutine_func):
    try:
        result = await execute_record(coroutine_func)
    except BaseException as exc:
        print(f"Caught exc={exc!r} in _run_task_body")
        raise
    return result

async def _execute_task(coroutine_func):
    return await asyncio.wait_for(_run_task_body(coroutine_func), timeout=None)

async def execute(coroutine_func):
    return await _execute_task(coroutine_func)

async def _execute_claimed(coroutine_func):
    try:
        await execute(coroutine_func)
    except asyncio.CancelledError:
        print("_execute_claimed caught CancelledError! Raising...")
        raise
    except BaseException as exc:
        print(f"_execute_claimed caught BaseException {exc!r}")

async def main():
    started = asyncio.Event()
    
    async def run():
        print("run() started")
        started.set()
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            print("run() caught CancelledError!")
            raise
        print("run() finished successfully!")
        return "done"
        
    t = asyncio.create_task(_execute_claimed(run))
    await started.wait()
    cancelled = t.cancel()
    print(f"t.cancel() returned {cancelled}")
    try:
        await t
    except asyncio.CancelledError:
        print("main caught CancelledError!")

asyncio.run(main())
