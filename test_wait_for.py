import asyncio
async def inner():
    try:
        await asyncio.sleep(5)
    except asyncio.CancelledError:
        print("Inner cancelled!")
        raise
async def run():
    await asyncio.wait_for(inner(), timeout=None)
async def main():
    t = asyncio.create_task(run())
    await asyncio.sleep(0.1)
    t.cancel()
    try:
        await t
    except asyncio.CancelledError:
        pass
asyncio.run(main())
