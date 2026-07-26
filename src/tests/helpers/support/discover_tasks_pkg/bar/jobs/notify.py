from litestar_queues import task


@task("discover.bar.notify")
async def notify() -> "str":
    return "bar.notify"
