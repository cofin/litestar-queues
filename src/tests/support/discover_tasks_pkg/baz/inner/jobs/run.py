from litestar_queues import task


@task("discover.baz.inner.run")
async def run() -> "str":
    return "baz.inner.run"
