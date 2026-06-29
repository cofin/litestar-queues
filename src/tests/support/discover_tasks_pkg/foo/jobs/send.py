from litestar_queues import task


@task("discover.foo.send")
async def send() -> "str":
    return "foo.send"
