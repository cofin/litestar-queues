from litestar_queues import task


@task("support_ping", interval=60)
async def support_ping() -> "str":
    return "pong"
