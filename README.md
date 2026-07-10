# Litestar Queues

[![PyPI](https://img.shields.io/pypi/v/litestar-queues)](https://pypi.org/project/litestar-queues/)
[![Python](https://img.shields.io/pypi/pyversions/litestar-queues)](https://pypi.org/project/litestar-queues/)
[![License](https://img.shields.io/pypi/l/litestar-queues)](https://github.com/cofin/litestar-queues/blob/main/LICENSE)
[![CI](https://github.com/cofin/litestar-queues/actions/workflows/ci.yml/badge.svg)](https://github.com/cofin/litestar-queues/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-cofin.github.io-blue)](https://cofin.github.io/litestar-queues/)

Litestar Queues lets a Litestar application persist work, run it in a worker,
and inspect the result. Use it for work that should outlive the request that
started it: sending email, importing files, refreshing reports, or calling a
slow service.

## Quickstart

Install the package:

```bash
pip install litestar-queues
```

Create `app.py`:

```python
from litestar import Litestar, post
from litestar.di import NamedDependency

from litestar_queues import QueueConfig, QueuePlugin, QueueService, task


@task("accounts.sync", queue="accounts", timeout=30)
async def sync_account(account_id: str) -> dict[str, str]:
    return {"account_id": account_id, "status": "synced"}


@post("/accounts/{account_id:str}/sync")
async def create_sync_job(
    account_id: str,
    queue_service: NamedDependency[QueueService],
) -> dict[str, str]:
    result = await queue_service.enqueue(sync_account, account_id)
    return {"task_id": str(result.id), "status": result.status or "pending"}


app = Litestar(
    route_handlers=[create_sync_job],
    plugins=[QueuePlugin(config=QueueConfig())],
)
```

Run the application:

```bash
LITESTAR_APP=app:app litestar run --reload
```

Enqueue a task:

```bash
curl -X POST http://127.0.0.1:8000/accounts/acct-123/sync
```

The response contains a task ID and an initial status. The default in-memory
queue and in-app worker are ideal for this first run.

## Production boundary

Choose where tasks are stored separately from where they run. The default
memory backend stores tasks in one Python process. If the web app and worker
run in separate processes, use a shared backend such as SQLSpec, Advanced
Alchemy, Redis, or Valkey. Use standalone workers when the web app and task
workers must scale separately. Cloud Run runs tasks; it does not store them.

## Next steps

- [Start here](https://cofin.github.io/litestar-queues/getting_started/index.html)
- [Understand the model](https://cofin.github.io/litestar-queues/usage/concepts.html)
- [Follow a how-to guide](https://cofin.github.io/litestar-queues/usage/index.html)
- [Choose backends](https://cofin.github.io/litestar-queues/usage/backends.html)
- [Run an example](https://cofin.github.io/litestar-queues/examples/index.html)
- [Browse the API reference](https://cofin.github.io/litestar-queues/reference/index.html)

Litestar Queues supports Python 3.10 through 3.14 and is licensed under MIT.
