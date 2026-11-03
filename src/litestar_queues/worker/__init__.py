"""Everything that runs queue work, grouped by who owns the process.

- `litestar_queues.worker.worker` is the claim/execute loop itself, the
  only public name here.
- `litestar_queues.worker.runtime` orchestrates one worker start-to-stop
  without knowing how it was launched. Both the CLI and the server child use it.
- `litestar_queues.worker.supervisor` owns the fresh worker child that a
  `litestar run` invocation starts, plus that child's process entry point.
- `litestar_queues.worker.invocation` publishes and verifies the marker that
  tells a process whether its invocation already owns a worker.
- `litestar_queues.worker.heartbeat` keeps claims alive while work runs.

Submodules are imported lazily so importing `Worker` does not pull in
`multiprocessing` or the Litestar CLI.
"""

from litestar_queues.worker.worker import Worker

__all__ = ("Worker",)
