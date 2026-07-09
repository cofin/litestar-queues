"""Small shared helpers for the copyable realtime examples."""

import os

__all__ = ("standalone_worker_options",)


def standalone_worker_options() -> dict[str, bool]:
    """Return only the explicit override for a separately run queue worker."""
    if os.getenv("LITESTAR_QUEUES_EXAMPLE_IN_APP_WORKER") == "0":
        return {"in_app_worker": False}
    return {}
