"""Public structural types for the SQLSpec queue backend.

The supported import location for
:mod:`litestar_queues.backends.sqlspec._typing`. These protocols describe the
subset of SQLSpec configs, sessions, and drivers this backend depends on, so a
store or test can be typed against them without matching a concrete adapter.
"""

from litestar_queues.backends.sqlspec._typing import (
    DatetimeParam,
    SQLSpecConfig,
    SQLSpecDriver,
    SQLSpecManager,
    SQLSpecSessionConfig,
    SQLSpecStoreConfig,
)

__all__ = (
    "DatetimeParam",
    "SQLSpecConfig",
    "SQLSpecDriver",
    "SQLSpecManager",
    "SQLSpecSessionConfig",
    "SQLSpecStoreConfig",
)
