"""Public-API benchmark adapters loaded only inside child processes."""

from tools.queue_bench.adapters.base import AdapterRequest, AdapterResult

__all__ = ("AdapterRequest", "AdapterResult")
