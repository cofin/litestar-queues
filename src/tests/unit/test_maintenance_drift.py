"""Drift gates keeping queue maintenance finite, bounded, and externally scheduled.

These tests fail if maintenance regresses toward a hidden recurring task,
infrastructure auto-provisioning, destructive retention defaults, or a
minute-level recommended cadence.
"""

from pathlib import Path

from litestar_queues import QueueMaintenanceConfig
from litestar_queues.maintenance import PHASE_ORDER

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src" / "litestar_queues"
DOCS = ROOT / "docs"

_MAINTENANCE_SOURCE = (SRC / "maintenance.py").read_text()
_CLI_SOURCE = (SRC / "_cli.py").read_text()
_MAINTENANCE_DOCS = (DOCS / "usage" / "maintenance.rst").read_text()


def test_retention_defaults_are_non_destructive() -> None:
    """Stale recovery and both retention phases stay disabled by default."""
    config = QueueMaintenanceConfig()
    assert config.stale_after is None
    assert config.terminal_retention is None
    assert config.event_retention is None


def test_no_hidden_recurring_maintenance_task() -> None:
    """Maintenance is never registered or enqueued as a recurring queue task."""
    assert "@task" not in _MAINTENANCE_SOURCE
    assert "ScheduleConfig" not in _MAINTENANCE_SOURCE
    assert "initialize_schedules" not in _MAINTENANCE_SOURCE
    assert "enqueue" not in _MAINTENANCE_SOURCE
    # The CLI command is finite: it never starts a worker or a scheduler loop.
    assert "Worker(" not in _CLI_SOURCE.split("def run_maintenance_command")[1].split("def register")[0]


def test_maintenance_does_not_auto_provision_infrastructure() -> None:
    """The service creates no cron records, containers, or schema out of band."""
    for forbidden in ("CronJob", "kubernetes", "pg_cron", "create_cron", "metadata.create_all", "create_all("):
        assert forbidden not in _MAINTENANCE_SOURCE


def test_docs_recommend_infrequent_not_minute_level_cadence() -> None:
    """Docs recommend a six-hour/daily cadence and never a minute-level one."""
    assert "0 */6 * * *" in _MAINTENANCE_DOCS  # six-hour recommendation
    assert "* * * * *" not in _MAINTENANCE_DOCS  # no minute-level cron
    assert "every minute" not in _MAINTENANCE_DOCS.lower()


def test_docs_cover_lease_migration_and_memory_rejection() -> None:
    """The operator guide documents the lease, migrations, and memory rejection."""
    lower = _MAINTENANCE_DOCS.lower()
    assert "lease" in lower
    assert "migration" in lower
    assert "in-memory" in lower
    assert "exit" in lower
    for phase in PHASE_ORDER:
        assert phase in lower
    # Cloud Run is documented as one launch surface, not the core design.
    assert "cloud run" in lower
    assert "not part of the core design" in lower
