import json
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.benchmark_queue_transports import (  # noqa: E402
    SCHEMA_VERSION,
    benchmark_claim_batches,
    benchmark_event_batches,
    benchmark_idle_polling,
    benchmark_native_notifications,
    build_report,
)


def _variants(results: "list[dict[str, Any]]", scenario: "str") -> "dict[str, dict[str, Any]]":
    return {str(result["variant"]): result for result in results if result["scenario"] == scenario}


def test_idle_polling_counts_fixed_and_adaptive_backend_calls() -> "None":
    results = _variants(benchmark_idle_polling(duration=10.0, interval=1.0, maximum_interval=4.0), "idle_polling")

    assert results["fixed"]["operation_counts"] == {"backend_calls": 10}
    assert results["adaptive"]["operation_counts"] == {"backend_calls": 4}


def test_native_notifications_count_reconciliation_calls_and_latency() -> "None":
    [result] = benchmark_native_notifications(task_count=100, reconcile_every=25)

    assert result["operation_counts"] == {"claim_calls": 100, "notification_calls": 100, "reconciliation_calls": 4}
    assert result["p50_seconds"] is not None
    assert result["p95_seconds"] is not None
    assert result["p99_seconds"] is not None


def test_native_claim_and_event_batches_reduce_controlled_operations() -> "None":
    claim_results = _variants(benchmark_claim_batches(task_count=100, batch_size=16), "claim_batching")
    event_results = _variants(benchmark_event_batches(event_count=100, batch_size=16), "event_batching")

    assert claim_results["sequential"]["operation_counts"] == {"backend_operations": 100}
    assert claim_results["native_batch"]["operation_counts"] == {"backend_operations": 7}
    assert event_results["sequential"]["operation_counts"] == {"flushes": 100, "transport_calls": 100}
    assert event_results["publish_many"]["operation_counts"] == {"flushes": 7, "transport_calls": 7}
    assert event_results["publish_many"]["parameters"]["maximum_flush_size"] == 16


def test_report_schema_is_versioned_and_json_serializable(tmp_path: "Path") -> "None":
    report = build_report(task_count=32, batch_size=8, idle_duration=4.0)
    output = tmp_path / "transport-benchmark.json"
    output.write_text(json.dumps(report), encoding="utf-8")
    restored = json.loads(output.read_text(encoding="utf-8"))

    assert restored["schema_version"] == SCHEMA_VERSION
    assert set(restored["environment"]) >= {"cpu_count", "git_dirty", "git_sha", "implementation", "platform", "python"}
    assert {result["scenario"] for result in restored["results"]} == {
        "claim_batching",
        "event_batching",
        "idle_polling",
        "native_notification",
    }
    for result in restored["results"]:
        assert set(result) == {
            "backend",
            "count",
            "duration_seconds",
            "operation_counts",
            "p50_seconds",
            "p95_seconds",
            "p99_seconds",
            "parameters",
            "scenario",
            "throughput_per_second",
            "variant",
        }
