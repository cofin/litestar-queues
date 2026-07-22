"""Contract for the versioned, pickle-free task identity helper."""

import hashlib
import inspect
from typing import Any

import pytest

from litestar_queues._identity import IDENTITY_VERSION, arguments_identity, task_identity
from litestar_queues.exceptions import TaskIdentityError, TaskPayloadTooLargeError


def _sig(func: "Any") -> "inspect.Signature":
    return inspect.signature(func)


def render(report_id: "str", *, fmt: "str" = "pdf") -> "None": ...


def collect(*items: "Any", **options: "Any") -> "None": ...


def with_context(value: "str", _task_context: "Any" = None) -> "None": ...


def test_task_identity_is_versioned_namespaced_and_name_scoped() -> "None":
    key = task_identity("reports.refresh")
    assert key.startswith(f"lq:u:{IDENTITY_VERSION}:task:")
    assert task_identity("reports.refresh") != task_identity("reports.render")


def test_task_identity_matches_canonical_sha256() -> "None":
    expected = hashlib.sha256(b'{"task":"demo","version":"v1"}').hexdigest()
    assert task_identity("demo") == f"lq:u:v1:task:{expected}"


def test_arguments_identity_is_versioned_and_deterministic() -> "None":
    key, size = arguments_identity("t", _sig(render), ("x",), {})
    assert key.startswith("lq:u:v1:arguments:")
    assert arguments_identity("t", _sig(render), ("x",), {}).key == key
    assert size > 0


def test_arguments_identity_matches_canonical_sha256() -> "None":
    def one(a: "int") -> "None": ...

    expected = hashlib.sha256(b'{"arguments":{"a":1},"task":"t","version":"v1"}').hexdigest()
    key, size = arguments_identity("t", _sig(one), (1,), {})
    assert key == f"lq:u:v1:arguments:{expected}"
    assert size == len(b'{"arguments":{"a":1},"task":"t","version":"v1"}')


def test_positional_and_keyword_calls_share_identity() -> "None":
    positional = arguments_identity("reports.render", _sig(render), ("abc",), {}).key
    keyword = arguments_identity("reports.render", _sig(render), (), {"report_id": "abc"}).key
    assert positional == keyword


def test_applied_defaults_share_identity() -> "None":
    implicit = arguments_identity("reports.render", _sig(render), ("abc",), {}).key
    explicit = arguments_identity("reports.render", _sig(render), ("abc",), {"fmt": "pdf"}).key
    assert implicit == explicit


def test_kwargs_ordering_is_irrelevant() -> "None":
    first = arguments_identity("t", _sig(collect), (), {"a": 1, "b": 2}).key
    second = arguments_identity("t", _sig(collect), (), {"b": 2, "a": 1}).key
    assert first == second


def test_task_name_namespaces_identity() -> "None":
    left = arguments_identity("tasks.left", _sig(render), ("abc",), {}).key
    right = arguments_identity("tasks.right", _sig(render), ("abc",), {}).key
    assert left != right


def test_tuple_and_list_arguments_normalize_to_same_wire_identity() -> "None":
    tuple_key = arguments_identity("t", _sig(collect), (("a", "b"),), {}).key
    list_key = arguments_identity("t", _sig(collect), (["a", "b"],), {}).key
    assert tuple_key == list_key


def test_task_context_is_excluded_from_identity() -> "None":
    baseline = arguments_identity("t", _sig(collect), ("x",), {}).key
    with_var_context = arguments_identity("t", _sig(collect), ("x",), {"_task_context": object()}).key
    assert baseline == with_var_context

    plain = arguments_identity("t", _sig(with_context), ("x",), {}).key
    injected = arguments_identity("t", _sig(with_context), ("x",), {"_task_context": object()}).key
    assert plain == injected


def test_non_finite_float_arguments_are_rejected() -> "None":
    with pytest.raises(TaskIdentityError):
        arguments_identity("t", _sig(collect), (float("nan"),), {})
    with pytest.raises(TaskIdentityError):
        arguments_identity("t", _sig(collect), (float("inf"),), {})


def test_non_json_arguments_are_rejected() -> "None":
    with pytest.raises(TaskIdentityError):
        arguments_identity("t", _sig(collect), (object(),), {})


def test_payload_size_guard_reports_actual_and_max() -> "None":
    _, size = arguments_identity("t", _sig(collect), ("x" * 1024,), {})
    with pytest.raises(TaskPayloadTooLargeError) as excinfo:
        arguments_identity("t", _sig(collect), ("x" * 1024,), {}, max_payload_bytes=size - 1)
    assert excinfo.value.actual_bytes == size
    assert excinfo.value.max_bytes == size - 1
    # Exactly at the limit is allowed.
    assert arguments_identity("t", _sig(collect), ("x" * 1024,), {}, max_payload_bytes=size).payload_bytes == size
