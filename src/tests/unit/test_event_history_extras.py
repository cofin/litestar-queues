from litestar_queues.events import EventHistoryExtraColumn, validate_event_history_extra_columns


def test_extra_column_is_public_from_events() -> "None":
    (column,) = validate_event_history_extra_columns(
        [EventHistoryExtraColumn(name="tenant", source="tenant_id")]
    )
    assert (column.name, column.source, column.indexed) == ("tenant", "tenant_id", False)

def test_sqlspec_no_longer_re_exports_the_declaration() -> "None":
    import litestar_queues.backends.sqlspec as sqlspec_backend

    assert not hasattr(sqlspec_backend, "EventHistoryExtraColumn")
