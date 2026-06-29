"""Advanced Alchemy backend configuration tests."""

import pytest

pytest.importorskip("advanced_alchemy")
pytest.importorskip("sqlalchemy")


def test_advanced_alchemy_config_defaults_to_singular_queue_task_model() -> "None":
    """Default Advanced Alchemy config should use the built-in queue task model."""
    from litestar_queues.backends.advanced_alchemy import AdvancedAlchemyBackendConfig, QueueTaskModel

    config = AdvancedAlchemyBackendConfig()

    assert config.model_class is QueueTaskModel
    assert QueueTaskModel.__tablename__ == "litestar_queue_task"
