from pathlib import Path

TESTS = Path("src/tests")


def test_shared_test_helpers_use_the_single_helpers_tree() -> None:
    assert not (TESTS / "support").exists()
    assert not (TESTS / "_factories").exists()
    assert not (TESTS / "topology").exists()
    assert not (TESTS / "e2e").exists()
    assert (TESTS / "helpers" / "_timing.py").is_file()
    assert (TESTS / "helpers" / "queue_tasks.py").is_file()
    assert (TESTS / "helpers" / "support" / "server_worker_app.py").is_file()
