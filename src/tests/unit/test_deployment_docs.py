"""The deployment guides have to be copy-ready and honest.

Deployment docs fail in a way ordinary prose does not: someone pastes the
commands, the topology comes up, and the constraint that was left out surfaces
weeks later as tasks that silently never run. So the parts that cannot be
discovered by trying it -- the ceilings Google enforces, the delivery guarantee,
what scale-to-zero does and does not stop costing -- are asserted here rather
than trusted to survive editing.
"""

from pathlib import Path

DEPLOYMENT = Path("docs/usage/deployment")


def _cloud_tasks() -> "str":
    """The Cloud Tasks deployment guide.

    Returns:
        The guide's text.
    """
    return (DEPLOYMENT / "cloud-tasks.rst").read_text(encoding="utf-8")


def test_every_topology_is_named_where_a_reader_chooses_between_them() -> "None":
    """Four ways to run this, and picking wrong is expensive to undo."""
    chooser = (DEPLOYMENT / "cloud-run.rst").read_text(encoding="utf-8")

    for marker in ("In-process worker", "External dispatcher", "Cloud Tasks", "Eventarc"):
        assert marker in chooser
    assert "cloud-tasks" in chooser


def test_eventarc_is_named_as_future_work_and_nothing_more() -> "None":
    """It is not part of this backend, and a reader must not plan around it."""
    chooser = (DEPLOYMENT / "cloud-run.rst").read_text(encoding="utf-8")

    # The disclaimer has to be inside the same table row, where someone
    # comparing topologies reads it, not in a footnote further down.
    row = chooser[chooser.index("Eventarc") :].split("* - ")[0]
    assert "Not implemented" in row


def test_the_hard_ceilings_are_stated_with_their_numbers() -> "None":
    """Each of these turns a working queue into a silently broken one."""
    guide = _cloud_tasks()

    assert "1800" in guide
    assert "15 seconds" in guide
    assert "30 days" in guide


def test_the_delivery_guarantee_is_stated_rather_than_implied() -> "None":
    """A reader who assumes exactly-once will write a task that double-charges."""
    guide = _cloud_tasks()

    assert "at-least-once" in guide
    assert "idempotent" in guide


def test_the_cost_model_is_dated_and_does_not_pretend_to_be_free() -> "None":
    """Scale-to-zero removes the always-on instance, not the database behind it."""
    guide = _cloud_tasks()

    assert "2026" in guide
    assert "two operations" in guide or "one create" in guide
    assert "database" in guide


def test_the_guide_distinguishes_cloud_tasks_from_what_it_is_confused_with() -> "None":
    """All four are 'the Google thing that runs work later' and none are substitutes."""
    guide = _cloud_tasks()

    for neighbour in ("Cloud Scheduler", "Pub/Sub", "Eventarc", "Cloud Run Jobs"):
        assert neighbour in guide


def test_the_guide_never_tells_the_package_to_provision_anything() -> "None":
    """Queues, services, and IAM are the operator's, created before any record exists."""
    guide = _cloud_tasks()

    assert "gcloud tasks queues create" in guide
    assert "roles/run.invoker" in guide
    assert "The package never creates" in guide


def test_the_guide_says_there_is_no_emulator() -> "None":
    """Every adopter looks for one, and the answer shapes how they test."""
    guide = _cloud_tasks()

    assert "emulator" in guide


def test_recovery_and_repair_are_documented_where_they_are_needed() -> "None":
    """Nothing polls these records, so a lost delivery has no other way back."""
    guide = _cloud_tasks()

    assert "maintenance" in guide
    assert "repair" in guide


def test_no_credential_is_embedded_in_any_deployment_command() -> "None":
    """Copy-ready must not mean copy-a-secret."""
    for guide in DEPLOYMENT.glob("*.rst"):
        text = guide.read_text(encoding="utf-8")
        assert "-----BEGIN" not in text
        assert "GOOGLE_APPLICATION_CREDENTIALS=/" not in text
