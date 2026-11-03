"""Unit tests for the tools/docs/ documentation build and verification pipeline."""

import sys
import zlib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.docs.generate_cli_docs import main as cli_docs_main  # noqa: E402
from tools.docs.generate_cli_docs import render_cli_reference  # noqa: E402
from tools.docs.postbuild import (  # noqa: E402
    LEGACY_REDIRECTS,
    build_objects_inv,
    compact_site_html,
    write_legacy_redirects,
    write_llms_files,
)
from tools.docs.scan_public_docstrings import scan_package  # noqa: E402
from tools.docs.verify_docs_artifacts import (  # noqa: E402
    BANNED_PHRASES,
    MAX_REFERENCE_HTML_BYTES,
    VerificationFinding,
    VerificationReport,
    verify_site,
)


def test_scan_public_docstrings_clean_file(tmp_path: Path) -> None:
    """Verify scan_package reports zero findings on a well-documented module."""
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "clean.py").write_text(
        '"""Clean module docstring."""\n\n'
        "from dataclasses import dataclass\n\n"
        "@dataclass(slots=True)\n"
        "class CleanModel:\n"
        '    """Clean model docstring."""\n\n'
        "    name: str\n"
        '    """Field docstring."""\n\n'
        "    def run(self) -> str:\n"
        '        """Run the model."""\n'
        "        return self.name\n",
        encoding="utf-8",
    )
    assert scan_package(pkg) == []


def test_scan_public_docstrings_detects_violations(tmp_path: Path) -> None:
    """Verify scan_package reports missing docstrings, missing field docstrings, and rST syntax."""
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "dirty.py").write_text(
        "from dataclasses import dataclass\n\n"
        "@dataclass(slots=True)\n"
        "class DirtyModel:\n"
        '    """Model referencing :class:`OtherModel`.\n\n'
        "    Attributes:\n"
        "        name: Duplicate section.\n"
        '    """\n\n'
        "    name: str\n\n"
        "    def Undocumented(self) -> None:\n"
        "        return None\n",
        encoding="utf-8",
    )
    findings = scan_package(pkg)
    rules = {finding.rule for finding in findings}
    assert "missing-module-docstring" in rules
    assert "rst-directive-in-docstring" in rules
    assert "hybrid-duplicate-attributes" in rules
    assert "missing-field-docstring" in rules
    assert "missing-function-docstring" in rules


@pytest.mark.parametrize(
    "subcommand", ["run", "run-consumer", "run-task", "run-maintenance", "status", "scheduler-health"]
)
def test_generate_cli_docs_documents_all_subcommands(subcommand: str) -> None:
    """Verify render_cli_reference includes every litestar queues subcommand."""
    rendered = render_cli_reference()
    assert f"`litestar queues {subcommand}`" in rendered


def test_generate_cli_docs_check_mode(tmp_path: Path) -> None:
    """Verify generate_cli_docs.py --check detects drift and succeeds when up to date."""
    out_file = tmp_path / "cli.md"
    assert cli_docs_main(["--output", str(out_file), "--check"]) == 1
    assert cli_docs_main(["--output", str(out_file)]) == 0
    assert cli_docs_main(["--output", str(out_file), "--check"]) == 0
    out_file.write_text("drifted\n", encoding="utf-8")
    assert cli_docs_main(["--output", str(out_file), "--check"]) == 1


def test_compact_site_html_preserves_pre_and_textarea(tmp_path: Path) -> None:
    """Verify compact_site_html strips indentation outside pre/textarea while keeping pre/textarea intact."""
    site = tmp_path / "site"
    site.mkdir()
    html_path = site / "index.html"
    html_path.write_text(
        "<html>\n"
        "    <body>\n"
        "        <p>Hello</p>\n\n\n"
        "        <pre><code>    def foo():\n        return 1\n</code></pre>\n"
        "        <textarea>    keep indent\n\n    line2</textarea>\n"
        "    </body>\n"
        "</html>\n",
        encoding="utf-8",
    )
    saved = compact_site_html(site)
    assert saved > 0
    compacted = html_path.read_text(encoding="utf-8")
    assert "    def foo():\n        return 1\n" in compacted
    assert "    keep indent\n\n    line2" in compacted
    assert "\n    <body>" not in compacted


def test_write_legacy_redirects_covers_all_44_moved_routes(tmp_path: Path) -> None:
    """Verify LEGACY_REDIRECTS contains all 44 moved legacy routes and emits hash-preserving stubs."""
    assert len(LEGACY_REDIRECTS) == 44
    site = tmp_path / "site"
    (site / "getting-started" / "quickstart").mkdir(parents=True)
    (site / "getting-started" / "quickstart" / "index.html").write_text("<html></html>", encoding="utf-8")

    written = write_legacy_redirects(site)
    assert written >= 44

    stub = (site / "getting_started" / "quickstart.html").read_text(encoding="utf-8")
    assert "window.location.hash" in stub
    assert "getting-started/quickstart/" in stub

    flat_built_stub = (site / "getting-started" / "quickstart.html").read_text(encoding="utf-8")
    assert "window.location.hash" in flat_built_stub


def test_write_llms_files_expands_snippets(tmp_path: Path) -> None:
    """Verify write_llms_files generates llms.txt and llms-full.txt with expanded snippets."""
    docs = tmp_path / "docs"
    site = tmp_path / "site"
    docs.mkdir()
    site.mkdir()
    (docs / "index.md").write_text("# Litestar Queues\n\nWelcome to queues.\n", encoding="utf-8")
    llms_txt, llms_full = write_llms_files(site_dir=site, docs_dir=docs)
    assert llms_txt.is_file()
    assert llms_full.is_file()
    assert "Litestar Queues" in llms_txt.read_text(encoding="utf-8")
    assert "Welcome to queues." in llms_full.read_text(encoding="utf-8")


def _seed_valid_site(site: Path, docs: Path) -> None:
    """Populate a minimal valid site and docs tree for verify_site tests."""
    site.mkdir(parents=True, exist_ok=True)
    docs.mkdir(parents=True, exist_ok=True)
    (docs / "index.md").write_text("# Litestar Queues\n\nOverview.\n", encoding="utf-8")
    (site / "index.html").write_text(
        '<!doctype html><html><body><h1 id="top">Top</h1><a href="#top">Self</a></body></html>', encoding="utf-8"
    )
    (site / ".nojekyll").write_text("", encoding="utf-8")
    build_objects_inv(site_dir=site, docs_dir=docs)
    write_llms_files(site_dir=site, docs_dir=docs)
    write_legacy_redirects(site_dir=site)


def test_verify_site_passes_on_clean_site(tmp_path: Path) -> None:
    """Verify verify_site reports zero findings on a valid site tree."""
    site = tmp_path / "site"
    docs = tmp_path / "docs"
    _seed_valid_site(site, docs)
    report = verify_site(site_dir=site, docs_dir=docs)
    assert isinstance(report, VerificationReport)
    assert report.ok
    assert report.findings == ()


@pytest.mark.parametrize("phrase", list(BANNED_PHRASES))
def test_verify_site_detects_banned_phrases(tmp_path: Path, phrase: str) -> None:
    """Verify verify_site catches raw Sphinx directives and unexpanded snippet markers in built HTML."""
    site = tmp_path / "site"
    docs = tmp_path / "docs"
    _seed_valid_site(site, docs)
    (site / "index.html").write_text(
        f"<!doctype html><html><body><p>Leaked directive: {phrase}</p></body></html>", encoding="utf-8"
    )
    report = verify_site(site_dir=site, docs_dir=docs)
    assert not report.ok
    assert any(isinstance(f, VerificationFinding) and f.rule == "banned-phrase" for f in report.findings)


def test_verify_site_detects_oversized_reference_html_and_broken_anchors(tmp_path: Path) -> None:
    """Verify verify_site flags reference HTML exceeding 220 KB, empty attribute rows, and broken anchors."""
    site = tmp_path / "site"
    docs = tmp_path / "docs"
    _seed_valid_site(site, docs)

    ref_dir = site / "reference" / "backends"
    ref_dir.mkdir(parents=True)
    padding = "a" * (MAX_REFERENCE_HTML_BYTES + 128)
    (ref_dir / "index.html").write_text(
        "<!doctype html><html><body>"
        '<tr class="doc-section-item"><td><code>_x</code></td><td></td><td><div class="doc-md-description"></div></td></tr>'
        f'<a href="#nonexistent-anchor">Broken</a><p>{padding}</p></body></html>',
        encoding="utf-8",
    )

    report = verify_site(site_dir=site, docs_dir=docs)
    rules = {finding.rule for finding in report.findings}
    assert "reference-html-budget" in rules
    assert "empty-attribute-row" in rules
    assert "broken-anchor" in rules


def test_build_objects_inv_contains_valid_zlib_payload(tmp_path: Path) -> None:
    """Verify build_objects_inv writes a valid Sphinx v2 objects.inv inventory."""
    site = tmp_path / "site"
    docs = tmp_path / "docs"
    site.mkdir()
    (docs / "reference").mkdir(parents=True)
    (docs / "reference" / "plugin-and-config.md").write_text(
        "# Plugin\n\n::: litestar_queues.config.QueueConfig\n", encoding="utf-8"
    )
    inv_path = build_objects_inv(site_dir=site, docs_dir=docs)
    raw = inv_path.read_bytes()
    assert raw.startswith(b"# Sphinx inventory version 2\n")
    lines = raw.splitlines(keepends=True)
    payload = zlib.decompress(b"".join(lines[4:])).decode("utf-8")
    assert "litestar_queues.config.QueueConfig" in payload
    assert "litestar_queues.QueueConfig" in payload
