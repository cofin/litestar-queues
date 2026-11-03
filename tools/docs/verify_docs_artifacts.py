"""Verification gate for documentation build artifacts, budgets, and coverage."""

import argparse
import posixpath
import re
import subprocess
import sys
import zlib
from pathlib import Path
from urllib.parse import unquote

import msgspec

from litestar_queues.cli import queues_group
from tools.docs.generate_cli_docs import main as cli_docs_main
from tools.docs.postbuild import DEFAULT_DOCS_DIR, DEFAULT_SITE_DIR, LEGACY_REDIRECTS, run_postbuild
from tools.docs.scan_public_docstrings import scan_package

__all__ = (
    "BANNED_PHRASES",
    "MAX_REFERENCE_HTML_BYTES",
    "VerificationFinding",
    "VerificationReport",
    "main",
    "verify_docs_build",
    "verify_site",
)

REPO_ROOT = Path(__file__).resolve().parents[2]
MAX_REFERENCE_HTML_BYTES = 220 * 1024

BANNED_PHRASES: tuple[str, ...] = (
    ":class:",
    ":meth:",
    ":func:",
    ":attr:",
    ":doc:",
    ":ref:",
    ".. code-block::",
    ".. automodule::",
    "--8<--",
    "--8 < --",
)

WARNING_PATTERN = re.compile(r"\b(WARNING|ERROR)\b")
OFFLINE_INVENTORY_WARNING_RE = re.compile(r"Could not load inventory|objects\.inv", re.IGNORECASE)
EMPTY_ATTRIBUTE_ROW_RE = re.compile(
    r'<tr class="doc-section-item">\s*<td><code>[^<]+</code></td>\s*<td>\s*</td>\s*<td>\s*<div class="doc-md-description">\s*</div>\s*</td>\s*</tr>'
)
ID_ATTR_RE = re.compile(r'\bid="([^"]+)"')
HREF_ATTR_RE = re.compile(r'\bhref="([^"]+)"')


class VerificationFinding(msgspec.Struct, frozen=True, gc=False):
    """A single documentation artifact verification failure."""

    path: str
    rule: str
    message: str


class VerificationReport(msgspec.Struct, frozen=True, gc=False):
    """Structured report returned by `verify_site`."""

    findings: tuple[VerificationFinding, ...]

    @property
    def ok(self) -> bool:
        """Return True when zero findings were recorded."""
        return len(self.findings) == 0


def verify_site(site_dir: Path = DEFAULT_SITE_DIR, docs_dir: Path = DEFAULT_DOCS_DIR) -> VerificationReport:
    """Verify an existing built `site/` directory for required artifacts, banned phrases, budgets, and links.

    Args:
        site_dir: Built site directory.
        docs_dir: Documentation source directory.

    Returns:
        Structured `VerificationReport`.
    """
    findings: list[VerificationFinding] = []
    findings.extend(_verify_site_artifacts(site_dir))
    findings.extend(_verify_autodoc_coverage(docs_dir))
    findings.extend(_verify_reference_budgets(site_dir))
    ids_by_rel_path, html_text_by_rel_path, content_findings = _verify_html_contents(site_dir)
    findings.extend(content_findings)
    findings.extend(_verify_internal_links(site_dir, ids_by_rel_path, html_text_by_rel_path))
    return VerificationReport(findings=tuple(findings))


def verify_docs_build(
    *,
    site_dir: Path = DEFAULT_SITE_DIR,
    docs_dir: Path = DEFAULT_DOCS_DIR,
    run_build: bool = True,
    strict_warnings: bool = True,
) -> list[str]:
    """Verify all documentation build artifacts, coverage gates, and size budgets."""
    errors: list[str] = []

    if run_build:
        docstring_findings = scan_package()
        if docstring_findings:
            errors.append(f"scan_public_docstrings reported {len(docstring_findings)} finding(s).")
        errors.extend(_run_zensical_build(strict_warnings=strict_warnings))
        if errors:
            return errors
        run_postbuild(site_dir=site_dir, docs_dir=docs_dir)

    report = verify_site(site_dir=site_dir, docs_dir=docs_dir)
    errors.extend(f"{finding.path} [{finding.rule}]: {finding.message}" for finding in report.findings)
    return errors


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for `verify_docs_artifacts.py`."""
    parser = argparse.ArgumentParser(description="Verify documentation build and artifacts.")
    parser.add_argument(
        "--strict-warnings",
        action="store_true",
        default=False,
        help="Fail if the documentation build emits any WARNING or ERROR log lines.",
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        default=False,
        help="Verify existing site/ artifacts without running zensical build.",
    )
    args = parser.parse_args(argv)

    errors = verify_docs_build(run_build=not args.no_build, strict_warnings=args.strict_warnings)
    if errors:
        for err in errors:
            sys.stderr.write(f"ERROR: {err}\n")
        return 1

    sys.stdout.write("Documentation verification passed with 0 errors.\n")
    return 0


def _run_zensical_build(strict_warnings: bool) -> list[str]:
    errors: list[str] = []
    proc = subprocess.run(
        [sys.executable, "-m", "zensical", "build", "--clean"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    combined_output = f"{proc.stdout}\n{proc.stderr}"
    if proc.returncode != 0:
        errors.append(f"zensical build failed with exit code {proc.returncode}:\n{combined_output}")
        return errors

    if strict_warnings:
        errors.extend(
            f"Documentation build warning/error detected: {line.strip()}"
            for line in combined_output.splitlines()
            if WARNING_PATTERN.search(line) and not OFFLINE_INVENTORY_WARNING_RE.search(line)
        )
    return errors


def _verify_site_artifacts(site_dir: Path) -> list[VerificationFinding]:
    findings: list[VerificationFinding] = [
        VerificationFinding(
            path=required_artifact,
            rule="missing-artifact",
            message=f"Missing required site artifact: {required_artifact}",
        )
        for required_artifact in ("objects.inv", "llms.txt", "llms-full.txt", ".nojekyll")
        if not (site_dir / required_artifact).exists()
    ]

    inv_path = site_dir / "objects.inv"
    if inv_path.exists():
        inv_bytes = inv_path.read_bytes()
        if not inv_bytes.startswith(b"# Sphinx inventory version 2\n"):
            findings.append(
                VerificationFinding(
                    path="objects.inv",
                    rule="invalid-objects-inv",
                    message="site/objects.inv does not have a valid Sphinx v2 header.",
                )
            )
        else:
            lines = inv_bytes.splitlines(keepends=True)
            decompressed = zlib.decompress(b"".join(lines[4:])).decode("utf-8")
            if "litestar_queues.config.QueueConfig" not in decompressed:
                findings.append(
                    VerificationFinding(
                        path="objects.inv",
                        rule="incomplete-objects-inv",
                        message="site/objects.inv is missing litestar_queues.config.QueueConfig.",
                    )
                )

    llms_full = site_dir / "llms-full.txt"
    if llms_full.exists() and "--8<--" in llms_full.read_text(encoding="utf-8"):
        findings.append(
            VerificationFinding(
                path="llms-full.txt",
                rule="unexpanded-snippet",
                message="site/llms-full.txt contains unexpanded --8<-- snippet directives.",
            )
        )

    if site_dir.exists() and inv_path.exists():
        findings.extend(
            VerificationFinding(
                path=f"{legacy_route}.html",
                rule="missing-legacy-redirect",
                message=f"Missing flat legacy redirect stub: {legacy_route}.html",
            )
            for legacy_route in LEGACY_REDIRECTS
            if not (site_dir / f"{legacy_route}.html").exists()
        )
    return findings


def _verify_autodoc_coverage(docs_dir: Path) -> list[VerificationFinding]:
    findings: list[VerificationFinding] = []
    cli_ref_md = docs_dir / "reference" / "cli.md"
    if cli_ref_md.exists():
        if cli_docs_main(["--output", str(cli_ref_md), "--check"]) != 0:
            findings.append(
                VerificationFinding(
                    path="docs/reference/cli.md",
                    rule="cli-docs-drift",
                    message="docs/reference/cli.md is out of sync with generate_cli_docs.py --check.",
                )
            )
        cli_text = cli_ref_md.read_text(encoding="utf-8")
        findings.extend(
            VerificationFinding(
                path="docs/reference/cli.md",
                rule="missing-cli-subcommand",
                message=f"CLI command 'litestar queues {cmd_name}' is missing from docs/reference/cli.md",
            )
            for cmd_name in sorted(queues_group.commands)
            if f"`litestar queues {cmd_name}`" not in cli_text
        )
    return findings


def _verify_reference_budgets(site_dir: Path) -> list[VerificationFinding]:
    findings: list[VerificationFinding] = []
    site_ref_dir = site_dir / "reference"
    if site_ref_dir.exists():
        for html_file in sorted(site_ref_dir.rglob("*.html")):
            size_bytes = html_file.stat().st_size
            if size_bytes > MAX_REFERENCE_HTML_BYTES:
                rel_path = html_file.relative_to(site_dir).as_posix()
                findings.append(
                    VerificationFinding(
                        path=rel_path,
                        rule="reference-html-budget",
                        message=f"{rel_path} is {size_bytes} bytes (exceeds {MAX_REFERENCE_HTML_BYTES} byte budget)",
                    )
                )
    return findings


def _verify_html_contents(site_dir: Path) -> tuple[dict[str, set[str]], dict[str, str], list[VerificationFinding]]:
    findings: list[VerificationFinding] = []
    ids_by_rel_path: dict[str, set[str]] = {}
    html_text_by_rel_path: dict[str, str] = {}
    if not site_dir.exists():
        return ids_by_rel_path, html_text_by_rel_path, findings

    for html_file in sorted(site_dir.rglob("*.html")):
        rel_path = html_file.relative_to(site_dir).as_posix()
        text = html_file.read_text(encoding="utf-8")
        html_text_by_rel_path[rel_path] = text
        ids_by_rel_path[rel_path] = {unquote(m) for m in ID_ATTR_RE.findall(text)}

        if '<meta http-equiv="refresh"' in text:
            continue

        findings.extend(
            VerificationFinding(
                path=rel_path, rule="banned-phrase", message=f"Built HTML contains banned phrase {phrase!r}."
            )
            for phrase in BANNED_PHRASES
            if phrase in text
        )

        if EMPTY_ATTRIBUTE_ROW_RE.search(text):
            findings.append(
                VerificationFinding(
                    path=rel_path,
                    rule="empty-attribute-row",
                    message="Built HTML contains an empty mkdocstrings attribute table row.",
                )
            )

    return ids_by_rel_path, html_text_by_rel_path, findings


def _verify_internal_links(
    site_dir: Path, ids_by_rel_path: dict[str, set[str]], html_text_by_rel_path: dict[str, str]
) -> list[VerificationFinding]:
    findings: list[VerificationFinding] = []
    for rel_path, text in html_text_by_rel_path.items():
        if '<meta http-equiv="refresh"' in text:
            continue
        cur_dir = posixpath.dirname(rel_path)
        for href in HREF_ATTR_RE.findall(text):
            finding = _check_single_href(
                site_dir=site_dir,
                rel_path=rel_path,
                cur_dir=cur_dir,
                href=href,
                ids_by_rel_path=ids_by_rel_path,
                html_text_by_rel_path=html_text_by_rel_path,
            )
            if finding is not None:
                findings.append(finding)
    return findings


def _check_single_href(
    *,
    site_dir: Path,
    rel_path: str,
    cur_dir: str,
    href: str,
    ids_by_rel_path: dict[str, set[str]],
    html_text_by_rel_path: dict[str, str],
) -> VerificationFinding | None:
    if not href or href.startswith(("http://", "https://", "mailto:", "tel:", "javascript:", "data:")):
        return None
    path_part, _sep, frag_part = href.partition("#")
    frag = unquote(frag_part)
    if not path_part:
        if frag and frag not in ids_by_rel_path.get(rel_path, set()):
            return VerificationFinding(
                path=rel_path, rule="broken-anchor", message=f"Broken same-page anchor #{frag} in {rel_path}."
            )
        return None

    resolved = (
        posixpath.normpath(path_part.lstrip("/"))
        if path_part.startswith("/")
        else posixpath.normpath(posixpath.join(cur_dir, path_part))
    )
    if path_part.endswith("/") and not resolved.endswith("/"):
        resolved += "/"

    target_rel = _resolve_site_target(site_dir, resolved, ids_by_rel_path)
    if target_rel is None:
        if not (site_dir / resolved.rstrip("/")).exists():
            return VerificationFinding(
                path=rel_path, rule="broken-link", message=f"Broken internal link {href!r} in {rel_path}."
            )
        return None

    if frag and target_rel in ids_by_rel_path:
        target_text = html_text_by_rel_path.get(target_rel, "")
        if '<meta http-equiv="refresh"' not in target_text and frag not in ids_by_rel_path[target_rel]:
            return VerificationFinding(
                path=rel_path,
                rule="broken-anchor",
                message=f"Broken cross-page anchor {href!r} (missing #{frag} in {target_rel}).",
            )
    return None


def _resolve_site_target(site_dir: Path, resolved: str, ids_by_rel_path: dict[str, set[str]]) -> str | None:
    stripped = resolved.rstrip("/")
    candidates = (f"{stripped}/index.html" if stripped else "index.html", stripped, f"{stripped}.html")
    for candidate in candidates:
        if candidate in ids_by_rel_path or (site_dir / candidate).is_file():
            return candidate
    return None


if __name__ == "__main__":
    raise SystemExit(main())
