"""Post-build pipeline for documentation artifacts and legacy redirects."""

import importlib
import importlib.metadata
import inspect
import posixpath
import re
import sys
import zlib
from pathlib import Path

import msgspec

import litestar_queues

__all__ = (
    "DEFAULT_DOCS_DIR",
    "DEFAULT_SITE_DIR",
    "LEGACY_REDIRECTS",
    "SITE_URL",
    "InventoryEntry",
    "build_llms_files",
    "build_objects_inv",
    "compact_site_html",
    "main",
    "run_postbuild",
    "write_legacy_redirects",
    "write_llms_files",
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SITE_DIR = REPO_ROOT / "site"
DEFAULT_DOCS_DIR = REPO_ROOT / "docs"
SITE_URL = "https://cofin.github.io/litestar-queues"

SNIPPET_INCLUDE_RE = re.compile(r'^(\s*)--8<--\s+"([^"]+)"\s*$', re.MULTILINE)
DIRECTIVE_RE = re.compile(r"^:::\s+(litestar_queues(?:\.[A-Za-z0-9_]+)+)\s*$", re.MULTILINE)
PRESERVED_BLOCK_RE = re.compile(r"(<(?:pre|textarea)\b[^>]*>.*?</(?:pre|textarea)>)", re.DOTALL | re.IGNORECASE)
LEADING_INDENT_RE = re.compile(r"\n[ \t]+")
MULTI_BLANK_LINES_RE = re.compile(r"\n\s*\n+")
TRAILING_WS_RE = re.compile(r"[ \t]+\n")

LEGACY_REDIRECTS: dict[str, str] = {
    "getting_started/index": "getting-started/",
    "getting_started/installation": "getting-started/installation/",
    "getting_started/quickstart": "getting-started/quickstart/",
    "getting_started/introduction": "getting-started/concepts/",
    "usage/index": "learn/",
    "usage/concepts": "getting-started/concepts/",
    "usage/tasks": "learn/tasks/",
    "usage/task-options": "learn/task-options/",
    "usage/results": "learn/results/",
    "usage/failures-and-cancellation": "learn/failures-and-cancellation/",
    "usage/schedules": "learn/schedules/",
    "usage/dependency-resolver": "learn/dependency-injection/",
    "usage/background-tasks": "learn/background-responses/",
    "usage/workers": "learn/workers/",
    "usage/worker-wakeups": "backends/",
    "usage/configuration": "learn/configuration/",
    "usage/runtime-namespace": "learn/runtime-namespace/",
    "usage/testing": "learn/testing/",
    "usage/events": "events/",
    "usage/event-streams": "events/streams/",
    "usage/event-history": "events/history/",
    "usage/event-history-extending": "events/history-extending/",
    "usage/event-standalone": "events/standalone/",
    "usage/event-testing": "events/testing/",
    "usage/observability": "events/observability/",
    "usage/cli": "execution/cli/",
    "usage/maintenance": "execution/maintenance/",
    "usage/migration": "backends/migration/",
    "usage/backends/index": "backends/",
    "usage/backends/sqlspec": "backends/sqlspec/",
    "usage/backends/advanced-alchemy": "backends/advanced-alchemy/",
    "usage/backends/redis-valkey": "backends/redis-valkey/",
    "usage/deployment/index": "execution/",
    "usage/deployment/cloud-tasks": "execution/cloud-tasks/",
    "usage/deployment/cloud-run": "execution/cloud-run/",
    "usage/deployment/broker-transports": "execution/broker-transports/",
    "usage/deployment/pubsub": "execution/pubsub/",
    "usage/deployment/sqs": "execution/sqs/",
    "usage/deployment/rabbitmq": "execution/rabbitmq/",
    "usage/deployment/kafka": "execution/kafka/",
    "examples/index": "architecture/examples/",
    "contributing/index": "reference/contributing/",
    "contributing/documentation": "reference/contributing/",
    "reference/exceptions": "reference/observability/",
}


class InventoryEntry(msgspec.Struct, frozen=True, gc=False):
    """Single Sphinx objects.inv v2 entry."""

    name: str
    domain_role: str
    priority: int
    uri: str
    dispname: str = "-"


def compact_site_html(site_dir: Path = DEFAULT_SITE_DIR) -> int:
    """Compact redundant blank lines and leading/trailing whitespace outside `<pre>` and `<textarea>` blocks.

    Args:
        site_dir: Built site directory.

    Returns:
        Total UTF-8 bytes saved across all `.html` files.
    """
    total_saved = 0
    for html_file in sorted(site_dir.rglob("*.html")):
        raw = html_file.read_text(encoding="utf-8")
        parts = PRESERVED_BLOCK_RE.split(raw)
        compacted_parts: list[str] = []
        for idx, segment in enumerate(parts):
            if idx % 2 == 1:
                compacted_parts.append(segment)
            else:
                cleaned = TRAILING_WS_RE.sub("\n", segment)
                cleaned = LEADING_INDENT_RE.sub("\n", cleaned)
                cleaned = MULTI_BLANK_LINES_RE.sub("\n", cleaned)
                compacted_parts.append(cleaned)
        compacted = "".join(compacted_parts)
        saved = len(raw.encode("utf-8")) - len(compacted.encode("utf-8"))
        if saved > 0:
            html_file.write_text(compacted, encoding="utf-8")
            total_saved += saved
    return total_saved


def build_objects_inv(site_dir: Path = DEFAULT_SITE_DIR, docs_dir: Path = DEFAULT_DOCS_DIR) -> Path:
    """Write a Sphinx v2 `objects.inv` file to `site/objects.inv`.

    Args:
        site_dir: Built site directory.
        docs_dir: Documentation source directory.

    Returns:
        Path to `site/objects.inv`.
    """
    entries = _collect_inventory_entries(docs_dir)
    pkg_version = importlib.metadata.version("litestar-queues")
    header = (
        f"# Sphinx inventory version 2\n"
        f"# Project: litestar-queues\n"
        f"# Version: {pkg_version}\n"
        f"# The remainder of this file is compressed using zlib.\n"
    ).encode()
    payload_lines = [f"{e.name} {e.domain_role} {e.priority} {e.uri} {e.dispname}\n" for e in entries]
    compressed = zlib.compress("".join(payload_lines).encode("utf-8"), level=9)
    inv_path = site_dir / "objects.inv"
    inv_path.write_bytes(header + compressed)
    return inv_path


def write_llms_files(site_dir: Path = DEFAULT_SITE_DIR, docs_dir: Path = DEFAULT_DOCS_DIR) -> tuple[Path, Path]:
    """Generate `site/llms.txt` and `site/llms-full.txt` with expanded snippets.

    Args:
        site_dir: Built site directory.
        docs_dir: Documentation source directory.

    Returns:
        Tuple of `(llms_txt_path, llms_full_path)`.
    """
    llms_index_lines = [
        "# Litestar Queues",
        "",
        "> Task queues, workers, schedules, realtime event streams, and storage/execution backend integrations for Litestar.",
        "",
        "## Documentation Index",
        "",
    ]

    full_sections: list[str] = ["# Litestar Queues — Complete Documentation Corpus", ""]

    md_files = [path for path in sorted(docs_dir.rglob("*.md")) if path.relative_to(docs_dir).parts[0] != "examples"]
    for md_path in md_files:
        rel_md = md_path.relative_to(docs_dir).as_posix()
        raw_md = md_path.read_text(encoding="utf-8")
        expanded_md = _expand_snippets(raw_md)
        title_match = re.search(r"^#\s+(.+)$", expanded_md, re.MULTILINE) or re.search(
            r"^title:\s*(.+)$", expanded_md, re.MULTILINE
        )
        title = title_match.group(1).strip().strip("\"'") if title_match else rel_md
        if rel_md == "index.md":
            url_path = ""
        elif rel_md.endswith("/index.md"):
            url_path = rel_md.removesuffix("index.md")
        else:
            url_path = rel_md.removesuffix(".md") + "/"
        llms_index_lines.append(f"- [{title}]({SITE_URL}/{url_path})")
        full_sections.append(f"---\n\n<!-- Source: docs/{rel_md} -->\n\n{expanded_md.strip()}\n")

    llms_txt_path = site_dir / "llms.txt"
    llms_full_path = site_dir / "llms-full.txt"
    llms_txt_path.write_text("\n".join(llms_index_lines).strip() + "\n", encoding="utf-8")
    llms_full_path.write_text("\n".join(full_sections).strip() + "\n", encoding="utf-8")
    return llms_txt_path, llms_full_path


def build_llms_files(site_dir: Path = DEFAULT_SITE_DIR, docs_dir: Path = DEFAULT_DOCS_DIR) -> tuple[Path, Path]:
    """Alias for `write_llms_files`."""
    return write_llms_files(site_dir=site_dir, docs_dir=docs_dir)


def write_legacy_redirects(site_dir: Path = DEFAULT_SITE_DIR) -> int:
    """Write flat `.html` and directory `/index.html` redirect stubs for all retired and directory routes.

    Args:
        site_dir: Built site directory.

    Returns:
        Number of redirect stub files written.
    """
    written = 0

    for index_html in sorted(site_dir.rglob("index.html")):
        rel_dir = index_html.parent.relative_to(site_dir).as_posix()
        if rel_dir in ("", "."):
            continue
        flat_rel = f"{rel_dir}.html"
        flat_path = site_dir / flat_rel
        if not flat_path.exists():
            flat_path.parent.mkdir(parents=True, exist_ok=True)
            flat_path.write_text(_render_redirect_html(flat_rel, f"{rel_dir}/"), encoding="utf-8")
            written += 1

    for legacy_route, target_rel in sorted(LEGACY_REDIRECTS.items()):
        flat_rel = f"{legacy_route}.html"
        flat_path = site_dir / flat_rel
        if not flat_path.exists():
            flat_path.parent.mkdir(parents=True, exist_ok=True)
            flat_path.write_text(_render_redirect_html(flat_rel, target_rel), encoding="utf-8")
            written += 1

        if not legacy_route.endswith("/index") and legacy_route != "index":
            dir_rel = f"{legacy_route}/index.html"
        else:
            dir_rel = f"{legacy_route}.html"
        dir_path = site_dir / dir_rel
        if not dir_path.exists():
            dir_path.parent.mkdir(parents=True, exist_ok=True)
            dir_path.write_text(_render_redirect_html(dir_rel, target_rel), encoding="utf-8")
            written += 1

    return written


def run_postbuild(site_dir: Path = DEFAULT_SITE_DIR, docs_dir: Path = DEFAULT_DOCS_DIR) -> None:
    """Execute the full post-build artifact and redirect pipeline."""
    site_dir.mkdir(parents=True, exist_ok=True)
    compact_site_html(site_dir=site_dir)
    (site_dir / ".nojekyll").write_text("", encoding="utf-8")
    build_objects_inv(site_dir=site_dir, docs_dir=docs_dir)
    write_llms_files(site_dir=site_dir, docs_dir=docs_dir)
    write_legacy_redirects(site_dir=site_dir)


def main(argv: list[str] | None = None) -> int:
    """Run `postbuild.py` from the CLI."""
    _ = argv
    run_postbuild()
    sys.stdout.write("Postbuild completed: objects.inv, llms.txt, llms-full.txt, .nojekyll, and legacy redirects.\n")
    return 0


def _resolve_dotted(dotted_path: str) -> object | None:
    parts = dotted_path.split(".")
    for idx in range(len(parts), 0, -1):
        mod_name = ".".join(parts[:idx])
        try:
            obj: object = importlib.import_module(mod_name)
        except ImportError:
            continue
        for attr in parts[idx:]:
            obj = getattr(obj, attr, None)
            if obj is None:
                return None
        return obj
    return None


def _classify_role(obj: object) -> str:
    if inspect.ismodule(obj):
        return "py:module"
    if inspect.isclass(obj):
        if issubclass(obj, BaseException):
            return "py:exception"
        return "py:class"
    if inspect.isfunction(obj) or inspect.isbuiltin(obj) or callable(obj):
        return "py:function"
    return "py:data"


def _collect_inventory_entries(docs_dir: Path) -> list[InventoryEntry]:
    entries: dict[str, InventoryEntry] = {}
    ref_pages = (
        ("plugin-and-config.md", "reference/plugin-and-config/"),
        ("tasks-and-models.md", "reference/tasks-and-models/"),
        ("backends.md", "reference/backends/"),
        ("execution-and-workers.md", "reference/execution-and-workers/"),
        ("events.md", "reference/events/"),
        ("observability.md", "reference/observability/"),
        ("cli.md", "reference/cli/"),
    )

    default_targets: tuple[tuple[str, str], ...] = (
        ("litestar_queues.plugin.QueuePlugin", "reference/plugin-and-config/"),
        ("litestar_queues.config.QueueConfig", "reference/plugin-and-config/"),
        ("litestar_queues.config.WorkerConfig", "reference/plugin-and-config/"),
        ("litestar_queues.service.QueueService", "reference/plugin-and-config/"),
        ("litestar_queues.task.task", "reference/tasks-and-models/"),
        ("litestar_queues.task.TaskDefinition", "reference/tasks-and-models/"),
        ("litestar_queues.task.ScheduleConfig", "reference/tasks-and-models/"),
        ("litestar_queues.models.QueuedTaskRecord", "reference/tasks-and-models/"),
        ("litestar_queues.backends.base.BaseQueueBackend", "reference/backends/"),
        ("litestar_queues.backends.sqlspec.config.SQLSpecBackendConfig", "reference/backends/"),
        ("litestar_queues.execution.base.BaseExecutionBackend", "reference/execution-and-workers/"),
        ("litestar_queues.events.config.QueueEventsConfig", "reference/events/"),
        ("litestar_queues.observability.ObservabilityConfig", "reference/observability/"),
    )

    for dotted, page_uri in default_targets:
        obj = _resolve_dotted(dotted)
        role = _classify_role(obj) if obj is not None else "py:class"
        entries[dotted] = InventoryEntry(name=dotted, domain_role=role, priority=1, uri=f"{page_uri}#{dotted}")
        short_name = dotted.rsplit(".", 1)[-1]
        if short_name in litestar_queues.__all__:
            root_dotted = f"litestar_queues.{short_name}"
            entries.setdefault(
                root_dotted, InventoryEntry(name=root_dotted, domain_role=role, priority=1, uri=f"{page_uri}#{dotted}")
            )

    for filename, page_uri in ref_pages:
        page_path = docs_dir / "reference" / filename
        if not page_path.exists():
            continue
        content = page_path.read_text(encoding="utf-8")
        for dotted in DIRECTIVE_RE.findall(content):
            obj = _resolve_dotted(dotted)
            role = _classify_role(obj) if obj is not None else "py:class"
            entries[dotted] = InventoryEntry(name=dotted, domain_role=role, priority=1, uri=f"{page_uri}#{dotted}")
            short_name = dotted.rsplit(".", 1)[-1]
            if short_name in litestar_queues.__all__:
                root_dotted = f"litestar_queues.{short_name}"
                entries.setdefault(
                    root_dotted,
                    InventoryEntry(name=root_dotted, domain_role=role, priority=1, uri=f"{page_uri}#{dotted}"),
                )

    return sorted(entries.values(), key=lambda item: item.name)


def _expand_snippets(markdown_text: str) -> str:
    def _replace(match: re.Match[str]) -> str:
        indent = match.group(1)
        rel_path = match.group(2)
        target = REPO_ROOT / rel_path
        if not target.exists():
            return f"{indent}[Included file: {rel_path}]"
        if target.suffix == ".html":
            return f"{indent}[Interactive Architecture Diagram: {target.name}]"
        body = target.read_text(encoding="utf-8").rstrip()
        return "\n".join(f"{indent}{line}" if line else "" for line in body.splitlines())

    return SNIPPET_INCLUDE_RE.sub(_replace, markdown_text)


def _render_redirect_html(from_html_rel: str, target_rel: str) -> str:
    from_dir = posixpath.dirname(from_html_rel)
    rel_target = posixpath.relpath(target_rel, start=from_dir or ".")
    if target_rel.endswith("/") and not rel_target.endswith("/"):
        rel_target += "/"
    return (
        "<!doctype html>\n"
        '<html lang="en">\n'
        "<head>\n"
        '  <meta charset="utf-8">\n'
        f'  <meta http-equiv="refresh" content="0; url={rel_target}">\n'
        f'  <link rel="canonical" href="{SITE_URL}/{target_rel.lstrip("/")}">\n'
        "  <title>Redirecting...</title>\n"
        "  <script>\n"
        f'    window.location.replace("{rel_target}" + (window.location.hash || ""));\n'
        "  </script>\n"
        "</head>\n"
        "<body>\n"
        f'  <p>Redirecting to <a href="{rel_target}">{rel_target}</a>...</p>\n'
        "</body>\n"
        "</html>\n"
    )


if __name__ == "__main__":
    raise SystemExit(main())
