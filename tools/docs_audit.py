"""Audit the documentation tree without modifying it."""

import argparse
import ast
import builtins
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DOCS = ROOT / "docs"
DEFAULT_README = ROOT / "README.md"
HEADING_MARKS = '=-~^"`:+*#'
CANONICAL_TERMS = ("queue backend", "execution backend", "worker wakeup", "task event", "event history")
OLD_TRANSPORT_TERMS = ("listen_notify", "listen_notify_durable", "table_queue")
OBSOLETE_HISTORY_TERMS = ("SQLiteQueueEventSink", "standalone SQLite", "queue-events.db")
QUICKSTART_DUPLICATE_THRESHOLD = 4


@dataclass(slots=True)
class PageReport:
    """Collected facts for one reStructuredText page."""

    path: Path
    words: int
    headings: list[tuple[int, str]]
    code_blocks: int
    literalincludes: list[str]
    generated_directives: int
    tutorial_signals: int
    toctree_entries: list[str]
    undefined_names: set[str] = field(default_factory=set)
    prompts: list[str] = field(default_factory=list)

    GUIDE_WORD_BUDGET: ClassVar[int] = 1200

    @property
    def relative_path(self) -> str:
        """Return the page path relative to the documentation root."""
        return self.path.as_posix()


def parse_page(path: Path, docs_root: Path) -> PageReport:
    """Collect deterministic metrics and review prompts for one page.

    Returns:
        The collected page report.
    """
    source = path.read_text(encoding="utf-8")
    headings = _headings(source)
    python_blocks = _python_code_blocks(source)
    includes = re.findall(r"^\.\. literalinclude::\s+(.+?)\s*$", source, flags=re.MULTILINE)
    generated = len(re.findall(r"^\.\. auto(?:module|class|function|method)::", source, flags=re.MULTILINE))
    tutorial_signals = len(re.findall(r"^\.\. (?:code-block|literalinclude)::", source, flags=re.MULTILINE))
    report = PageReport(
        path=path.relative_to(docs_root),
        words=len(re.findall(r"\b[\w'-]+\b", _without_directives(source))),
        headings=headings,
        code_blocks=len(re.findall(r"^\.\. code-block::", source, flags=re.MULTILINE)),
        literalincludes=includes,
        generated_directives=generated,
        tutorial_signals=tutorial_signals,
        toctree_entries=_toctree_entries(source),
    )
    report.undefined_names.update(_undefined_names(python_blocks[0]) if python_blocks else ())
    report.prompts.extend(_review_prompts(report))
    return report


def audit(docs_root: Path, readme: Path) -> int:
    """Print the documentation audit and return a process exit status.

    Returns:
        One when structural errors exist, otherwise zero.
    """
    pages = [parse_page(path, docs_root) for path in sorted(docs_root.rglob("*.rst"))]
    membership = _toctree_membership(pages)
    errors: list[str] = []

    print("Documentation source manifest")
    print("path | words | headings | code | includes | toctree")
    for page in pages:
        member = "yes" if _docname(page.path) in membership or page.path == Path("index.rst") else "no"
        print(
            f"{page.relative_path} | {page.words} | {len(page.headings)} | "
            f"{page.code_blocks} | {len(page.literalincludes)} | {member}"
        )
        if member == "no":
            errors.append(f"page is not in a toctree: {page.relative_path}")
        errors.extend(_literalinclude_errors(page, docs_root))

    print("\nReview prompts")
    prompts = [f"{page.relative_path}: {prompt}" for page in pages for prompt in page.prompts]
    for prompt in prompts or ["none"]:
        print(f"- {prompt}")

    corpus = "\n".join((docs_root / page.path).read_text(encoding="utf-8") for page in pages)
    print("\nVocabulary occurrences")
    for term in CANONICAL_TERMS:
        print(f"- {term}: {_occurrences(corpus, term)}")
    for term in (*OLD_TRANSPORT_TERMS, *OBSOLETE_HISTORY_TERMS):
        print(f"- obsolete {term}: {_occurrences(corpus, term)}")

    print("\nQuickstart duplication")
    print(f"- {_quickstart_duplication(readme, docs_root / 'getting_started' / 'quickstart.rst')}")

    print("\nStructural errors")
    for error in errors or ["none"]:
        print(f"- {error}")
    return 1 if errors else 0


def _headings(source: str) -> list[tuple[int, str]]:
    lines = source.splitlines()
    headings: list[tuple[int, str]] = []
    marks: list[str] = []
    for index in range(len(lines) - 1):
        title = lines[index].strip()
        underline = lines[index + 1].strip()
        if not title or len(underline) < len(title) or len(set(underline)) != 1 or underline[0] not in HEADING_MARKS:
            continue
        mark = underline[0]
        if mark not in marks:
            marks.append(mark)
        headings.append((marks.index(mark) + 1, title))
    return headings


def _python_code_blocks(source: str) -> list[str]:
    lines = source.splitlines()
    blocks: list[str] = []
    index = 0
    while index < len(lines):
        match = re.match(r"^\.\. code-block::\s+python\s*$", lines[index])
        if match is None:
            index += 1
            continue
        index += 1
        while index < len(lines) and (not lines[index].strip() or lines[index].lstrip().startswith(":")):
            index += 1
        body: list[str] = []
        while index < len(lines) and (not lines[index].strip() or lines[index].startswith("   ")):
            body.append(lines[index][3:] if lines[index].startswith("   ") else "")
            index += 1
        blocks.append("\n".join(body).rstrip())
    return blocks


def _undefined_names(block: str) -> set[str]:
    try:
        tree = ast.parse(block)
    except SyntaxError:
        return {"<code block does not parse>"}
    defined = set(dir(builtins))
    loaded: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            defined.add(node.name)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            defined.add(node.name)
            defined.update(
                argument.arg for argument in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs)
            )
        elif isinstance(node, ast.Import):
            defined.update(alias.asname or alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            defined.update(alias.asname or alias.name for alias in node.names)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            defined.add(node.id)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            loaded.add(node.id)
    return loaded - defined


def _review_prompts(report: PageReport) -> list[str]:
    prompts: list[str] = []
    if report.words > report.GUIDE_WORD_BUDGET and report.path.parts[0] != "reference":
        prompts.append(f"review length ({report.words} words; guide threshold {report.GUIDE_WORD_BUDGET})")
    previous = 0
    for level, heading in report.headings:
        if previous and level > previous + 1:
            prompts.append(f"review heading hierarchy near {heading!r}")
        previous = level
        words = heading.split()
        if len(words) > 1 and sum(word[:1].isupper() for word in words[1:]) > len(words[1:]) / 2:
            prompts.append(f"review sentence case for heading {heading!r}")
    if report.generated_directives and report.tutorial_signals:
        prompts.append("page mixes generated API directives with tutorial code")
    if report.undefined_names:
        prompts.append(f"review primary Python block names: {', '.join(sorted(report.undefined_names))}")
    return prompts


def _toctree_entries(source: str) -> list[str]:
    lines = source.splitlines()
    entries: list[str] = []
    index = 0
    while index < len(lines):
        if not re.match(r"^\.\. toctree::\s*$", lines[index]):
            index += 1
            continue
        index += 1
        while index < len(lines) and (not lines[index].strip() or lines[index].startswith("   :")):
            index += 1
        while index < len(lines) and lines[index].startswith("   "):
            value = lines[index].strip()
            if value and not value.startswith(":"):
                entries.append(value)
            index += 1
    return entries


def _toctree_membership(pages: list[PageReport]) -> set[str]:
    membership: set[str] = set()
    for page in pages:
        parent = page.path.parent
        for entry in page.toctree_entries:
            if entry.startswith(("http://", "https://")):
                continue
            target = (parent / entry).with_suffix("")
            membership.add(target.as_posix())
    return membership


def _literalinclude_errors(page: PageReport, docs_root: Path) -> list[str]:
    source_dir = (docs_root / page.path).parent
    return [
        f"missing literalinclude from {page.relative_path}: {include}"
        for include in page.literalincludes
        if not (source_dir / include).resolve().is_file()
    ]


def _quickstart_duplication(readme: Path, quickstart: Path) -> str:
    readme_source = readme.read_text(encoding="utf-8")
    docs_source = quickstart.read_text(encoding="utf-8")
    markers = ("QueuePlugin", "QueueService", "@task", "litestar run", "curl -X POST")
    shared = [marker for marker in markers if marker in readme_source and marker in docs_source]
    if len(shared) >= QUICKSTART_DUPLICATE_THRESHOLD:
        return "README and docs share the canonical quickstart markers; review them together when either changes"
    return "no likely competing quickstart detected"


def _without_directives(source: str) -> str:
    return re.sub(r"^\s*\.\..*$", "", source, flags=re.MULTILINE)


def _docname(path: Path) -> str:
    return path.with_suffix("").as_posix()


def _occurrences(source: str, term: str) -> int:
    return len(re.findall(re.escape(term), source, flags=re.IGNORECASE))


def main() -> int:
    """Run the command-line audit.

    Returns:
        The audit process exit status.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docs", type=Path, default=DEFAULT_DOCS, help="Documentation source directory")
    parser.add_argument("--readme", type=Path, default=DEFAULT_README, help="Project README path")
    args = parser.parse_args()
    return audit(args.docs.resolve(), args.readme.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
