import os
import sys
import warnings
from importlib import metadata
from pathlib import Path
from typing import Any

try:
    from sqlalchemy.exc import SAWarning
except ModuleNotFoundError:
    SAWarning = None
else:
    warnings.filterwarnings(
        "ignore",
        message="Unmanaged access of declarative attribute .* from non-mapped class QueueTaskModelMixin",
        category=SAWarning,
    )

current_path = Path(__file__).parent.parent.resolve()
sys.path.append(str(current_path))

project = "litestar-queues"
version = metadata.version("litestar-queues")
copyright = "2026, Litestar-Org"
author = "Litestar-Org"
release = os.getenv("_LITESTAR_QUEUES_DOCS_BUILD_VERSION", version.rsplit(".")[0])
queues_light_style = "tools.sphinx_ext.pygments_styles.LitestarQueuesLightStyle"
queues_dark_style = "tools.sphinx_ext.pygments_styles.LitestarQueuesDarkStyle"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.githubpages",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinx_design",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "litestar": ("https://docs.litestar.dev/latest/", None),
}

napoleon_google_docstring = True
autoclass_content = "class"
autodoc_default_options = {"special-members": "__init__", "show-inheritance": True, "members": True}
autodoc_member_order = "bysource"
autodoc_typehints_format = "short"
autosectionlabel_prefix_document = True
suppress_warnings = ["app.add_node", "ref.python", "autodoc", "duplicate"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "shibuya"
html_static_path = ["_static"]
html_js_files = ["versioning.js"]
html_css_files = ["custom.css", "style.css"]
html_title = "Litestar Queues"
html_short_title = "Queues"
html_favicon = "_static/favicon.svg"
html_context = {
    "source_type": "github",
    "source_user": "cofin",
    "source_repo": "litestar-queues",
    "current_version": "latest",
    "version": release,
}
html_sidebars = {"**": []}
pygments_style = queues_light_style
pygments_dark_style = queues_dark_style

try:
    from shibuya._pygments import ShibuyaPygmentsBridge
except ModuleNotFoundError:
    pass
else:
    # Avoid plugin-style scanning so SQLSpec's similarly named docs helper module
    # cannot collide with this repo's top-level tools package.
    ShibuyaPygmentsBridge.dark_style_name = queues_dark_style

__all__ = ("setup",)


html_theme_options: "dict[str, Any]" = {
    "github_url": "https://github.com/cofin/litestar-queues",
    "discord_url": "https://discord.gg/litestar-919193495116337154",
    "discussion_url": "https://github.com/cofin/litestar-queues/discussions",
    "navigation_with_keys": True,
    "globaltoc_expand_depth": 0,
    "accent_color": "amber",
    "light_logo": "_static/logo-icon.svg",
    "dark_logo": "_static/logo-icon.svg",
    "nav_links": [
        {
            "title": "Docs",
            "children": [
                {
                    "title": "Get Started",
                    "url": "getting_started/index",
                    "summary": "Install the package and enqueue the first task.",
                },
                {
                    "title": "Usage",
                    "url": "usage/index",
                    "summary": "Configure tasks, workers, schedules, events, and testing.",
                },
                {
                    "title": "Backends",
                    "url": "usage/backends",
                    "summary": "Choose queue persistence and execution integrations.",
                },
                {
                    "title": "API Reference",
                    "url": "reference/index",
                    "summary": "Browse the public queue, backend, worker, and event APIs.",
                },
            ],
        },
        {
            "title": "Developers",
            "children": [
                {
                    "title": "Contributing",
                    "url": "contributing/index",
                    "summary": "Set up the repo and follow project conventions.",
                },
                {
                    "title": "Testing",
                    "url": "contributing/testing",
                    "summary": "Run unit, integration, backend, and docs checks.",
                },
            ],
        },
        {
            "title": "Help",
            "children": [
                {
                    "title": "Discord",
                    "url": "https://discord.gg/litestar-919193495116337154",
                    "summary": "Ask questions in the Litestar community.",
                },
                {
                    "title": "GitHub Discussions",
                    "url": "https://github.com/cofin/litestar-queues/discussions",
                    "summary": "Discuss usage, design, and support topics on GitHub.",
                },
            ],
        },
    ],
}


def setup(app: Any) -> "dict[str, bool]":
    app.setup_extension("shibuya")
    return {"parallel_read_safe": True, "parallel_write_safe": True}
