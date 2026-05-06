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
        message="Unmanaged access of declarative attribute _sentinel.*",
        category=SAWarning,
    )

current_path = Path(__file__).parent.parent.resolve()
sys.path.append(str(current_path))

project = "litestar-queues"
version = metadata.version("litestar-queues")
copyright = "2026, Litestar-Org"  # noqa: A001
author = "Litestar-Org"
release = os.getenv("_LITESTAR_QUEUES_DOCS_BUILD_VERSION", version.rsplit(".")[0])

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
autodoc_default_options = {
    "special-members": "__init__",
    "show-inheritance": True,
    "members": True,
}
autodoc_member_order = "bysource"
autodoc_typehints_format = "short"
autosectionlabel_prefix_document = True
suppress_warnings = [
    "app.add_node",
    "ref.python",
    "autodoc",
    "duplicate",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "litestar_sphinx_theme"
html_static_path = ["_static"]
html_css_files = ["style.css"]
html_title = "Litestar Queues"
html_favicon = "_static/favicon.ico"
html_context = {
    "source_type": "github",
    "source_user": "litestar-org",
    "source_repo": "litestar-queues",
}

html_theme_options: dict[str, Any] = {
    "logo_target": "/",
    "github_repo_name": "litestar-queues",
    "github_url": "https://github.com/litestar-org/litestar-queues",
    "navigation_with_keys": True,
}
