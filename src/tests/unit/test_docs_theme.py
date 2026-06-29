import importlib.util
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
    import tomli as tomllib


ROOT = Path(__file__).resolve().parents[3]


def load_docs_conf() -> object:
    spec = importlib.util.spec_from_file_location("litestar_queues_docs_conf", ROOT / "docs" / "conf.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_pyproject() -> dict[str, object]:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())


def test_docs_use_direct_shibuya_theme_with_queue_navigation() -> None:
    conf = load_docs_conf()

    assert conf.html_theme == "shibuya"
    assert conf.html_css_files == ["custom.css", "style.css"]
    assert conf.html_favicon == "_static/favicon.svg"
    assert conf.pygments_style == "tools.sphinx_ext.pygments_styles.LitestarQueuesLightStyle"
    assert conf.pygments_dark_style == "tools.sphinx_ext.pygments_styles.LitestarQueuesDarkStyle"
    assert conf.html_context == {
        "source_type": "github",
        "source_user": "cofin",
        "source_repo": "litestar-queues",
        "current_version": "latest",
        "version": conf.release,
    }

    options = conf.html_theme_options
    assert options["accent_color"] == "amber"
    assert options["light_logo"] == "_static/logo-light.svg"
    assert options["dark_logo"] == "_static/logo-dark.svg"
    assert options["navigation_with_keys"] is True
    assert options["globaltoc_expand_depth"] == 0

    nav_links = options["nav_links"]
    docs_children = nav_links[0]["children"]
    assert {item["url"] for item in docs_children} >= {
        "getting_started/index",
        "usage/index",
        "usage/backends",
        "reference/index",
    }


def test_docs_assets_are_real_litestar_queues_assets() -> None:
    static_path = ROOT / "docs" / "_static"

    for asset_name in ("logo-light.svg", "logo-dark.svg", "favicon.svg"):
        asset = static_path / asset_name
        assert asset.exists()
        content = asset.read_text()
        assert "Email" not in content
        assert "Litestar Queues" in content


def test_pyproject_uses_direct_shibuya_dependency_without_docs_helper_entry_points() -> None:
    pyproject = load_pyproject()

    docs_dependencies = pyproject["dependency-groups"]["docs"]
    assert "shibuya" in docs_dependencies
    assert not any("litestar-sphinx-theme" in dependency for dependency in docs_dependencies)
    assert "entry-points" not in pyproject["project"]
