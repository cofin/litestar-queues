"""Assertions shared by browser and process-level example tests."""

import re
from urllib.parse import urljoin, urlsplit


def assert_asset_urls_on_origin(html: str, origin: str) -> None:
    """Reject asset URLs that escape the Litestar origin."""
    refs = re.findall(r"<(?:script\b[^>]*\bsrc|link\b[^>]*\bhref)=[\"']([^\"']+)[\"']", html, flags=re.IGNORECASE)

    assert refs, "page rendered no script or stylesheet references"
    expected = urlsplit(origin)
    for ref in refs:
        resolved = urlsplit(urljoin(f"{origin}/", ref))
        if resolved.scheme in {"http", "https"}:
            assert (resolved.scheme, resolved.netloc) == (expected.scheme, expected.netloc), (
                f"asset reference escapes the Litestar origin: {ref}"
            )


def assert_production_assets(html: str) -> None:
    """Ensure built pages do not retain the Vite development client."""
    assert "@vite/client" not in html, "production page still references the Vite development client"
