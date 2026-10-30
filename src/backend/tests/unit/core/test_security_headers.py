"""The page CSP and the frontend agree on what the app itself loads.

The A2UI Map's tile URL lives in the frontend (LeafletMap.tsx, also vendored
into the exported app); the CSP that must allow it lives here. This keeps the
two from drifting: change the tile host in one place and this fails.
"""

import re
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from src.core.security_headers import CONTENT_SECURITY_POLICY

SRC = Path(__file__).resolve().parents[4]
LEAFLET_MAPS = [
    SRC / "frontend" / "src" / "shared" / "a2ui" / "LeafletMap.tsx",
    SRC
    / "backend"
    / "src"
    / "services"
    / "export"
    / "templates"
    / "databricks_app"
    / "frontend"
    / "src"
    / "a2ui"
    / "LeafletMap.tsx",
]
# Leaflet's default subdomains for a `{s}` placeholder.
SUBDOMAINS = ["a", "b", "c"]


def _sources(directive: str) -> list[str]:
    for part in CONTENT_SECURITY_POLICY.split(";"):
        tokens = part.split()
        if tokens and tokens[0] == directive:
            return tokens[1:]
    raise AssertionError(f"{directive} missing from the CSP")


def _allows(sources: list[str], url: str) -> bool:
    """CSP host-source matching for scheme://host and scheme://*.host."""
    parts = urlsplit(url)
    for source in sources:
        match = re.fullmatch(r"(https?)://(\*\.)?([^/:]+)", source)
        if not match or match[1] != parts.scheme:
            continue
        host = parts.hostname or ""
        if match[2] and host.endswith("." + match[3]):
            return True
        if not match[2] and host == match[3]:
            return True
    return False


def _default_tile_url(path: Path) -> str:
    source = path.read_text()
    match = re.search(r"VITE_MAP_TILE_URL\s*\|\|\s*'([^']+)'", source)
    assert match, f"no default tile URL found in {path}"
    return match[1]


@pytest.mark.parametrize("path", LEAFLET_MAPS, ids=["frontend", "exported-app"])
def test_the_map_tile_url_is_allowed_by_img_src(path):
    template = _default_tile_url(path)
    urls = (
        [template.replace("{s}", s) for s in SUBDOMAINS]
        if "{s}" in template
        else [template]
    )
    img_src = _sources("img-src")
    for url in urls:
        concrete = re.sub(r"\{[a-z]\}", "0", url)
        assert _allows(img_src, concrete), f"{concrete} is not allowed by {img_src}"


def test_osm_bare_host_and_subdomains_are_both_allowed():
    img_src = _sources("img-src")
    assert _allows(img_src, "https://tile.openstreetmap.org/0/0/0.png")
    assert _allows(img_src, "https://a.tile.openstreetmap.org/0/0/0.png")
    assert not _allows(img_src, "https://images.example.org/x.png")
    assert not _allows(img_src, "https://tile.openstreetmap.org.example.org/x.png")
