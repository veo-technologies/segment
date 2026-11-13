#!/usr/bin/env python3
"""Download Segment's embedded Public API OpenAPI spec and update crate references."""

from __future__ import annotations

import gzip
import hashlib
import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

DOCS_URL = "https://docs.segmentapis.com/"
USER_AGENT = "segment-rust-openapi-updater/1.0"

REPO_ROOT = Path(__file__).resolve().parent.parent
CRATE_DIR = REPO_ROOT / "segment-public-api"
OPENAPI_DIR = CRATE_DIR / "openapi"
BUILD_RS = CRATE_DIR / "build.rs"
LIB_RS = CRATE_DIR / "src" / "lib.rs"
CRATE_README = CRATE_DIR / "README.md"
OPENAPI_README = OPENAPI_DIR / "README.md"


def fetch_text(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "Accept-Encoding": "gzip",
            "User-Agent": USER_AGENT,
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = response.read()
        encoding = response.headers.get("Content-Encoding", "").lower()
        if "gzip" in encoding:
            payload = gzip.decompress(payload)
        charset = response.headers.get_content_charset() or "utf-8"
        return payload.decode(charset)


def extract_redoc_state_url(html: str) -> str:
    match = re.search(r"""src=["']([^"']*redocly-state-[^"']+\.js)["']""", html)
    if not match:
        raise RuntimeError("Could not locate redocly-state-<hash>.js in docs HTML")
    return urllib.parse.urljoin(DOCS_URL, match.group(1))


def extract_openapi_spec(state_js: str) -> dict:
    match = re.search(
        r"""const\s+__redoc_state\s*=\s*JSON\.parse\(("(?:\\.|[^"\\])*")\)\s*;""",
        state_js,
    )
    if not match:
        raise RuntimeError("Could not locate __redoc_state JSON.parse(...) in state JS")
    outer_json_str = json.loads(match.group(1))
    state = json.loads(outer_json_str)
    spec = state["definition"]["data"]
    if not isinstance(spec, dict) or "info" not in spec:
        raise RuntimeError("Extracted Redoc state does not contain an OpenAPI definition")
    return spec


def replace_exactly_once(path: Path, pattern: str, replacement: str, flags: int = 0) -> None:
    original = path.read_text(encoding="utf-8")
    updated, count = re.subn(pattern, replacement, original, count=1, flags=flags)
    if count != 1:
        raise RuntimeError(f"Expected 1 match for {pattern!r} in {path}, found {count}")
    if updated != original:
        path.write_text(updated, encoding="utf-8")


def main() -> None:
    docs_html = fetch_text(DOCS_URL)
    state_url = extract_redoc_state_url(docs_html)
    state_js = fetch_text(state_url)
    spec = extract_openapi_spec(state_js)

    version = str(spec["info"]["version"])
    rendered = json.dumps(spec, indent=2, ensure_ascii=False) + "\n"
    rendered_bytes = rendered.encode("utf-8")
    sha256 = hashlib.sha256(rendered_bytes).hexdigest()

    OPENAPI_DIR.mkdir(parents=True, exist_ok=True)
    target_filename = f"segment-public-api-{version}.json"
    target_path = OPENAPI_DIR / target_filename

    for existing in OPENAPI_DIR.glob("segment-public-api-*.json"):
        if existing != target_path:
            existing.unlink()

    if not target_path.exists() or target_path.read_bytes() != rendered_bytes:
        target_path.write_bytes(rendered_bytes)

    replace_exactly_once(
        BUILD_RS,
        r'const SPEC_PATH: &str = "openapi/segment-public-api-[^"]+\.json";',
        f'const SPEC_PATH: &str = "openapi/{target_filename}";',
    )
    replace_exactly_once(
        LIB_RS,
        r'pub const OPENAPI_VERSION: &str = "[^"]+";',
        f'pub const OPENAPI_VERSION: &str = "{version}";',
    )
    replace_exactly_once(
        LIB_RS,
        r"api-version: [0-9]+\.[0-9]+\.[0-9]+",
        f"api-version: {version}",
    )
    replace_exactly_once(
        CRATE_README,
        r"(OpenAPI\s+`)[^`]+(`)",
        rf"\g<1>{version}\g<2>",
    )

    today = datetime.now(timezone.utc).date().isoformat()
    replace_exactly_once(
        OPENAPI_README,
        r"`segment-public-api-[^`]+\.json`",
        f"`{target_filename}`",
    )
    replace_exactly_once(
        OPENAPI_README,
        r"on \d{4}-\d{2}-\d{2}\.",
        f"on {today}.",
    )
    replace_exactly_once(
        OPENAPI_README,
        r"- Public API version: `[^`]+`",
        f"- Public API version: `{version}`",
    )
    replace_exactly_once(
        OPENAPI_README,
        r"(- Upstream document SHA-256:\s+`)[0-9a-f]{64}(`)",
        rf"\g<1>{sha256}\g<2>",
    )

    print(f"Updated OpenAPI spec to {version} (sha256: {sha256})")


if __name__ == "__main__":
    main()
