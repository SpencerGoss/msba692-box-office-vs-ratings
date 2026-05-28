"""Render the Mermaid ER diagram to PNG via the mermaid.ink service.

Reads docs/er_diagram.md, extracts the ```mermaid ... ``` code block,
base64-encodes it, and fetches the PNG from mermaid.ink (no Node.js install
needed). Writes to submission/er_diagram.png.
"""
from __future__ import annotations

import base64
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs" / "er_diagram.md"
TARGET = ROOT / "submission" / "er_diagram.png"


def extract_mermaid_block(md_text: str) -> str:
    m = re.search(r"```mermaid\s*\n(.*?)\n```", md_text, flags=re.DOTALL)
    if not m:
        raise SystemExit("No ```mermaid ... ``` block found in er_diagram.md")
    return m.group(1).strip()


def main() -> int:
    md = SOURCE.read_text(encoding="utf-8")
    mermaid_src = extract_mermaid_block(md)

    # mermaid.ink accepts URL-safe base64 of the Mermaid source.
    encoded = base64.urlsafe_b64encode(mermaid_src.encode("utf-8")).decode("ascii")
    url = f"https://mermaid.ink/img/{encoded}?type=png&bgColor=white"

    print(f"Fetching: {url[:80]}... ({len(encoded)} chars encoded)")
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0",
        "Accept": "image/png,image/*,*/*",
    })
    with urllib.request.urlopen(req, timeout=30) as r:
        data = r.read()
    TARGET.write_bytes(data)
    print(f"Wrote {len(data):,} bytes -> {TARGET}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
