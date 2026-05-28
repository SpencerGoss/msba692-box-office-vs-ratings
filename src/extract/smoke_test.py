"""Smoke-test the TMDB API.

Reads TMDB_READ_ACCESS_TOKEN from .env and hits /movie/popular. Prints the first
5 titles so we know auth + connectivity are working before the bulk extract.
"""
from __future__ import annotations

import os
import sys

import requests
from dotenv import load_dotenv

TMDB_BASE = "https://api.themoviedb.org/3"


def main() -> int:
    load_dotenv()
    token = os.environ.get("TMDB_READ_ACCESS_TOKEN")
    if not token:
        print("ERROR: TMDB_READ_ACCESS_TOKEN missing from .env", file=sys.stderr)
        return 1

    headers = {"Authorization": f"Bearer {token}", "accept": "application/json"}
    resp = requests.get(f"{TMDB_BASE}/movie/popular", headers=headers, timeout=15)
    resp.raise_for_status()

    results = resp.json().get("results", [])
    print(f"OK — fetched {len(results)} popular films. First 5:")
    for film in results[:5]:
        print(f"  - {film['title']} ({film.get('release_date', '?')[:4]}) "
              f"vote_avg={film['vote_average']} count={film['vote_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
