"""Bulk-extract films from TMDB for years 2000-2024 with the standard
cleaning rules applied.

Two-stage extraction:
  1. /discover/movie paginated by release year (filter vote_count >= 100,
     en-US metadata, sort by popularity desc). Gives us the candidate list.
  2. For each candidate, /movie/{id} detail call. Gives us budget, revenue,
     runtime, imdb_id, and the full genre objects.

Cleaning applied during stage 2:
  - drop films with budget == 0
  - drop films with revenue == 0
  - (vote_count >= 100 is already enforced by the /discover filter)

Output: data/raw/films_YYYY.json — one file per year, post-cleaning.
        data/raw/genres.json     — the TMDB genre lookup, fetched once.
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

TMDB_BASE = "https://api.themoviedb.org/3"
YEARS = range(2000, 2027)               # 2000..2026 inclusive
MIN_VOTE_COUNT = 100
PAGE_PAUSE_SEC = 0.05                   # ~20 req/s during discover paging
DETAIL_PAUSE_SEC = 0.03                 # ~30 req/s during detail fetches
# Set FORCE_REFETCH=1 in the environment to re-extract years that already have a JSON file.
FORCE_REFETCH = os.environ.get("FORCE_REFETCH", "").lower() in ("1", "true", "yes")

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"


def session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {token}", "accept": "application/json"})
    return s


def fetch_genres(s: requests.Session) -> list[dict]:
    r = s.get(f"{TMDB_BASE}/genre/movie/list", params={"language": "en-US"}, timeout=15)
    r.raise_for_status()
    return r.json()["genres"]


def discover_year(s: requests.Session, year: int) -> list[dict]:
    """Page through /discover/movie for one release year; return the lightweight list."""
    films: list[dict] = []
    page = 1
    while True:
        params = {
            "primary_release_year": year,
            "vote_count.gte": MIN_VOTE_COUNT,
            "language": "en-US",
            "sort_by": "popularity.desc",
            "include_adult": "false",
            "include_video": "false",
            "page": page,
        }
        r = s.get(f"{TMDB_BASE}/discover/movie", params=params, timeout=30)
        r.raise_for_status()
        payload = r.json()
        films.extend(payload.get("results", []))
        total_pages = min(payload.get("total_pages", 1), 500)  # TMDB hard cap
        if page >= total_pages:
            break
        page += 1
        time.sleep(PAGE_PAUSE_SEC)
    return films


def fetch_detail(s: requests.Session, tmdb_id: int) -> dict | None:
    """Fetch /movie/{id}. Returns None on 404 (deleted/private)."""
    r = s.get(f"{TMDB_BASE}/movie/{tmdb_id}", params={"language": "en-US"}, timeout=30)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def enrich_and_clean(s: requests.Session, candidates: list[dict]) -> list[dict]:
    """For each candidate, fetch detail; drop budget=0 OR revenue=0."""
    kept: list[dict] = []
    for c in candidates:
        detail = fetch_detail(s, c["id"])
        time.sleep(DETAIL_PAUSE_SEC)
        if not detail:
            continue
        if not detail.get("budget") or not detail.get("revenue"):
            continue
        # Merge: discover gave us genre_ids (we still want them as fallback);
        # detail gives us genres[], budget, revenue, runtime, imdb_id.
        kept.append({
            **c,
            "imdb_id":  detail.get("imdb_id"),
            "budget":   detail["budget"],
            "revenue":  detail["revenue"],
            "runtime":  detail.get("runtime"),
            # detail's "genres" is preferred (full objects), but we keep
            # discover's "genre_ids" so the load step has a clean integer list.
            "genres":   detail.get("genres", []),
        })
    return kept


def main() -> int:
    load_dotenv()
    token = os.environ.get("TMDB_READ_ACCESS_TOKEN")
    if not token:
        print("ERROR: TMDB_READ_ACCESS_TOKEN missing from .env", file=sys.stderr)
        return 1

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    s = session(token)

    genres = fetch_genres(s)
    (RAW_DIR / "genres.json").write_text(json.dumps(genres, indent=2), encoding="utf-8")
    print(f"genres: wrote {len(genres)} -> {RAW_DIR / 'genres.json'}")

    total_candidates = 0
    total_kept = 0
    skipped = 0
    for year in YEARS:
        out = RAW_DIR / f"films_{year}.json"
        if out.exists() and not FORCE_REFETCH:
            print(f"{year}: already extracted -> {out.name} (set FORCE_REFETCH=1 to redo)")
            skipped += 1
            continue
        candidates = discover_year(s, year)
        kept = enrich_and_clean(s, candidates)
        out.write_text(json.dumps(kept, indent=2), encoding="utf-8")
        print(f"{year}: {len(candidates):>4} candidates -> {len(kept):>4} kept "
              f"(after budget>0 & revenue>0) -> {out.name}")
        total_candidates += len(candidates)
        total_kept += len(kept)

    print(f"\nDone. {total_candidates} candidates considered, {total_kept} kept "
          f"in newly-fetched years. {skipped} years skipped (already on disk).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
