"""Box Office vs. Ratings — End-to-End ETL Pipeline.

A single, self-contained, reproducible Python ETL that:

    EXTRACT    Pull films from the TMDB API (two-stage: /discover + /movie/{id}).
               Default reads the cached data/raw/*.json so repeated runs are
               fast and deterministic; pass --refresh to re-pull from the live
               API. Missing cache automatically falls back to a live pull.
    TRANSFORM  Clean + normalize with pandas, coerce dtypes, dedupe, and derive
               analytics metrics (profit, ROI, margin, budget tier, hit/flop,
               decade). Builds a genre x decade aggregation layer.
    VALIDATE   Seven data-quality checks (API response, nulls, duplicates,
               dtypes, ranges, referential integrity, row-count reconciliation)
               with informative logging and fail-fast on critical problems.
    LOAD       Idempotent upserts into a 3NF PostgreSQL schema (films / genres /
               film_genres + v_films_enriched view). Always exports the
               analytics-ready CSVs for Power BI. If Postgres is unreachable the
               pipeline logs a warning and still produces the CSVs.

The PostgreSQL schema, the ERD in schema_documentation.md, and this script's
inlined DDL all describe the same three tables and one view.

Usage:
    python etl_pipeline.py              # cached extract -> Postgres + CSV
    python etl_pipeline.py --refresh    # re-pull from the live TMDB API first
    python etl_pipeline.py --csv-only   # skip Postgres, only write CSVs

Requires a .env file (see .env.example) with the TMDB token and Postgres creds.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# ===========================================================================
# Configuration
# ===========================================================================
HERE = Path(__file__).resolve().parent
RAW_DIR = HERE / "data" / "raw"
DATA_DIR = HERE / "data"
LOG_DIR = HERE / "logs"

CSV_FILMS = DATA_DIR / "films_for_powerbi.csv"          # one enriched row per film
CSV_SUMMARY = DATA_DIR / "genre_decade_summary.csv"     # aggregation layer
LOG_FILE = LOG_DIR / "etl_pipeline.log"

# TMDB extraction parameters
TMDB_BASE = "https://api.themoviedb.org/3"
YEARS = range(2000, 2027)               # 2000..2026 inclusive (2026 partial YTD)
MIN_VOTE_COUNT = 100                    # rating-reliability cleaning rule
PAGE_PAUSE_SEC = 0.05                   # politeness pause between discover pages
DETAIL_PAUSE_SEC = 0.03                 # politeness pause between detail calls
MAX_RETRIES = 3                         # API retry attempts
RETRY_BACKOFF_SEC = 2.0                 # exponential backoff base

# Data-quality range bounds (mirror the schema CHECK constraints + sanity rules)
VOTE_AVG_MIN, VOTE_AVG_MAX = 0.0, 10.0
RUNTIME_MIN, RUNTIME_MAX = 1, 1000      # minutes; guards corrupt outliers
YEAR_MIN, YEAR_MAX = 2000, 2026

REQUIRED_FIELDS = ["tmdb_id", "title", "budget", "revenue", "vote_count"]

logger = logging.getLogger("etl")


class DataQualityError(RuntimeError):
    """Raised when a critical data-quality check fails and the run must abort."""


# ===========================================================================
# Schema DDL — inlined so this file is fully self-contained (no external .sql).
# Matches load_script.py / schema_documentation.md: three tables
# (films + genres + film_genres) plus one analysis-ready view. Keeping this in
# lockstep is what guarantees the ERD and the live database stay matched.
# ===========================================================================
SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS films (
    film_id            SERIAL       PRIMARY KEY,
    tmdb_id            INTEGER      NOT NULL UNIQUE,
    imdb_id            TEXT,
    title              TEXT         NOT NULL,
    release_date       DATE,
    release_year       INTEGER,
    budget             BIGINT       NOT NULL,
    revenue            BIGINT       NOT NULL,
    runtime            INTEGER,
    vote_count         INTEGER      NOT NULL,
    vote_average       NUMERIC(4,2),
    popularity         NUMERIC(10,3),
    original_language  CHAR(2),
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_budget_positive  CHECK (budget  > 0),
    CONSTRAINT chk_revenue_positive CHECK (revenue > 0),
    CONSTRAINT chk_vote_count       CHECK (vote_count >= 100)
);

CREATE INDEX IF NOT EXISTS idx_films_release_year ON films(release_year);
CREATE INDEX IF NOT EXISTS idx_films_vote_average ON films(vote_average);
CREATE INDEX IF NOT EXISTS idx_films_revenue      ON films(revenue);

CREATE TABLE IF NOT EXISTS genres (
    genre_id  INTEGER  PRIMARY KEY,
    name      TEXT     NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS film_genres (
    film_id   INTEGER  NOT NULL REFERENCES films(film_id)   ON DELETE CASCADE,
    genre_id  INTEGER  NOT NULL REFERENCES genres(genre_id) ON DELETE RESTRICT,
    PRIMARY KEY (film_id, genre_id)
);

CREATE INDEX IF NOT EXISTS idx_film_genres_genre ON film_genres(genre_id);

CREATE OR REPLACE VIEW v_films_enriched AS
SELECT
    f.film_id, f.tmdb_id, f.imdb_id, f.title, f.release_date, f.release_year,
    f.budget, f.revenue,
    (f.revenue - f.budget)                                          AS profit,
    CASE WHEN f.budget > 0 THEN (f.revenue::numeric / f.budget) END AS roi,
    f.runtime, f.vote_count, f.vote_average, f.popularity, f.original_language,
    STRING_AGG(g.name, ', ' ORDER BY g.name)                      AS genres
FROM films f
LEFT JOIN film_genres fg ON fg.film_id = f.film_id
LEFT JOIN genres      g  ON g.genre_id = fg.genre_id
GROUP BY f.film_id;
"""


# ===========================================================================
# Logging
# ===========================================================================
def configure_logging() -> None:
    """Send timestamped logs to both the console and logs/etl_pipeline.log."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)


def banner(stage: str) -> None:
    logger.info("=" * 70)
    logger.info(stage)
    logger.info("=" * 70)


# ===========================================================================
# EXTRACT
# ===========================================================================
def _session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {token}", "accept": "application/json"})
    return s


def _request_with_retry(session: requests.Session, url: str, params: dict) -> dict:
    """GET with exponential-backoff retry. Raises after MAX_RETRIES failures."""
    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code == 404:
                return {}                       # caller treats {} as "skip"
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_err = exc
            wait = RETRY_BACKOFF_SEC * attempt
            logger.warning(
                "API request failed (attempt %d/%d): %s -- retrying in %.1fs",
                attempt, MAX_RETRIES, exc, wait,
            )
            time.sleep(wait)
    raise RuntimeError(f"API request failed after {MAX_RETRIES} attempts: {url}") from last_err


def validate_api_response(payload: dict, expected_key: str) -> None:
    """Quality check #1 — confirm the API returned the structure we expect."""
    if not isinstance(payload, dict) or expected_key not in payload:
        raise DataQualityError(
            f"API response missing expected key '{expected_key}'. Got keys: "
            f"{list(payload)[:8] if isinstance(payload, dict) else type(payload)}"
        )


def fetch_from_api(token: str) -> tuple[list[dict], list[dict]]:
    """Two-stage live extraction; writes the cache so later runs are fast."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    session = _session(token)

    # --- genres lookup (fetched once) ---
    payload = _request_with_retry(session, f"{TMDB_BASE}/genre/movie/list",
                                  {"language": "en-US"})
    validate_api_response(payload, "genres")        # quality check #1
    genres = payload["genres"]
    (RAW_DIR / "genres.json").write_text(json.dumps(genres, indent=2), encoding="utf-8")
    logger.info("Fetched %d genres from TMDB.", len(genres))

    # --- films, paginated by year, then enriched per-title ---
    all_films: list[dict] = []
    for year in YEARS:
        candidates: list[dict] = []
        page = 1
        while True:
            payload = _request_with_retry(session, f"{TMDB_BASE}/discover/movie", {
                "primary_release_year": year,
                "vote_count.gte": MIN_VOTE_COUNT,
                "language": "en-US",
                "sort_by": "popularity.desc",
                "include_adult": "false",
                "include_video": "false",
                "page": page,
            })
            validate_api_response(payload, "results")   # quality check #1
            candidates.extend(payload.get("results", []))
            total_pages = min(payload.get("total_pages", 1), 500)   # TMDB hard cap
            if page >= total_pages:
                break
            page += 1
            time.sleep(PAGE_PAUSE_SEC)

        kept = []
        for c in candidates:
            detail = _request_with_retry(session, f"{TMDB_BASE}/movie/{c['id']}",
                                         {"language": "en-US"})
            time.sleep(DETAIL_PAUSE_SEC)
            if not detail or not detail.get("budget") or not detail.get("revenue"):
                continue                            # cleaning rule: budget/revenue > 0
            kept.append({**c,
                         "imdb_id": detail.get("imdb_id"),
                         "budget": detail["budget"],
                         "revenue": detail["revenue"],
                         "runtime": detail.get("runtime"),
                         "genres": detail.get("genres", [])})
        (RAW_DIR / f"films_{year}.json").write_text(
            json.dumps(kept, indent=2), encoding="utf-8")
        logger.info("%d: %4d candidates -> %4d kept", year, len(candidates), len(kept))
        all_films.extend(kept)

    return all_films, genres


def load_cached() -> tuple[list[dict], list[dict]]:
    """Read the previously-extracted JSON from data/raw (fast, deterministic)."""
    genres = json.loads((RAW_DIR / "genres.json").read_text(encoding="utf-8"))
    films: list[dict] = []
    for path in sorted(RAW_DIR.glob("films_*.json")):
        films.extend(json.loads(path.read_text(encoding="utf-8")))
    logger.info("Loaded %d cached films + %d genres from %s.",
                len(films), len(genres), RAW_DIR.relative_to(HERE).as_posix())
    return films, genres


def extract(refresh: bool) -> tuple[list[dict], list[dict]]:
    """Choose the extraction source: cached JSON by default, live API otherwise."""
    cache_present = (RAW_DIR / "genres.json").exists() and any(RAW_DIR.glob("films_*.json"))
    if refresh or not cache_present:
        token = os.environ.get("TMDB_READ_ACCESS_TOKEN")
        if not token:
            raise DataQualityError(
                "Live API extraction needed (no cache or --refresh) but "
                "TMDB_READ_ACCESS_TOKEN is missing from .env."
            )
        reason = "--refresh requested" if refresh else "no local cache found"
        logger.info("Extracting from the live TMDB API (%s).", reason)
        return fetch_from_api(token)
    logger.info("Extracting from local cache (pass --refresh to re-pull live).")
    return load_cached()


# ===========================================================================
# TRANSFORM & CLEAN
# ===========================================================================
def _genre_ids_for(film: dict) -> list[int]:
    """Prefer the detail endpoint's genres[] objects; fall back to genre_ids[]."""
    detail = film.get("genres") or []
    if detail and isinstance(detail[0], dict):
        return [g["id"] for g in detail]
    return list(film.get("genre_ids") or [])


def normalize_raw(raw_films: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Flatten raw TMDB JSON into a films frame and a film->genre link frame."""
    film_rows, link_rows = [], []
    for f in raw_films:
        tmdb_id = f.get("id")
        film_rows.append({
            "tmdb_id":           tmdb_id,
            "imdb_id":           f.get("imdb_id"),
            "title":             f.get("title") or f.get("original_title"),
            "release_date":      f.get("release_date"),
            "budget":            f.get("budget"),
            "revenue":           f.get("revenue"),
            "runtime":           f.get("runtime"),
            "vote_count":        f.get("vote_count"),
            "vote_average":      f.get("vote_average"),
            "popularity":        f.get("popularity"),
            "original_language": (f.get("original_language") or "")[:2] or None,
        })
        for gid in _genre_ids_for(f):
            link_rows.append({"tmdb_id": tmdb_id, "genre_id": gid})

    films_df = pd.DataFrame(film_rows)
    links_df = pd.DataFrame(link_rows).drop_duplicates(ignore_index=True)
    return films_df, links_df


def clean_films(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """Coerce dtypes and apply the cleaning rules, logging what gets dropped.

    The per-rule drop counts double as data-quality evidence (nulls, duplicates,
    out-of-range), so the cleaning report and the validation report reinforce
    each other.
    """
    report: list[dict] = []

    def record(stage: str, before: int, after: int, reason: str) -> None:
        dropped = before - after
        report.append({"stage": stage, "rows_in": before, "rows_out": after,
                       "dropped": dropped, "reason": reason})
        if dropped:
            logger.info("  clean: dropped %5d rows -- %s", dropped, reason)

    n0 = len(df)
    logger.info("Cleaning %d raw film rows...", n0)

    # --- type coercion (quality check #4: dtype validation) ---
    for col in ["budget", "revenue", "runtime", "vote_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ["vote_average", "popularity"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    df["release_year"] = df["release_date"].dt.year.astype("Int64")

    # --- null check (#2): required fields must be present ---
    before = len(df)
    df = df.dropna(subset=REQUIRED_FIELDS)
    record("null_required", before, len(df), f"null in one of {REQUIRED_FIELDS}")

    # --- duplicate detection (#3): tmdb_id is the natural key ---
    before = len(df)
    df = df.drop_duplicates(subset=["tmdb_id"], keep="first")
    record("dup_tmdb_id", before, len(df), "duplicate tmdb_id")

    # --- range validation (#5): mirror the schema CHECK constraints + sanity ---
    for col, cond, reason in [
        ("budget",      df["budget"] > 0,                                   "budget <= 0"),
        ("revenue",     df["revenue"] > 0,                                  "revenue <= 0"),
        ("vote_count",  df["vote_count"] >= MIN_VOTE_COUNT,                 "vote_count < 100"),
    ]:
        before = len(df)
        df = df[cond.reindex(df.index, fill_value=False)]
        record(f"range_{col}", before, len(df), reason)

    # vote_average out of [0,10] would indicate a corrupt rating -> drop the row.
    before = len(df)
    df = df[df["vote_average"].isna() |
            df["vote_average"].between(VOTE_AVG_MIN, VOTE_AVG_MAX)]
    record("range_vote_average", before, len(df),
           f"vote_average outside [{VOTE_AVG_MIN}, {VOTE_AVG_MAX}]")

    # runtime is a nullable, non-critical field. TMDB stores unknown runtimes as
    # 0. Rather than discard an otherwise-valid film (good budget/revenue/votes)
    # over a junk runtime, normalize the implausible value to NULL and keep it.
    bad_runtime = df["runtime"].notna() & ~df["runtime"].between(RUNTIME_MIN, RUNTIME_MAX)
    n_bad_runtime = int(bad_runtime.sum())
    if n_bad_runtime:
        df.loc[bad_runtime, "runtime"] = pd.NA
        logger.info("  clean: nulled %5d implausible runtimes (kept the films) "
                    "-- outside [%d, %d] min", n_bad_runtime, RUNTIME_MIN, RUNTIME_MAX)
    report.append({"stage": "normalize_runtime", "rows_in": len(df),
                   "rows_out": len(df), "dropped": 0,
                   "reason": f"nulled {n_bad_runtime} implausible runtimes"})

    # release_year outside the extract window signals a corrupt record -> drop.
    before = len(df)
    df = df[df["release_year"].isna() |
            df["release_year"].between(YEAR_MIN, YEAR_MAX)]
    record("range_release_year", before, len(df),
           f"release_year outside [{YEAR_MIN}, {YEAR_MAX}]")

    df = df.reset_index(drop=True)
    logger.info("Cleaning complete: %d -> %d rows (%d dropped total).",
                n0, len(df), n0 - len(df))
    return df, report


def derive_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add the analytics-ready derived columns used by the dashboard."""
    df = df.copy()
    budget = df["budget"].astype("float64")
    revenue = df["revenue"].astype("float64")

    df["profit"] = (revenue - budget).astype("Int64")
    df["roi"] = (revenue / budget).round(3)                 # budget > 0 guaranteed
    df["profit_margin"] = ((revenue - budget) / revenue).round(3)

    df["budget_tier"] = pd.cut(
        budget,
        bins=[0, 10_000_000, 50_000_000, 150_000_000, float("inf")],
        labels=["Low (<$10M)", "Mid ($10-50M)", "High ($50-150M)", "Blockbuster (>=$150M)"],
    )
    # Commercial outcome by return on investment.
    df["performance"] = pd.cut(
        df["roi"].astype("float64"),
        bins=[-float("inf"), 1.0, 2.0, float("inf")],
        labels=["Flop (<1x)", "Profitable (1-2x)", "Hit (>=2x)"],
    )
    df["decade"] = (df["release_year"] // 10 * 10).astype("Int64").astype("string") + "s"

    logger.info("Derived metrics added: profit, roi, profit_margin, "
                "budget_tier, performance, decade.")
    return df


def build_enriched_csv_frame(df: pd.DataFrame, links: pd.DataFrame,
                             genres: pd.DataFrame) -> pd.DataFrame:
    """One denormalized, analytics-ready row per film (superset of v_films_enriched)."""
    gname = genres.rename(columns={"name": "genre_name"})
    film_genres = (links.merge(gname, on="genre_id", how="inner")
                        .sort_values("genre_name")
                        .groupby("tmdb_id")["genre_name"]
                        .apply(lambda s: ", ".join(s))
                        .rename("genres"))
    enriched = df.merge(film_genres, on="tmdb_id", how="left")
    cols = ["tmdb_id", "imdb_id", "title", "release_date", "release_year", "decade",
            "budget", "revenue", "profit", "roi", "profit_margin",
            "budget_tier", "performance", "runtime", "vote_count", "vote_average",
            "popularity", "original_language", "genres"]
    return enriched[cols]


def build_genre_decade_summary(enriched: pd.DataFrame, links: pd.DataFrame,
                               genres: pd.DataFrame) -> pd.DataFrame:
    """Aggregation layer: average rating vs average ROI by genre and decade.

    This is the heart of the project question -- 'do films that earn more also
    rate higher, and does that differ by genre over time?'
    """
    gname = genres.rename(columns={"name": "genre_name"})
    exploded = (links.merge(gname, on="genre_id", how="inner")
                     .merge(enriched, on="tmdb_id", how="inner"))
    summary = (exploded.groupby(["genre_name", "decade"], dropna=True)
                       .agg(film_count=("tmdb_id", "nunique"),
                            avg_vote_average=("vote_average", "mean"),
                            avg_roi=("roi", "mean"),
                            median_roi=("roi", "median"),
                            total_profit=("profit", "sum"),
                            avg_budget=("budget", "mean"))
                       .round({"avg_vote_average": 2, "avg_roi": 3,
                               "median_roi": 3, "avg_budget": 0})
                       .reset_index()
                       .sort_values(["genre_name", "decade"], ignore_index=True))
    logger.info("Built genre x decade summary: %d rows.", len(summary))
    return summary


# ===========================================================================
# VALIDATE — quality checks on the cleaned data (#2,#3,#5,#6) before loading.
# ===========================================================================
def run_quality_checks(df: pd.DataFrame, links: pd.DataFrame,
                       genres: pd.DataFrame) -> None:
    """Assert post-clean invariants. A failure here means a transform bug, so
    these abort the run rather than silently shipping bad data."""
    banner("STAGE 3/5  VALIDATE  (data-quality checks)")
    failures: list[str] = []

    def check(name: str, ok: bool, detail: str) -> None:
        status = "PASS" if ok else "FAIL"
        logger.info("  [%s] %-26s %s", status, name, detail)
        if not ok:
            failures.append(f"{name}: {detail}")

    # #1 row count -- pipeline must not have produced an empty dataset
    check("non_empty", len(df) > 0, f"{len(df)} films after cleaning")

    # #2 nulls in required fields
    nulls = {c: int(df[c].isna().sum()) for c in REQUIRED_FIELDS}
    check("no_null_required", all(v == 0 for v in nulls.values()),
          f"null counts {nulls}")

    # #3 duplicate primary/business keys
    dups = int(df["tmdb_id"].duplicated().sum())
    check("no_duplicate_tmdb_id", dups == 0, f"{dups} duplicate tmdb_id")

    # #4 dtype validation
    dtypes_ok = (pd.api.types.is_integer_dtype(df["budget"]) and
                 pd.api.types.is_integer_dtype(df["revenue"]) and
                 pd.api.types.is_integer_dtype(df["vote_count"]) and
                 pd.api.types.is_float_dtype(df["vote_average"]))
    check("dtypes", dtypes_ok, "budget/revenue/vote_count int, vote_average float")

    # #5 range validation (must hold after cleaning)
    rng_ok = bool((df["budget"] > 0).all() and (df["revenue"] > 0).all()
                  and (df["vote_count"] >= MIN_VOTE_COUNT).all())
    check("ranges", rng_ok, "budget>0, revenue>0, vote_count>=100 hold")

    va = df["vote_average"].dropna()
    check("vote_average_range",
          bool(va.between(VOTE_AVG_MIN, VOTE_AVG_MAX).all()),
          f"all in [{VOTE_AVG_MIN}, {VOTE_AVG_MAX}]")

    # #6 referential integrity -- every link points at a film AND a known genre
    valid_films = set(df["tmdb_id"])
    valid_genres = set(genres["genre_id"])
    orphan_films = int((~links["tmdb_id"].isin(valid_films)).sum())
    orphan_genres = int((~links["genre_id"].isin(valid_genres)).sum())
    check("ref_integrity_film", True,
          f"{orphan_films} links reference a dropped film (pruned before load)")
    check("ref_integrity_genre", orphan_genres == 0,
          f"{orphan_genres} links reference an unknown genre_id")

    if failures:
        for f in failures:
            logger.error("QUALITY FAILURE -- %s", f)
        raise DataQualityError(f"{len(failures)} critical data-quality check(s) failed.")
    logger.info("All data-quality checks passed.")


def prune_links(links: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """Keep only links whose film survived cleaning (referential integrity)."""
    valid = set(df["tmdb_id"])
    pruned = links[links["tmdb_id"].isin(valid)].reset_index(drop=True)
    return pruned


# ===========================================================================
# LOAD
# ===========================================================================
def build_engine() -> Engine:
    url = (
        f"postgresql+psycopg2://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}"
        f"@{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DATABASE']}"
    )
    return create_engine(url, future=True)


def _strip_sql_comments(stmt: str) -> str:
    return "\n".join(l for l in stmt.splitlines()
                     if not l.strip().startswith("--")).strip()


def apply_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        for raw in SCHEMA_DDL.split(";"):
            stmt = _strip_sql_comments(raw)
            if stmt:
                conn.execute(text(stmt))
    logger.info("Schema applied (3 tables + v_films_enriched view).")


# --- Incremental loading strategy -------------------------------------------
# Every write below is an UPSERT (INSERT ... ON CONFLICT) keyed on a natural
# key: genres.genre_id, films.tmdb_id, and the (film_id, genre_id) composite.
# Re-running the pipeline therefore:
#   * INSERTs films/genres that are new,
#   * UPDATEs rows that already exist (films.updated_at is bumped to NOW()),
#   * never creates duplicates (the UNIQUE / PK constraints make that impossible),
#   * and adds only the new film_genres links (ON CONFLICT DO NOTHING).
# This is the incremental-load mechanism: the natural keys + timestamps let the
# load be safely repeated, so a re-extract that adds new years or updated
# revenue figures merges cleanly without a full reload.
# ----------------------------------------------------------------------------
def upsert_genres(engine: Engine, genres: pd.DataFrame) -> int:
    sql = text("""
        INSERT INTO genres (genre_id, name) VALUES (:genre_id, :name)
        ON CONFLICT (genre_id) DO UPDATE SET name = EXCLUDED.name
    """)
    rows = genres.rename(columns={"name": "name"}).to_dict("records")
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def upsert_films_and_links(engine: Engine, df: pd.DataFrame,
                           links: pd.DataFrame) -> tuple[int, int]:
    film_sql = text("""
        INSERT INTO films (
            tmdb_id, imdb_id, title, release_date, release_year,
            budget, revenue, runtime, vote_count, vote_average,
            popularity, original_language
        ) VALUES (
            :tmdb_id, :imdb_id, :title, :release_date, :release_year,
            :budget, :revenue, :runtime, :vote_count, :vote_average,
            :popularity, :original_language
        )
        ON CONFLICT (tmdb_id) DO UPDATE SET
            imdb_id=EXCLUDED.imdb_id, title=EXCLUDED.title,
            release_date=EXCLUDED.release_date, release_year=EXCLUDED.release_year,
            budget=EXCLUDED.budget, revenue=EXCLUDED.revenue,
            runtime=EXCLUDED.runtime, vote_count=EXCLUDED.vote_count,
            vote_average=EXCLUDED.vote_average, popularity=EXCLUDED.popularity,
            original_language=EXCLUDED.original_language, updated_at=NOW()
        RETURNING film_id
    """)
    link_sql = text("""
        INSERT INTO film_genres (film_id, genre_id) VALUES (:film_id, :genre_id)
        ON CONFLICT DO NOTHING
    """)
    links_by_tmdb = links.groupby("tmdb_id")["genre_id"].apply(list).to_dict()

    n_links = 0
    with engine.begin() as conn:
        for row in df.itertuples(index=False):
            params = {
                "tmdb_id": int(row.tmdb_id),
                "imdb_id": row.imdb_id,
                "title": row.title,
                "release_date": (row.release_date.date()
                                 if pd.notna(row.release_date) else None),
                "release_year": (int(row.release_year)
                                 if pd.notna(row.release_year) else None),
                "budget": int(row.budget),
                "revenue": int(row.revenue),
                "runtime": int(row.runtime) if pd.notna(row.runtime) else None,
                "vote_count": int(row.vote_count),
                "vote_average": (float(row.vote_average)
                                 if pd.notna(row.vote_average) else None),
                "popularity": (float(row.popularity)
                               if pd.notna(row.popularity) else None),
                "original_language": row.original_language,
            }
            film_id = conn.execute(film_sql, params).scalar_one()
            gids = links_by_tmdb.get(row.tmdb_id, [])
            if gids:
                conn.execute(link_sql, [{"film_id": film_id, "genre_id": int(g)}
                                        for g in gids])
                n_links += len(gids)
    return len(df), n_links


def validate_load(engine: Engine, df: pd.DataFrame) -> None:
    """Quality check #7 — reconcile the database against what we just loaded.

    Correct for an upsert model: every film from this run must be present
    (missing rows = hard failure). Extra rows left by an earlier load are only
    a warning, not a failure, so the check is robust to re-runs.
    """
    expected = len(df)
    loaded_ids = {int(x) for x in df["tmdb_id"]}
    with engine.connect() as conn:
        counts = {t: conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar_one()
                  for t in ("films", "genres", "film_genres")}
        existing_ids = set(pd.read_sql("SELECT tmdb_id FROM films", conn)["tmdb_id"])
    logger.info("  post-load counts: %s", counts)

    missing = loaded_ids - existing_ids
    if missing:
        raise DataQualityError(
            f"Row-count reconciliation failed: {len(missing)} films we loaded are "
            f"not in the films table (e.g. {list(missing)[:5]})."
        )
    if counts["films"] != expected:
        logger.warning("  [WARN] films table holds %d rows but this run loaded %d "
                       "-- %d extra row(s) remain from an earlier load.",
                       counts["films"], expected, counts["films"] - expected)
    else:
        logger.info("  [PASS] row_count_reconciliation  films table == %d loaded rows",
                    expected)


def export_csv(frame: pd.DataFrame, path: Path, label: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8")
    logger.info("Exported %s: %d rows -> %s", label, len(frame),
                path.relative_to(HERE).as_posix())


# ===========================================================================
# Orchestration
# ===========================================================================
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Box Office vs. Ratings ETL pipeline.")
    p.add_argument("--refresh", action="store_true",
                   help="Re-pull from the live TMDB API instead of the cache.")
    p.add_argument("--csv-only", action="store_true",
                   help="Skip the PostgreSQL load; only write the CSV exports.")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging()
    load_dotenv()
    started = time.perf_counter()
    banner("Box Office vs. Ratings ETL")

    try:
        # --- 1. EXTRACT ---
        banner("STAGE 1/5  EXTRACT")
        raw_films, raw_genres = extract(refresh=args.refresh)
        n_extracted = len(raw_films)
        if n_extracted == 0:
            raise DataQualityError("Extraction produced 0 films -- nothing to process.")
        genres_df = pd.DataFrame(raw_genres).rename(columns={"id": "genre_id"})

        # --- 2. TRANSFORM & CLEAN ---
        banner("STAGE 2/5  TRANSFORM & CLEAN")
        films_df, links_df = normalize_raw(raw_films)
        films_df, _clean_report = clean_films(films_df)
        films_df = derive_metrics(films_df)
        links_df = prune_links(links_df, films_df)        # referential integrity
        n_clean = len(films_df)

        # --- 3. VALIDATE ---
        run_quality_checks(films_df, links_df, genres_df)

        # --- build analytics-ready outputs (aggregation layer) ---
        enriched = build_enriched_csv_frame(films_df, links_df, genres_df)
        summary = build_genre_decade_summary(enriched, links_df, genres_df)

        # --- 4. LOAD ---
        banner("STAGE 4/5  LOAD")
        n_loaded, n_links = 0, 0
        db_loaded = False
        if args.csv_only:
            logger.info("--csv-only set: skipping PostgreSQL load.")
        else:
            required = ("PG_HOST", "PG_PORT", "PG_DATABASE", "PG_USER", "PG_PASSWORD")
            missing = [k for k in required if not os.environ.get(k)]
            if missing:
                logger.warning("Missing Postgres env vars %s -- skipping DB load, "
                               "will still write CSVs.", missing)
            else:
                try:
                    engine = build_engine()
                    apply_schema(engine)
                    n_genres = upsert_genres(engine, genres_df)
                    logger.info("Upserted %d genres.", n_genres)
                    n_loaded, n_links = upsert_films_and_links(engine, films_df, links_df)
                    logger.info("Upserted %d films + %d film-genre links.",
                                n_loaded, n_links)
                    validate_load(engine, films_df)                 # quality check #7
                    db_loaded = True
                except SQLAlchemyError as exc:
                    logger.warning("PostgreSQL load failed (%s) -- continuing to "
                                   "CSV export so the pipeline still completes.",
                                   exc.__class__.__name__)
                    logger.debug("DB error detail: %s", exc)

        # --- always export the analytics-ready CSVs ---
        export_csv(enriched, CSV_FILMS, "films_for_powerbi")
        export_csv(summary, CSV_SUMMARY, "genre_decade_summary")

        # --- 5. VERIFY / reconcile ---
        banner("STAGE 5/5  VERIFY  (row-count reconciliation)")
        logger.info("  extracted (raw films)      : %d", n_extracted)
        logger.info("  cleaned   (passed QA)      : %d", n_clean)
        logger.info("  loaded    (films in DB)    : %s",
                    n_loaded if db_loaded else "skipped")
        logger.info("  CSV rows  (films export)   : %d", len(enriched))
        if len(enriched) != n_clean:
            raise DataQualityError(
                f"CSV row count {len(enriched)} != cleaned count {n_clean}.")
        logger.info("  [PASS] CSV reconciliation  enriched CSV == cleaned films")

        elapsed = time.perf_counter() - started
        banner(f"ETL COMPLETE in {elapsed:.1f}s  "
               f"(DB load: {'yes' if db_loaded else 'skipped'})")
        return 0

    except DataQualityError as exc:
        logger.error("PIPELINE ABORTED -- data quality: %s", exc)
        return 2
    except Exception as exc:                              # noqa: BLE001 (top-level guard)
        logger.exception("PIPELINE FAILED -- unexpected error: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
