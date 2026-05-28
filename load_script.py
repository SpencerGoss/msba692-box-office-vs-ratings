"""Initial PostgreSQL Load Script — Box Office vs. Ratings.

Loads TMDB extract JSON into a 3NF PostgreSQL schema and exports a
denormalized CSV for the Power BI dashboard. Idempotent — every insert
uses ON CONFLICT, so re-running the script never duplicates rows.

Usage:
    python load_script.py

Requires:
    - PostgreSQL 17 running locally with database `boxoffice` created.
    - A .env file with PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD.
    - TMDB extract JSON at data/raw/films_YYYY.json and data/raw/genres.json
      (produced by src/extract/fetch_tmdb.py).
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------------
# Schema DDL — inlined so this file is self-contained (no external .sql).
# Three tables (films + genres + film_genres) + one analysis-ready view.
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Paths — resolved relative to this script so the layout is portable.
# ---------------------------------------------------------------------------
HERE        = Path(__file__).resolve().parent
RAW_DIR     = HERE / "data" / "raw"
CSV_EXPORT  = HERE / "data" / "films_for_powerbi.csv"


def build_engine() -> Engine:
    url = (
        f"postgresql+psycopg2://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}"
        f"@{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DATABASE']}"
    )
    return create_engine(url, future=True)


def _strip_sql_comments(stmt: str) -> str:
    return "\n".join(
        line for line in stmt.splitlines() if not line.strip().startswith("--")
    ).strip()


def apply_schema(engine: Engine) -> None:
    """Apply the inlined DDL. Safe to re-run."""
    with engine.begin() as conn:
        for raw in SCHEMA_DDL.split(";"):
            stmt = _strip_sql_comments(raw)
            if stmt:
                conn.execute(text(stmt))


def upsert_genres(engine: Engine, genres: Iterable[dict]) -> int:
    sql = text("""
        INSERT INTO genres (genre_id, name) VALUES (:genre_id, :name)
        ON CONFLICT (genre_id) DO UPDATE SET name = EXCLUDED.name
    """)
    rows = [{"genre_id": g["id"], "name": g["name"]} for g in genres]
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def parse_release_date(s):
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def upsert_film(conn, film: dict) -> int:
    release = parse_release_date(film.get("release_date"))
    row = {
        "tmdb_id":           film["id"],
        "imdb_id":           film.get("imdb_id"),
        "title":             film.get("title") or film.get("original_title") or "",
        "release_date":      release,
        "release_year":      release.year if release else None,
        "budget":            int(film["budget"]),
        "revenue":           int(film["revenue"]),
        "runtime":           film.get("runtime"),
        "vote_count":        int(film.get("vote_count", 0)),
        "vote_average":      film.get("vote_average"),
        "popularity":        film.get("popularity"),
        "original_language": (film.get("original_language") or "")[:2] or None,
    }
    sql = text("""
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
            imdb_id           = EXCLUDED.imdb_id,
            title             = EXCLUDED.title,
            release_date      = EXCLUDED.release_date,
            release_year      = EXCLUDED.release_year,
            budget            = EXCLUDED.budget,
            revenue           = EXCLUDED.revenue,
            runtime           = EXCLUDED.runtime,
            vote_count        = EXCLUDED.vote_count,
            vote_average      = EXCLUDED.vote_average,
            popularity        = EXCLUDED.popularity,
            original_language = EXCLUDED.original_language,
            updated_at        = NOW()
        RETURNING film_id
    """)
    return conn.execute(sql, row).scalar_one()


def upsert_film_genres(conn, film_id: int, genre_ids: Iterable[int]) -> None:
    sql = text("""
        INSERT INTO film_genres (film_id, genre_id) VALUES (:film_id, :genre_id)
        ON CONFLICT DO NOTHING
    """)
    rows = [{"film_id": film_id, "genre_id": gid} for gid in genre_ids]
    if rows:
        conn.execute(sql, rows)


def genre_ids_for(film: dict) -> list:
    """Prefer detail-endpoint genres[] (objects); fall back to discover's genre_ids[]."""
    detail_genres = film.get("genres") or []
    if detail_genres and isinstance(detail_genres[0], dict):
        return [g["id"] for g in detail_genres]
    return list(film.get("genre_ids") or [])


def load_films(engine: Engine):
    films_total = 0
    links_total = 0
    json_files = sorted(RAW_DIR.glob("films_*.json"))
    if not json_files:
        print(f"WARNING: no films_*.json found in {RAW_DIR}.", file=sys.stderr)
        return 0, 0
    for path in json_files:
        films = json.loads(path.read_text(encoding="utf-8"))
        with engine.begin() as conn:
            for film in films:
                fid = upsert_film(conn, film)
                gids = genre_ids_for(film)
                upsert_film_genres(conn, fid, gids)
                links_total += len(gids)
        films_total += len(films)
        print(f"  loaded {len(films):>4} films from {path.name}")
    return films_total, links_total


def report_counts(engine: Engine) -> None:
    with engine.connect() as conn:
        for table in ("films", "genres", "film_genres"):
            n = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one()
            print(f"  {table:<12} {n:>7} rows")


def export_csv(engine: Engine) -> None:
    df = pd.read_sql(
        "SELECT * FROM v_films_enriched ORDER BY release_year, title", engine
    )
    CSV_EXPORT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CSV_EXPORT, index=False, encoding="utf-8")
    print(f"  exported {len(df)} rows -> {CSV_EXPORT}")


def main() -> int:
    load_dotenv()
    required = ("PG_HOST", "PG_PORT", "PG_DATABASE", "PG_USER", "PG_PASSWORD")
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        print(f"ERROR: missing env vars: {', '.join(missing)}", file=sys.stderr)
        return 1

    engine = build_engine()

    print("Applying schema...")
    apply_schema(engine)

    print("Upserting genres...")
    genres = json.loads((RAW_DIR / "genres.json").read_text(encoding="utf-8"))
    n_genres = upsert_genres(engine, genres)
    print(f"  {n_genres} genres processed")

    print("Loading films + film_genres...")
    n_films, n_links = load_films(engine)
    print(f"  {n_films} films, {n_links} film-genre links processed")

    print("\nFinal table counts:")
    report_counts(engine)

    print("\nExporting denormalized CSV for Power BI...")
    export_csv(engine)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
