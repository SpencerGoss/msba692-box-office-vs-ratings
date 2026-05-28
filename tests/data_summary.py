"""How much data do we have? Quick summary of the loaded DB."""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def main() -> int:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    url = (
        f"postgresql+psycopg2://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}"
        f"@{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DATABASE']}"
    )
    engine = create_engine(url, future=True)

    with engine.connect() as conn:
        n_films, n_genres, n_links = conn.execute(text("""
            SELECT
              (SELECT COUNT(*) FROM films),
              (SELECT COUNT(*) FROM genres),
              (SELECT COUNT(*) FROM film_genres)
        """)).one()
        print(f"Films:        {n_films:>7,}")
        print(f"Genres:       {n_genres:>7,}")
        print(f"Film-genres:  {n_links:>7,}   ({n_links/n_films:.2f} genres per film on avg)")

        row = conn.execute(text("""
            SELECT MIN(release_year) AS y_min, MAX(release_year) AS y_max,
                   SUM(budget)  AS sum_budget,
                   SUM(revenue) AS sum_revenue,
                   ROUND(AVG(vote_average)::numeric, 2) AS avg_rating,
                   ROUND(AVG(runtime)::numeric, 0)      AS avg_runtime,
                   COUNT(DISTINCT original_language)    AS n_languages
            FROM films
        """)).one()
        print(f"\nYear range:           {row.y_min} - {row.y_max}")
        print(f"Total budget logged:  ${float(row.sum_budget)/1e9:>8,.1f}B")
        print(f"Total revenue logged: ${float(row.sum_revenue)/1e9:>8,.1f}B")
        print(f"Total industry profit:${float(row.sum_revenue-row.sum_budget)/1e9:>8,.1f}B")
        print(f"Avg vote_average:     {float(row.avg_rating):>9}")
        print(f"Avg runtime:          {int(row.avg_runtime):>4} minutes")
        print(f"Languages represented:{row.n_languages:>4}")

        print("\n--- columns we have per film (14 total) ---")
        cols = conn.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'films'
            ORDER BY ordinal_position
        """)).all()
        for c in cols:
            print(f"  {c.column_name:<20} {c.data_type}")

        print("\n--- films per language (top 8) ---")
        rows = conn.execute(text("""
            SELECT original_language, COUNT(*) AS n
            FROM films
            GROUP BY original_language
            ORDER BY n DESC
            LIMIT 8
        """)).all()
        for r in rows:
            print(f"  {r.original_language:<4} {r.n:>5}")

        print("\n--- genre coverage (all 19) ---")
        rows = conn.execute(text("""
            SELECT g.name, COUNT(*) AS n_films
            FROM genres g
            LEFT JOIN film_genres fg ON fg.genre_id = g.genre_id
            GROUP BY g.name
            ORDER BY n_films DESC
        """)).all()
        for r in rows:
            print(f"  {r.name:<18} {r.n_films:>5}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
