"""Peek at the 2025 and 2026 data we just added."""
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
        for year in (2025, 2026):
            print(f"\n=== Top 10 by revenue, release_year = {year} ===")
            rows = conn.execute(text("""
                SELECT title, release_date, budget, revenue, vote_average, vote_count
                FROM films
                WHERE release_year = :y
                ORDER BY revenue DESC
                LIMIT 10
            """), {"y": year}).all()
            for r in rows:
                print(f"  {r.title:<42} {r.release_date}  "
                      f"${r.revenue/1e6:>7.1f}M  rating={r.vote_average}  votes={r.vote_count}")

        print("\n=== 2026 oddity check: what's in the data for an incomplete year? ===")
        rows = conn.execute(text("""
            SELECT release_date, COUNT(*) AS n, MIN(revenue), MAX(revenue)
            FROM films
            WHERE release_year = 2026
            GROUP BY release_date
            ORDER BY release_date
        """)).all()
        for r in rows:
            print(f"  {r.release_date}  n={r.n}  rev_range=${r.min/1e6:.1f}M - ${r.max/1e6:.1f}M")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
