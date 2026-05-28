"""Quick verification queries against the loaded database.
Confirms the schema supports the project's analytical questions.
"""
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
        print("=" * 70)
        print("Top 5 highest-grossing films in the DB")
        print("=" * 70)
        rows = conn.execute(text("""
            SELECT title, release_year, revenue, vote_average
            FROM films
            ORDER BY revenue DESC
            LIMIT 5
        """)).all()
        for r in rows:
            print(f"  {r.title:<40} ({r.release_year})  "
                  f"${r.revenue/1e9:.2f}B  rating={r.vote_average}")

        print("\n" + "=" * 70)
        print("Average ROI by genre (top 10)")
        print("=" * 70)
        rows = conn.execute(text("""
            SELECT g.name AS genre,
                   ROUND(AVG(f.revenue::numeric / f.budget), 2) AS avg_roi,
                   COUNT(*) AS n_films
            FROM films f
            JOIN film_genres fg ON fg.film_id = f.film_id
            JOIN genres      g  ON g.genre_id = fg.genre_id
            GROUP BY g.name
            ORDER BY avg_roi DESC
            LIMIT 10
        """)).all()
        print(f"  {'genre':<20} {'avg_roi':>10} {'n_films':>10}")
        for r in rows:
            print(f"  {r.genre:<20} {float(r.avg_roi):>10.2f} {r.n_films:>10}")

        print("\n" + "=" * 70)
        print("Revenue vs rating: 'films that audiences loved but lost money'")
        print("(vote_average >= 7.5 AND revenue < budget) — top 5 by rating")
        print("=" * 70)
        rows = conn.execute(text("""
            SELECT title, release_year, vote_average, budget, revenue,
                   (revenue - budget) AS profit
            FROM films
            WHERE vote_average >= 7.5
              AND revenue < budget
            ORDER BY vote_average DESC, vote_count DESC
            LIMIT 5
        """)).all()
        for r in rows:
            print(f"  {r.title:<40} ({r.release_year})  rating={r.vote_average}  "
                  f"loss=${(r.budget-r.revenue)/1e6:.1f}M")

        print("\n" + "=" * 70)
        print("Film counts per year (sanity check)")
        print("=" * 70)
        rows = conn.execute(text("""
            SELECT release_year, COUNT(*) AS n
            FROM films
            GROUP BY release_year
            ORDER BY release_year
        """)).all()
        for r in rows:
            bar = "#" * (r.n // 10)
            print(f"  {r.release_year}  {r.n:>4}  {bar}")

        print("\n" + "=" * 70)
        print("Constraint check: any rows violating CHECK constraints?")
        print("=" * 70)
        bad = conn.execute(text("""
            SELECT COUNT(*) FROM films
            WHERE budget <= 0 OR revenue <= 0 OR vote_count < 100
        """)).scalar_one()
        print(f"  rows violating constraints: {bad} (expected 0)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
