# Box Office vs. Ratings

A reproducible Python ETL pipeline that extracts ~6,000 films from the TMDB API
(2000–2026), cleans and validates them, derives analytics metrics, loads them
into a 3NF PostgreSQL database, and exports analytics-ready CSVs for Power BI.

**Question:** Do films that earn more also rate higher? Do certain genres show
consistent gaps between revenue and rating, and does that change across decades?

---

## The pipeline — `etl_pipeline.py`

A single, self-contained script that runs the full pipeline end-to-end with no
manual steps:

```
TMDB API ─► clean + normalize ─► validate (7 QA checks) ─► PostgreSQL (3NF)
                                                       └─► analytics CSVs ─► Power BI
```

| Stage | What it does |
|-------|--------------|
| **Extract** | Reads cached `data/raw/*.json` by default (fast, deterministic). `--refresh` re-pulls from the live TMDB API (two-stage: `/discover/movie` paginated by year → `/movie/{id}` for budget/revenue), with retry + backoff. Missing cache auto-falls back to a live pull. |
| **Transform** | pandas cleaning, dtype coercion, dedupe, and derived metrics: `profit`, `roi`, `profit_margin`, `budget_tier`, `performance` (hit/flop), `decade`. Builds a genre × decade aggregation layer. |
| **Validate** | 7 data-quality checks — API response, null required fields, duplicate keys, dtypes, range bounds, referential integrity, and row-count reconciliation — each logged PASS/FAIL. Critical failures abort the run. |
| **Load** | Idempotent `INSERT … ON CONFLICT` upserts into `films` / `genres` / `film_genres`. Re-running never duplicates (incremental load). Always writes the CSVs; if Postgres is unreachable it logs a warning and still produces the CSVs. |

Outputs:
- `data/films_for_powerbi.csv` — one enriched row per film (the dashboard source).
- `data/genre_decade_summary.csv` — the aggregation layer (rating vs ROI by genre/decade).
- `logs/etl_pipeline.log` — timestamped run log.

A captured example run (including post-load database queries) is in
[`sample_run_output.txt`](sample_run_output.txt).

## Quick start

```powershell
# 1. Activate the virtual environment
.\venv\Scripts\Activate.ps1

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure secrets: copy the template and fill in your values
Copy-Item .env.example .env   # then edit .env (TMDB token + Postgres creds)

# 4. Create the database once (if it doesn't exist)
#    psql -U postgres -c "CREATE DATABASE boxoffice;"

# 5. Run the full pipeline
python etl_pipeline.py
```

### Run options

```powershell
python etl_pipeline.py            # cached extract → PostgreSQL + CSV (default)
python etl_pipeline.py --refresh  # re-pull from the live TMDB API first
python etl_pipeline.py --csv-only # skip PostgreSQL, only write the CSVs
```

The default run finishes in a few seconds against the cached data. The pipeline
is idempotent — running it repeatedly produces the same database state.

## Configuration (`.env`)

| Variable | Purpose |
|----------|---------|
| `TMDB_READ_ACCESS_TOKEN` | TMDB v4 bearer token (only needed for `--refresh` or a cold cache) |
| `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD` | PostgreSQL connection |

Secrets live in `.env` (gitignored). `.env.example` is the committed template.

## Database schema (3NF)

Three tables plus one analytics view — fully documented in
[`schema_documentation.md`](schema_documentation.md) with an ER diagram.

| Table | Purpose |
|-------|---------|
| `films` | One row per movie (financials, runtime, ratings). PK `film_id`, unique `tmdb_id`. |
| `genres` | TMDB genre lookup (19 rows). |
| `film_genres` | M:N bridge resolving films ↔ genres. |
| `v_films_enriched` | View adding `profit`, `roi`, and a comma-joined genre list. |

The DDL is inlined in `etl_pipeline.py` as `SCHEMA_DDL`, so the live database, the
inlined DDL, and the ERD all describe the same schema.

## Current load

- **6,008 films** (2000–2026 YTD) · **19 genres** · **15,811 film-genre links**
- Genre × decade summary: **56 rows**

## Repo layout

```
etl_pipeline.py           Full single-file ETL pipeline (extract→transform→validate→load)
schema_documentation.md   Schema docs + ER diagram
load_script.py            Simple initial PostgreSQL load script
src/extract/              Standalone TMDB fetcher (two-stage: discover + movie detail)
data/raw/                 Extracted TMDB JSON (gitignored, regeneratable)
data/                     Exported CSVs (gitignored, regeneratable)
logs/                     Run logs (gitignored)
tests/                    Helper / verification scripts
sample_run_output.txt     Example run output + post-load database queries
requirements.txt          Python dependencies
```

## Tech stack

- Python 3 · `requests` · `pandas` · `python-dotenv`
- SQLAlchemy 2.0 + psycopg2 (DB driver)
- PostgreSQL 17 (local)
- Power BI (dashboard layer)

## Data source

Film data is sourced from [The Movie Database (TMDB) API](https://developer.themoviedb.org/).
This product uses the TMDB API but is not endorsed or certified by TMDB.
