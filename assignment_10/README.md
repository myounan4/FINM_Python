# FINM 32500 – Assignment 10: Market Data Storage and Querying

This repository contains a minimal implementation of the assignment:

> *Market Data Storage and Querying with SQLite3 and Parquet*

It demonstrates how to ingest multi‑ticker OHLCV data from CSV, store it in a
normalized SQLite3 schema, and (on a local machine) convert it to Parquet for
columnar analytics.

## File Overview

- `market_data_multi.csv` – Intraday OHLCV data for multiple tickers.
- `tickers.csv` – Ticker metadata (ticker_id, symbol, name, exchange).
- `schema.sql` – SQL DDL for the `tickers` and `prices` tables.
- `market_data.db` – SQLite3 database populated with the sample data.
- `market_data/` – Directory for Parquet output (partitioned by ticker).
  - In this environment, only a `README_PARQUET.md` placeholder is created.
  - On your local machine you should generate real Parquet files (see below).
- `query_tasks.md` – SQL / Parquet task descriptions, results, and performance notes.
- `comparison.md` – Discussion of SQLite3 vs Parquet and use cases.
- `tests/` – Unit tests for ingestion and querying (pytest‑style).

You are expected to add the Python modules from your codebase, such as:

- `data_loader.py`
- `sqlite_storage.py`
- `parquet_storage.py`

and wire them up as needed for your final submission.

## Setup Instructions

1. **Create and activate a virtual environment (optional but recommended):**

```bash
python -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate
```

2. **Install dependencies:**

At minimum you will need:

```bash
pip install pandas pytest
```

To enable Parquet support locally, also install **pyarrow** (recommended):

```bash
pip install pyarrow
```

3. **Generate the SQLite database (if not already present):**

If you use the provided scripts from the assignment solution, a typical flow is:

```bash
python data_loader.py        # optional: just to validate data
python sqlite_storage.py     # should create and populate market_data.db
```

Confirm that `market_data.db` exists in the project root and inspect it with
a tool like `sqlite3` or `DB Browser for SQLite` if desired.

4. **Generate Parquet files (on your machine):**

Once `pyarrow` is installed and you have your `parquet_storage.py` module,
run something like:

```bash
python parquet_storage.py
```

This should create a directory structure similar to:

```text
market_data/
  ticker=AAPL/part-....parquet
  ticker=AMZN/part-....parquet
  ...
```

Make sure `market_data/` is included in your submission as requested.

5. **Run Unit Tests:**

The `tests/` folder contains example pytest tests that assume you have
implemented:

- `load_and_validate_market_data` in `data_loader.py`
- `init_sqlite_db`, `populate_db`, and query helpers in `sqlite_storage.py`
- Parquet helpers in `parquet_storage.py`

Run the tests with:

```bash
pytest -q
```

You should see all tests pass after wiring your modules correctly.

## Notes

- The provided `query_tasks.md` includes both the SQL used for each task and
  concise summaries of the resulting outputs.
- The `comparison.md` file can be used directly (or adapted) for your format
  comparison and use‑case discussion.
- Make sure to keep paths consistent with your project layout (e.g., using
  a `data/` subdirectory vs. the project root).

Adjust filenames and import paths as needed to match the structure of your
own GitHub repo.
