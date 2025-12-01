# SQLite3 vs Parquet: Format Comparison and Use Case Discussion

## Storage Model and Layout

**SQLite3** is a lightweight relational database engine that stores data in
tables organized by rows. In this assignment we use a normalized schema with
a `tickers` dimension table and a `prices` fact table. This is a good match
for:

- Referential integrity (foreign keys between tickers and prices)
- Relational joins (e.g., joining prices to metadata, other tables)
- Ad‑hoc SQL queries that mix time‑series filters with other conditions

**Parquet** is a columnar file format. Instead of storing complete rows
together, Parquet stores each column in contiguous chunks on disk, optionally
compressed and encoded. In a typical analytics workflow this provides:

- Faster scans when you only need a subset of columns (e.g., `timestamp`,
  `close`, and `volume`)
- Better compression ratios because each column has homogeneous data
- Natural compatibility with pandas, Spark, and other data‑frame engines

In short: SQLite3 is row‑oriented and relational; Parquet is column‑oriented
and analytics‑friendly.

## File Size and Compression

In our small sample, the uncompressed CSV (`market_data_multi.csv`) is about
559 KB, while the SQLite database (`market_data.db`) is about 688 KB. The
SQLite file is larger because it includes table metadata, indexes, and some
overhead for the B‑tree storage structure, but it also allows efficient SQL
queries without loading the entire dataset into memory.

Parquet (with compression enabled) would typically produce a file set that is:

- Smaller than the CSV (thanks to column‑wise compression)
- Often comparable to or smaller than the SQLite file for pure time‑series
  analytics

For very large historical datasets (months or years of intraday bars), Parquet
usually wins on disk footprint, especially when partitioned by ticker and/or
date.

## Query Speed and Access Patterns

For the TSLA range query (*2025‑11‑17 to 2025‑11‑18*), both SQLite and the
pandas/Parquet‑style logic return 782 rows. The measured query times in this
environment were on the order of a few milliseconds for both approaches:

- SQLite3: ~0.006 s for a `SELECT` with `JOIN` + `DATE` filter
- In‑memory pandas filter (simulating Parquet): ~0.002 s

On a real system with Parquet files and `pyarrow`, the relative performance
depends on the access pattern:

- **SQLite3 is strong when:**
  - You need many small, selective queries with complex predicates
  - You frequently join `prices` to other relational tables (e.g., tick metadata,
    corporate actions, signals)
  - You benefit from indexes on (`ticker_id`, `timestamp`) or other keys

- **Parquet is strong when:**
  - You scan large amounts of historical data for analytics or research
  - You only need a subset of columns (e.g., `close` and `volume`)
  - You can leverage partition pruning (e.g., `ticker=TSLA/` directories, or
    date partitions) to avoid reading irrelevant files

In a backtesting or research context, Parquet files are often loaded into
pandas or Spark data frames once, and then reused for many vectorized
operations. SQLite, by contrast, excels when you issue many discrete SQL
queries from a trading engine or research notebook.

## Ease of Integration with Analytics Workflows

**SQLite3**:

- Integrates naturally with Python via `sqlite3` and `pandas.read_sql_query`
- Uses familiar SQL syntax, which is ideal for expressing time‑series filters,
  aggregations, and joins
- Can be embedded directly into a trading application as a single `.db` file
- Makes it easy to validate your results with simple ad‑hoc queries

**Parquet**:

- Integrates extremely well with pandas (`read_parquet`) and PySpark
- Encourages a “data‑frame first” workflow: load Parquet once, then run many
  vectorized operations (rolling windows, group‑bys, factor calculations)
- Plays nicely with distributed systems (Spark, Dask) when your dataset grows
  beyond a single machine
- Is often the default format in data lakes and research environments

In short, SQLite lets you treat market data as a relational database; Parquet
lets you treat it as a columnar analytics table or data frame.

## Use Cases in Trading Systems

### Backtesting

For **backtesting and research**, Parquet is usually the preferred format:

- You often need to stream through years of historical bars for many tickers.
- Computations (P&L, factor returns, rolling correlations) are vectorized and
  column‑centric.
- Partitioned Parquet (e.g., `ticker=XYZ/date=YYYY-MM-DD/`) lets you read just
  the subset of data you need while keeping good compression.

SQLite can still be used, especially if you want clean relational joins to
other tables (e.g., events, signals), but performance may lag behind Parquet
for large scans.

### Live Trading / Production

For **live or near‑real‑time trading systems**, SQLite can be attractive as a
lightweight embedded database:

- Simple deployment: a single `.db` file with no external database server
- Good support for transactional writes (e.g., appending new bars, storing
  orders and executions)
- Native SQL support for risk checks, P&L queries, and reporting

Parquet is less suited to highly dynamic, frequently updated datasets because
it is a file format, not a transactional database. It shines when you append
data in larger batches (e.g., end‑of‑day bar dumps) and then run read‑heavy
analytics.

### Hybrid Approach

A common real‑world pattern is to **combine both formats**:

- Store *raw, high‑volume historical data* in Parquet (partitioned, compressed)
  for research and backtesting.
- Maintain a smaller **SQLite (or other relational) database** for:
  - Symbol metadata
  - Model parameters
  - Strategy configurations
  - Aggregated or recent time‑series snapshots

Your Python code can then:

1. Pull large historical windows from Parquet into pandas for heavy analytics.
2. Use SQLite for lookups, joins to metadata, and fast configuration or
   reporting queries.

## Summary

- Use **SQLite3** when you want a small, embedded, relational database that is
  easy to query with SQL and integrates directly with applications.
- Use **Parquet** when you care about columnar analytics, compression, and
  high‑throughput scans over large blocks of historical data.
- In practice, a hybrid approach (Parquet for raw history, SQLite for metadata
  and transactional state) often gives you the best of both worlds for trading
  systems and financial research.
