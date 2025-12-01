# sqlite_storage.py

import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd

from data_loader import load_and_validate_market_data


def init_sqlite_db(
    db_path: str = "market_data.db",
    schema_path: str = "schema.sql",
) -> sqlite3.Connection:
    """
    Create SQLite database and tables from schema.sql.
    """
    db_path = Path(db_path)
    if db_path.exists():
        db_path.unlink()  # start clean each run

    conn = sqlite3.connect(db_path)
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()
    conn.executescript(schema_sql)
    conn.commit()
    return conn


def populate_db(
    conn: sqlite3.Connection,
    market_df: pd.DataFrame,
    tickers_df: pd.DataFrame,
) -> None:
    """
    Insert tickers and OHLCV data into SQLite DB, using the normalized schema.
    """
    # Insert tickers directly (ticker_id, symbol, name, exchange)
    tickers_df.to_sql("tickers", conn, if_exists="append", index=False)

    # Map symbol -> ticker_id
    symbol_to_id = dict(
        conn.execute("SELECT symbol, ticker_id FROM tickers").fetchall()
    )

    prices_df = market_df.copy()
    prices_df["ticker_id"] = prices_df["ticker"].map(symbol_to_id)
    prices_df = prices_df.drop(columns=["ticker"])

    # Ensure timestamp is ISO string
    prices_df["timestamp"] = prices_df["timestamp"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    prices_df[
        ["timestamp", "ticker_id", "open", "high", "low", "close", "volume"]
    ].to_sql("prices", conn, if_exists="append", index=False)
    conn.commit()


# ========== QUERY TASK HELPERS ==========

def get_tsla_between_dates(
    conn: sqlite3.Connection,
    start_date: str = "2025-11-17",
    end_date: str = "2025-11-18",
) -> pd.DataFrame:
    """
    Task 1 (SQLite):
    Retrieve all data for TSLA between start_date and end_date (inclusive, by date).
    """
    query = """
    SELECT p.timestamp,
           t.symbol,
           p.open, p.high, p.low, p.close, p.volume
    FROM prices p
    JOIN tickers t ON p.ticker_id = t.ticker_id
    WHERE t.symbol = 'TSLA'
      AND DATE(p.timestamp) BETWEEN ? AND ?
    ORDER BY p.timestamp;
    """
    return pd.read_sql_query(query, conn, params=(start_date, end_date))


def get_avg_daily_volume_per_ticker(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Task 2 (SQLite):
    Calculate average daily volume per ticker.
    """
    query = """
    WITH daily AS (
      SELECT t.symbol,
             DATE(p.timestamp) AS trade_date,
             SUM(p.volume) AS daily_volume
      FROM prices p
      JOIN tickers t ON p.ticker_id = t.ticker_id
      GROUP BY t.symbol, DATE(p.timestamp)
    )
    SELECT symbol,
           AVG(daily_volume) AS avg_daily_volume
    FROM daily
    GROUP BY symbol
    ORDER BY symbol;
    """
    return pd.read_sql_query(query, conn)


def get_top3_tickers_by_total_return(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Task 3 (SQLite):
    Identify the top 3 tickers by total return over full period.
    Return = (last_close / first_close - 1).
    """
    query = """
    WITH ordered AS (
      SELECT p.*, t.symbol,
             ROW_NUMBER() OVER (
               PARTITION BY p.ticker_id
               ORDER BY p.timestamp ASC
             ) AS rn_asc,
             ROW_NUMBER() OVER (
               PARTITION BY p.ticker_id
               ORDER BY p.timestamp DESC
             ) AS rn_desc
      FROM prices p
      JOIN tickers t ON p.ticker_id = t.ticker_id
    ),
    first_last AS (
      SELECT symbol,
             MAX(CASE WHEN rn_asc = 1 THEN close END) AS first_close,
             MAX(CASE WHEN rn_desc = 1 THEN close END) AS last_close
      FROM ordered
      GROUP BY symbol
    )
    SELECT symbol,
           first_close,
           last_close,
           (last_close - first_close) / first_close AS total_return
    FROM first_last
    ORDER BY total_return DESC
    LIMIT 3;
    """
    return pd.read_sql_query(query, conn)


def get_first_last_price_per_ticker_per_day(
    conn: sqlite3.Connection,
) -> pd.DataFrame:
    """
    Task 4 (SQLite):
    First and last trade price (close) for each ticker per day.
    """
    query = """
    WITH ordered AS (
      SELECT p.*, t.symbol,
             DATE(p.timestamp) AS trade_date,
             ROW_NUMBER() OVER (
               PARTITION BY t.symbol, DATE(p.timestamp)
               ORDER BY p.timestamp ASC
             ) AS rn_asc,
             ROW_NUMBER() OVER (
               PARTITION BY t.symbol, DATE(p.timestamp)
               ORDER BY p.timestamp DESC
             ) AS rn_desc
      FROM prices p
      JOIN tickers t ON p.ticker_id = t.ticker_id
    )
    SELECT symbol,
           trade_date,
           MAX(CASE WHEN rn_asc = 1 THEN close END) AS first_close,
           MAX(CASE WHEN rn_desc = 1 THEN close END) AS last_close
    FROM ordered
    GROUP BY symbol, trade_date
    ORDER BY symbol, trade_date;
    """
    return pd.read_sql_query(query, conn)


if __name__ == "__main__":
    # End-to-end smoke test
    market_df, tickers_df = load_and_validate_market_data(
        "market_data_multi.csv", "tickers.csv"
    )
    conn = init_sqlite_db("market_data.db", "schema.sql")
    populate_db(conn, market_df, tickers_df)

    tsla = get_tsla_between_dates(conn)
    print("TSLA rows:", len(tsla))

    print(get_avg_daily_volume_per_ticker(conn))
    print(get_top3_tickers_by_total_return(conn))
    print(get_first_last_price_per_ticker_per_day(conn).head())
