# parquet_storage.py

from pathlib import Path

import pandas as pd

from data_loader import load_and_validate_market_data


def write_parquet_partitioned(
    market_df: pd.DataFrame,
    out_dir: str = "market_data",
) -> None:
    """
    Convert market_df to Parquet, partitioned by ticker.
    Requires pyarrow or fastparquet installed.
    """
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # Pandas will create folder structure like market_data/ticker=AAPL/...
    market_df.to_parquet(
        out_path,
        engine="pyarrow",
        partition_cols=["ticker"],
        index=False,
    )


def load_parquet_for_ticker(
    base_dir: str, ticker: str
) -> pd.DataFrame:
    """
    Load Parquet data for a single ticker from a partitioned directory.
    """
    base_path = Path(base_dir)
    # Partition directory name pattern: ticker=XXX
    ticker_dir = base_path / f"ticker={ticker}"
    df = pd.read_parquet(ticker_dir, engine="pyarrow")
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


# ===== Parquet Tasks =====

def parquet_task1_aapl_rolling_5min(
    base_dir: str = "market_data",
) -> pd.DataFrame:
    """
    Task 1 (Parquet):
    Load all data for AAPL and compute 5-minute rolling average of close price.
    (Data is 1-minute bars → 5-row rolling window.)
    """
    df = load_parquet_for_ticker(base_dir, "AAPL")
    df = df.sort_values("timestamp")
    df["rolling_5min_close"] = (
        df["close"].rolling(window=5).mean()
    )
    return df


def parquet_task2_rolling_5d_vol(
    base_dir: str = "market_data",
) -> pd.DataFrame:
    """
    Task 2 (Parquet):
    Compute 5-day rolling volatility (std dev) of daily returns for each ticker.

    NOTE: With only 5 trading days in this dataset, a strict
    window=5, min_periods=5 will give NaN for all rows; you can
    set min_periods=1 if you want to see numbers in this toy example.
    """
    base_path = Path(base_dir)
    all_dfs = []
    for part in base_path.glob("ticker=*"):
        ticker = part.name.split("=", 1)[1]  # "AAPL"

        df = pd.read_parquet(part, engine="pyarrow")

        df["ticker"] = ticker

        all_dfs.append(df)


    full = pd.concat(all_dfs, ignore_index=True)
    full["timestamp"] = pd.to_datetime(full["timestamp"])
    full = full.sort_values(["ticker", "timestamp"])

    # Daily close per ticker (last close of each day)
    full["date"] = full["timestamp"].dt.date
    daily_close = (
        full.groupby(["ticker", "date"])["close"]
        .last()
        .reset_index()
    )

    daily_close["return"] = daily_close.groupby("ticker")["close"].pct_change()

    # strict 5-day rolling: will be NaN here because we only have 4 returns
    daily_close["vol_5d"] = (
        daily_close.groupby("ticker")["return"]
        .rolling(window=5, min_periods=5)
        .std()
        .reset_index(level=0, drop=True)
    )

    # if you want some numbers in this tiny sample:
    daily_close["vol_5d_min1"] = (
        daily_close.groupby("ticker")["return"]
        .rolling(window=5, min_periods=1)
        .std()
        .reset_index(level=0, drop=True)
    )

    return daily_close


def parquet_task3_compare_sqlite_parquet(
    sqlite_db_path: str = "market_data.db",
    parquet_dir: str = "market_data",
) -> dict:
    """
    Task 3 (Parquet):
    Compare query time and file size between SQLite3 and Parquet for
    'retrieve all data for AAPL between dates' (or TSLA, your choice).

    Returns a dict with sizes (bytes) and measured runtimes (seconds).
    """
    import os
    import sqlite3
    import time

    from sqlite_storage import get_tsla_between_dates  # or AAPL variant

    sizes = {
        "sqlite_db_bytes": os.path.getsize(sqlite_db_path),
        "csv_bytes": os.path.getsize("market_data_multi.csv"),
    }

    # Parquet size = sum of all parquet files
    parquet_bytes = 0
    for root, _, files in os.walk(parquet_dir):
        for f in files:
            if f.endswith(".parquet"):
                parquet_bytes += os.path.getsize(os.path.join(root, f))
    sizes["parquet_bytes"] = parquet_bytes

    # Timing SQLite query (same as Task 1 but timed)
    conn = sqlite3.connect(sqlite_db_path)
    t0 = time.perf_counter()
    _ = get_tsla_between_dates(conn)  # or AAPL variant
    t1 = time.perf_counter()
    sizes["sqlite_query_seconds"] = t1 - t0

    # Timing Parquet load + filter using pandas
    t2 = time.perf_counter()
    df_tsla = load_parquet_for_ticker(parquet_dir, "TSLA")
    # example: full date range filter
    df_tsla = df_tsla[
        (df_tsla["timestamp"].dt.date >= pd.to_datetime("2025-11-17").date())
        & (df_tsla["timestamp"].dt.date <= pd.to_datetime("2025-11-18").date())
    ]
    t3 = time.perf_counter()
    sizes["parquet_query_seconds"] = t3 - t2

    return sizes


if __name__ == "__main__":
    market_df, tickers_df = load_and_validate_market_data(
        "market_data_multi.csv", "tickers.csv"
    )
    write_parquet_partitioned(market_df, "market_data")

    aapl = parquet_task1_aapl_rolling_5min("market_data")
    print(aapl.head())

    daily = parquet_task2_rolling_5d_vol("market_data")
    print(daily)
