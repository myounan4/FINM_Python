import pandas as pd

from data_loader import load_and_validate_market_data
from parquet_storage import (
    write_parquet_partitioned,
    parquet_task1_aapl_rolling_5min,
    parquet_task2_rolling_5d_vol,
)


def test_parquet_partitioning_and_queries(tmp_path):
    """Test that parquet files can be written and basic queries run."""
    market_df, _ = load_and_validate_market_data(
        "market_data_multi.csv", "tickers.csv"
    )

    out_dir = tmp_path / "market_data"
    write_parquet_partitioned(market_df, str(out_dir))

    # Check that at least one partition directory exists
    parts = list(out_dir.glob("ticker=*"))
    assert parts, "Expected partitioned Parquet folders under market_data/"

    # AAPL rolling 5-minute close
    aapl_df = parquet_task1_aapl_rolling_5min(str(out_dir))
    assert "rolling_5min_close" in aapl_df.columns
    assert not aapl_df.empty

    # 5-day rolling volatility (may be NaN for small samples, but column should exist)
    daily_vol_df = parquet_task2_rolling_5d_vol(str(out_dir))
    assert "vol_5d" in daily_vol_df.columns or "vol_5d_min1" in daily_vol_df.columns
