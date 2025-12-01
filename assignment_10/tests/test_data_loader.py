import pandas as pd

from data_loader import load_and_validate_market_data


def test_load_and_validate_market_data(tmp_path, monkeypatch):
    """Smoke test that data_loader returns a non-empty DataFrame and no missing values."""
    # Assume CSVs live in the project root by default
    market_path = "market_data_multi.csv"
    tickers_path = "tickers.csv"

    market_df, tickers_df = load_and_validate_market_data(
        market_data_path=market_path,
        tickers_path=tickers_path,
    )

    # Basic shape checks
    assert not market_df.empty
    assert not tickers_df.empty

    # No missing timestamps or prices
    assert not market_df["timestamp"].isna().any()
    for col in ["open", "high", "low", "close"]:
        assert not market_df[col].isna().any()

    # All tickers from tickers.csv present in market data
    missing = set(tickers_df["symbol"]) - set(market_df["ticker"])
    assert not missing
