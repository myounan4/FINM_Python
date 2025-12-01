# data_loader.py

import pandas as pd


def load_and_validate_market_data(
    market_data_path: str = "market_data_multi.csv",
    tickers_path: str = "tickers.csv",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Load
    market_df = pd.read_csv(market_data_path)
    tickers_df = pd.read_csv(tickers_path)

    market_df.columns = [c.strip().lower() for c in market_df.columns]

    market_df["timestamp"] = pd.to_datetime(market_df["timestamp"])

    required_cols = {"timestamp", "ticker", "open", "high", "low", "close", "volume"}
    missing_cols = required_cols - set(market_df.columns)
    if missing_cols:
        raise ValueError(f"Market data missing required columns: {missing_cols}")

    if market_df["timestamp"].isna().any():
        raise ValueError("Market data contains missing timestamps.")
    price_cols = ["open", "high", "low", "close"]
    if market_df[price_cols].isna().any().any():
        raise ValueError("Market data contains missing price values.")

    tickers_in_meta = set(tickers_df["symbol"])
    tickers_in_data = set(market_df["ticker"])
    missing_tickers = tickers_in_meta - tickers_in_data
    if missing_tickers:
        raise ValueError(
            f"Tickers in tickers.csv not found in market_data_multi.csv: {missing_tickers}"
        )

    if market_df.duplicated(subset=["timestamp", "ticker"]).any():
        raise ValueError("Duplicate (timestamp, ticker) rows detected in market data.")

    market_df = market_df.sort_values(["ticker", "timestamp"]).reset_index(drop=True)

    return market_df, tickers_df


if __name__ == "__main__":
    mdf, tdf = load_and_validate_market_data(
        "market_data_multi.csv", "tickers.csv"
    )
    print(mdf.head())
    print(tdf)
