# part1_clean.py
import os
import numpy as np
import pandas as pd


def load_and_clean(path: str = "data/market_data.csv") -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Data file not found: {path}")

    df = pd.read_csv(path)

    # Ensure we have the standard columns
    # (adjust this list if your CSV has slightly different names)
    expected_cols = ["Datetime", "Open", "High", "Low", "Close", "Volume"]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {path}: {missing}. Got columns: {list(df.columns)}")

    # Convert OHLCV to numeric (coerce errors to NaN)
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Basic cleaning
    df.dropna(subset=["Close"], inplace=True)  # make sure Close is usable
    df.drop_duplicates(subset=["Datetime"], inplace=True)

    # Parse datetime and sort
    df["Datetime"] = pd.to_datetime(df["Datetime"])
    df.set_index("Datetime", inplace=True)
    df.sort_index(inplace=True)

    # Derived features
    df["returns"] = df["Close"].pct_change()
    df["log_return"] = np.log(df["Close"] / df["Close"].shift(1)).replace(
        [np.inf, -np.inf], 0
    ).fillna(0)

    return df


if __name__ == "__main__":
    df = load_and_clean()
    print(df.dtypes)
    print(df.head())
    print(df.tail())
