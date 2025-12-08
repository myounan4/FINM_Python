import json
from pathlib import Path

import numpy as np
import pandas as pd


def load_market_data(path):
    df = pd.read_csv(path)

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])
    elif "date" in df.columns:
        df["Date"] = pd.to_datetime(df["date"])
    else:
        raise ValueError("Expected a 'Date' or 'date' column in market_data_ml.csv")

    if "ticker" not in df.columns:
        raise ValueError("Expected a 'ticker' column in market_data_ml.csv")

    df = df.sort_values(["ticker", "Date"]).reset_index(drop=True)
    return df


def _compute_features_for_group(g: pd.DataFrame) -> pd.DataFrame:
    g = g.sort_values("Date").copy()
    close = g["close"]

    g["return_1d"] = close.pct_change(1)
    g["log_return_1d"] = np.log(close).diff(1)
    g["return_3d"] = close.pct_change(3)
    g["return_5d"] = close.pct_change(5)

    g["sma_5"] = close.rolling(5, min_periods=5).mean()
    g["sma_10"] = close.rolling(10, min_periods=10).mean()

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(14, min_periods=14).mean()
    avg_loss = loss.rolling(14, min_periods=14).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    g["rsi_14"] = 100 - (100 / (1 + rs))

    ema_fast = close.ewm(span=12, adjust=False).mean()
    ema_slow = close.ewm(span=26, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    g["macd"] = macd_line - signal_line

    return g


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.groupby("ticker", group_keys=False).apply(_compute_features_for_group)
    return df


def add_label_direction(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "return_1d" not in df.columns:
        raise ValueError("Need 'return_1d' column before labeling. Call add_features first.")

    df["next_return_1d"] = df.groupby("ticker")["return_1d"].shift(-1)
    df["direction"] = (df["next_return_1d"] > 0).astype(int)
    return df


def load_feature_config(path):
    path = Path(path)
    with open(path, "r") as f:
        return json.load(f)


def build_feature_matrix(df: pd.DataFrame, cfg_path: str | Path):
    cfg = load_feature_config(cfg_path)
    feature_cols = cfg["features"]
    label_col = cfg["label"]

    missing = [c for c in feature_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required feature columns: {missing}")

    X = df[feature_cols].copy()
    y = df[label_col].copy()
    meta_cols = ["Date", "ticker"]
    meta = df[meta_cols].copy()

    mask = X.notna().all(axis=1) & y.notna()
    X = X[mask]
    y = y[mask]
    meta = meta[mask]

    return X, y, meta


def prepare_dataset(data_path, features_config_path):
    df = load_market_data(data_path)
    df = add_features(df)
    df = add_label_direction(df)
    X, y, meta = build_feature_matrix(df, features_config_path)
    return X, y, meta
