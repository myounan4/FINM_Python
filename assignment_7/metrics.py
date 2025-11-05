# metrics.py
import pandas as pd
import polars as pl

def compute_metrics_pandas(df):
    df = df.sort_values(["symbol","timestamp"]).reset_index(drop=True)
    df["price"] = df["price"].astype("float64")
    g = df.groupby("symbol", group_keys=False)
    df["ret"] = g["price"].pct_change()
    ma = g["price"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    std = g["ret"].rolling(20, min_periods=20).std(ddof=1).reset_index(level=0, drop=True)
    mean_ret = g["ret"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    df["roll_ma_20"] = ma
    df["roll_std_20"] = std
    df["roll_sharpe_20"] = mean_ret / std
    return df

def compute_metrics_pandas_symbol(g):
    g = g.sort_values(["symbol","timestamp"]).reset_index(drop=True).copy()
    g["price"] = g["price"].astype("float64")
    ret = g["price"].pct_change()
    ma = g["price"].rolling(20, min_periods=20).mean()
    std = ret.rolling(20, min_periods=20).std(ddof=1)
    out = g
    out["ret"] = ret
    out["roll_ma_20"] = ma
    out["roll_std_20"] = std
    out["roll_sharpe_20"] = ret.rolling(20, min_periods=20).mean() / std
    return out

def compute_metrics_polars(df):
    df = df.sort(["symbol","timestamp"])
    df = df.with_columns(pl.col("price").cast(pl.Float64))
    df = df.with_columns(pl.col("price").pct_change().over("symbol").alias("ret"))
    df = df.with_columns(pl.col("price").rolling_mean(20, min_samples=20).over("symbol").alias("roll_ma_20"))
    df = df.with_columns(pl.col("ret").rolling_std(20, ddof=1, min_samples=20).over("symbol").alias("roll_std_20"))
    df = df.with_columns(
        (pl.col("ret").rolling_mean(20, min_samples=20).over("symbol") /
         pl.col("ret").rolling_std(20, ddof=1, min_samples=20).over("symbol")).alias("roll_sharpe_20")
    )
    return df
