import pandas as pd
import polars as pl

def load_pandas(path):
    df = pd.read_csv(path, parse_dates=["timestamp"])
    df = df[["timestamp","symbol","price"]]
    df["symbol"] = df["symbol"].astype(str)
    df["price"] = df["price"].astype("float32")
    df = df.dropna()
    df = df.sort_values(["symbol","timestamp"]).reset_index(drop=True)
    return df

def load_polars(path):
    df = pl.read_csv(path)
    df = df.select(["timestamp","symbol","price"])
    df = df.with_columns([
        pl.col("timestamp").str.strptime(pl.Datetime, strict=False),
        pl.col("symbol").cast(pl.Utf8),
        pl.col("price").cast(pl.Float32)
    ])
    df = df.drop_nulls()
    df = df.sort(["symbol","timestamp"])
    return df


