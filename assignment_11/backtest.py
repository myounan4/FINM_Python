
import pandas as pd

def backtest(df):
    df = df.sort_values(["ticker", "Date"]).reset_index(drop=True)
    df["return"] = df.groupby("ticker")["Close"].pct_change()
    df["position"] = df.groupby("ticker")["signal"].shift(1)
    df["strategy_return"] = df["position"] * df["return"]
    equity = (1 + df["strategy_return"].fillna(0)).cumprod()
    df["equity"] = equity
    return df
