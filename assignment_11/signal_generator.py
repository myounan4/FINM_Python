
import numpy as np
import pandas as pd

def generate_signals(model, X, meta, threshold=0.5):
    proba = model.predict_proba(X)[:,1]
    signals = (proba > threshold).astype(int)
    out = meta.copy()
    out["signal"] = signals
    out["proba"] = proba
    return out

def attach_signals_to_prices(signals_df, data_path):
    price_df = pd.read_csv(data_path)
    if "Date" in price_df.columns:
        price_df["Date"] = pd.to_datetime(price_df["Date"])
    signals_df["Date"] = pd.to_datetime(signals_df["Date"])
    return price_df.merge(signals_df, on=["Date", "ticker"], how="inner")
