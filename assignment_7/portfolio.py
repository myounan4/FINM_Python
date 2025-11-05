# portfolio.py
import pandas as pd
import numpy as np

def compute_position(df, symbol, qty):
    d = df[df["symbol"]==symbol].copy()
    price = d["price"].iloc[-1]
    value = price * qty
    ret = d["price"].pct_change()
    vol = ret.std()
    cum = (1+ret).cumprod()
    draw = (cum / cum.cummax() - 1).min()
    return value, vol, draw

def aggregate(port, df):
    vals = []
    vols = []
    draws = []
    for p in port.get("positions", []):
        v, s, d = compute_position(df, p["symbol"], p["quantity"])
        vals.append(v); vols.append(s); draws.append(d)
        p["value"] = v
        p["volatility"] = s
        p["drawdown"] = d
    for sp in port.get("sub_portfolios", []):
        aggregate(sp, df)
        vals.append(sp["total_value"])
        vols.append(sp["aggregate_volatility"])
        draws.append(sp["max_drawdown"])
    if vals:
        tot = sum(vals)
        w = np.array(vals) / tot
        port["total_value"] = tot
        port["aggregate_volatility"] = float((w * np.array(vols)).sum())
        port["max_drawdown"] = float(min(draws))
    return port
