from __future__ import annotations
import csv
from datetime import datetime, timedelta
from typing import List
from models import MarketDataPoint

def read_market_csv(path: str) -> List[MarketDataPoint]:
    """
    Read CSV with columns: timestamp, symbol, price (in that order).
    Uses only Python's built-in csv module.

    Space to store full dataset is O(N) for N rows.
    """
    points: List[MarketDataPoint] = []
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = [fn.strip().lower() for fn in reader.fieldnames] if reader.fieldnames else []
        ts_key = "timestamp" if "timestamp" in fieldnames else fieldnames[0]
        sym_key = "symbol" if "symbol" in fieldnames else fieldnames[1]
        px_key = "price" if "price" in fieldnames else fieldnames[2]

        for row in reader:
            ts = row[ts_key]
            try:
                dt = datetime.fromisoformat(ts)
            except Exception:
                try:
                    dt = datetime.fromtimestamp(float(ts))
                except Exception:
                    dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            symbol = row[sym_key]
            price = float(row[px_key])
            points.append(MarketDataPoint(timestamp=dt, symbol=symbol, price=price))
    return points

def synth_to_csv(path: str, n: int = 10000, symbol: str = "XYZ", seed: int = 42) -> str:
    """
    Generate synthetic random-walk price series and save to CSV.
    """
    import random, math
    random.seed(seed)
    start = datetime(2024,1,1,9,30,0)
    price = 100.0
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp","symbol","price"])
        for i in range(n):
            drift = 0.00005
            shock = random.gauss(0, 0.5)
            price = max(0.01, price * (1 + drift) + shock)
            ts = (start + timedelta(seconds=i)).isoformat()
            writer.writerow([ts, symbol, f"{price:.5f}"])
    return path
