# alpaca_data.py
import os
from typing import Optional

import pandas as pd
import alpaca_trade_api as tradeapi
from alpaca_trade_api import REST
from alpaca_trade_api.rest import TimeFrame
from alpaca_config import API_KEY_ID, API_SECRET_KEY, BASE_URL


def get_alpaca_bars(
    symbol: str = "AAPL",
    timeframe: str = "1Min",
    limit: int = 1000,
) -> pd.DataFrame:
    tf_map = {
        "1Min": TimeFrame.Minute,
        "1Day": TimeFrame.Day,
    }
    if timeframe not in tf_map:
        raise ValueError(
            f"Unsupported timeframe: {timeframe}. "
            f"Use one of {list(tf_map.keys())} or extend tf_map."
        )

    api = tradeapi.REST(API_KEY_ID, API_SECRET_KEY, 'https://paper-api.alpaca.markets')

    # data = api.get_barset('AAPL', '1Min', limit=1).df['AAPL'] THIS LINE DOESNT WORK
    bars = api.get_bars(
        symbol,
        tf_map[timeframe],
        limit=limit,
        adjustment="raw",
    ).df

    # If DataFrame uses a MultiIndex (symbol, timestamp), select symbol slice
    if isinstance(bars.index, pd.MultiIndex):
        if symbol in bars.index.levels[0]:
            bars = bars.xs(symbol, level=0)

    if bars.empty:
        raise ValueError(
            f"No bars returned from Alpaca for symbol={symbol}, timeframe={timeframe}, "
            f"limit={limit}. Check your market data subscription and permissions."
        )

    bars = bars.reset_index()

    colmap = {c.lower(): c for c in bars.columns}

    def find_col(candidates):
        for c in candidates:
            if c in colmap:
                return colmap[c]
        return None

    # Try to detect relevant columns
    time_key = find_col(["timestamp", "time", "t", "datetime", "index"])
    open_key = find_col(["open", "o"])
    high_key = find_col(["high", "h"])
    low_key = find_col(["low", "l"])
    close_key = find_col(["close", "c"])
    volume_key = find_col(["volume", "v"])

    missing = {
        "time": time_key,
        "open": open_key,
        "high": high_key,
        "low": low_key,
        "close": close_key,
        "volume": volume_key,
    }
    if any(v is None for v in missing.values()):
        raise ValueError(
            "Could not map Alpaca columns to OHLCV.\n"
            f"Columns={list(bars.columns)}\n"
            f"Resolved={missing}"
        )

    df = pd.DataFrame(
        {
            "Datetime": bars[time_key],
            "Open": bars[open_key],
            "High": bars[high_key],
            "Low": bars[low_key],
            "Close": bars[close_key],
            "Volume": bars[volume_key],
        }
    )

    return df


def save_alpaca_bars_to_csv(
    symbol: str = "AAPL",
    timeframe: str = "1Min",
    limit: int = 1000,
    out_path: Optional[str] = None,
) -> str:
    if out_path is None:
        os.makedirs("data", exist_ok=True)
        out_path = os.path.join("data", f"market_data_{symbol}_{timeframe}.csv")

    df = get_alpaca_bars(symbol=symbol, timeframe=timeframe, limit=limit)
    df.to_csv(out_path, index=False)

    print(
        f"Saved {len(df)} rows of {symbol} {timeframe} data from Alpaca v2 to {out_path}"
    )
    return out_path


if __name__ == "__main__":
    # Simple manual test
    save_alpaca_bars_to_csv()
