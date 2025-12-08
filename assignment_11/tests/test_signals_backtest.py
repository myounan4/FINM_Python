
import numpy as np
import pandas as pd

from signal_generator import generate_signals, attach_signals_to_prices
from backtest import backtest


class DummyModel:
    def predict_proba(self, X):
        n = X.shape[0]
        p1 = np.full(n, 0.7)
        p0 = 1 - p1
        return np.vstack([p0, p1]).T


def test_generate_signals_and_backtest(tmp_path):
    dates = pd.date_range("2024-01-01", periods=10, freq="D")
    data = []
    for ticker in ["AAA", "BBB"]:
        price = 100.0
        for d in dates:
            close_p = price * (1 + np.random.normal(0, 0.01))
            data.append(
                {
                    "Date": d.strftime("%Y-%m-%d"),
                    "ticker": ticker,
                    "Open": price,
                    "High": max(price, close_p),
                    "Low": min(price, close_p),
                    "close": close_p,
                    "Volume": 1000,
                }
            )
            price = close_p

    market_path = tmp_path / "market_data.csv"
    pd.DataFrame(data).to_csv(market_path, index=False)

    X = np.random.randn(len(dates) * 2, 4)
    meta = pd.DataFrame(
        {
            "Date": list(dates) * 2,
            "ticker": ["AAA"] * len(dates) + ["BBB"] * len(dates),
        }
    )

    model = DummyModel()
    signals_df = generate_signals(model, X, meta, threshold=0.5)
    assert "signal" in signals_df.columns
    assert "proba" in signals_df.columns
    assert signals_df["signal"].isin([0, 1]).all()

    merged = attach_signals_to_prices(signals_df, market_path)
    assert {"Date", "ticker", "close", "signal"}.issubset(merged.columns)

    result = backtest(merged)
    assert "equity" in result.columns
    assert result["equity"].notna().all()
