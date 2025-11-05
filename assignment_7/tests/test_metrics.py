import numpy as np
import polars as pl
from data_loader import load_pandas, load_polars
from metrics import compute_metrics_pandas, compute_metrics_polars

def test_rolling_equivalence():
    pdf = load_pandas("market_data-1.csv")
    pld = load_polars("market_data-1.csv")

    mp = compute_metrics_pandas(pdf).sort_values(["symbol","timestamp"]).reset_index(drop=True)
    ml = compute_metrics_polars(pld).sort(["symbol","timestamp"])

    for col in ["roll_ma_20","roll_std_20","roll_sharpe_20"]:
        a = mp[col].astype(float).to_numpy()
        b = ml.get_column(col).cast(pl.Float64).to_numpy()
        assert np.allclose(a, b, atol=1e-6, equal_nan=True)
