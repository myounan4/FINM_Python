import numpy as np
import polars as pl
from data_loader import load_pandas, load_polars
from metrics import compute_metrics_pandas, compute_metrics_polars
from parallel import (
    parallel_pandas_thread, parallel_pandas_process,
    parallel_polars_thread, parallel_polars_process,
)

def test_parallel_pandas_matches_sequential():
    pdf = load_pandas("market_data-1.csv")
    seq = compute_metrics_pandas(pdf).sort_values(["symbol","timestamp"]).reset_index(drop=True)
    thr = parallel_pandas_thread(pdf).sort_values(["symbol","timestamp"]).reset_index(drop=True)
    pro = parallel_pandas_process(pdf).sort_values(["symbol","timestamp"]).reset_index(drop=True)
    for col in ["ret","roll_ma_20","roll_std_20","roll_sharpe_20"]:
        a, b, c = seq[col].to_numpy(), thr[col].to_numpy(), pro[col].to_numpy()
        assert np.allclose(a, b, atol=1e-6, equal_nan=True)
        assert np.allclose(a, c, atol=1e-6, equal_nan=True)

def test_parallel_polars_matches_sequential():
    pld = load_polars("market_data-1.csv")
    seq = compute_metrics_polars(pld).sort(["symbol","timestamp"])
    thr = parallel_polars_thread(pld).sort(["symbol","timestamp"])
    # If you call the “process” variant with a DF, it writes a temp parquet internally (your code).
    pro = parallel_polars_process(pld).sort(["symbol","timestamp"])

    for col in ["ret","roll_ma_20","roll_std_20","roll_sharpe_20"]:
        a = seq.get_column(col).cast(pl.Float64).to_numpy()
        b = thr.get_column(col).cast(pl.Float64).to_numpy()
        c = pro.get_column(col).cast(pl.Float64).to_numpy()
        assert np.allclose(a, b, atol=1e-6, equal_nan=True)
        assert np.allclose(a, c, atol=1e-6, equal_nan=True)
