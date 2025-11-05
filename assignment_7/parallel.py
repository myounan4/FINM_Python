# parallel.py
import os, tempfile
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing as mp
import pandas as pd
import polars as pl
from metrics import compute_metrics_pandas_symbol, compute_metrics_polars

_PDF = None
_PL_PATH = None

def _set_pdf(df):
    global _PDF
    _PDF = df

def _set_pl_path(path):
    os.environ["POLARS_MAX_THREADS"] = "1"
    global _PL_PATH
    _PL_PATH = path

def _chunks(lst, k):
    k = max(1, k)
    m = (len(lst) + k - 1) // k
    return [lst[i:i+m] for i in range(0, len(lst), m)]

def _compute_pd_syms(syms):
    parts = []
    for s in syms:
        g = _PDF[_PDF["symbol"] == s]
        parts.append(compute_metrics_pandas_symbol(g))
    return pd.concat(parts, ignore_index=True) if parts else _PDF.iloc[0:0].copy()

def _compute_pl_syms(syms):
    lf = pl.scan_parquet(_PL_PATH) if _PL_PATH.endswith(".parquet") else pl.scan_csv(_PL_PATH)
    df = lf.filter(pl.col("symbol").is_in(syms)).collect()
    return compute_metrics_polars(df)

def parallel_pandas_thread(df, max_workers=None):
    syms = df["symbol"].unique().tolist()
    parts = [df[df["symbol"] == s] for s in syms]
    w = max_workers or min(os.cpu_count() or 1, len(parts))
    with ThreadPoolExecutor(max_workers=w) as ex:
        res = list(ex.map(compute_metrics_pandas_symbol, parts))
    return pd.concat(res, ignore_index=True)

def parallel_pandas_process(df, max_workers=None):
    syms = df["symbol"].unique().tolist()
    w = max_workers or min(os.cpu_count() or 1, len(syms), 4)
    batches = _chunks(syms, w)
    with ProcessPoolExecutor(max_workers=w, initializer=_set_pdf, initargs=(df,)) as ex:
        res = list(ex.map(_compute_pd_syms, batches, chunksize=1))
    return pd.concat(res, ignore_index=True)

def parallel_polars_thread(df, max_workers=None):
    return compute_metrics_polars(df)

def parallel_polars_process(data, max_workers=None):
    if isinstance(data, pl.DataFrame):
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        path = tmp.name
        tmp.close()
        data.write_parquet(path)
        cleanup = True
    else:
        path = os.path.abspath(str(data))
        cleanup = False
    syms = (pl.read_parquet(path, columns=["symbol"]) if path.endswith(".parquet")
            else pl.read_csv(path, columns=["symbol"])).get_column("symbol").unique().to_list()
    w = max_workers or min(os.cpu_count() or 1, len(syms), 4)
    batches = _chunks(syms, w)
    ctx = mp.get_context("spawn")
    with ProcessPoolExecutor(max_workers=w, mp_context=ctx, initializer=_set_pl_path, initargs=(path,)) as ex:
        res = list(ex.map(_compute_pl_syms, batches, chunksize=1))
    out = pl.concat(res)
    if cleanup:
        try: os.remove(path)
        except OSError: pass
    return out
