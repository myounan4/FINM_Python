# main.py
import time, psutil, os, json
from pathlib import Path
import numpy as np
from data_loader import load_pandas, load_polars
from metrics import compute_metrics_pandas, compute_metrics_polars
from parallel import (
    parallel_pandas_thread, parallel_pandas_process,
    parallel_polars_thread, parallel_polars_process,
)
from portfolio import aggregate
from reporting import Report

proc = psutil.Process(os.getpid())

def time_and_mem(fn, *args, **kwargs):
    t0 = time.perf_counter()
    rss0 = proc.memory_info().rss
    out = fn(*args, **kwargs)
    dt = time.perf_counter() - t0
    dmem = (proc.memory_info().rss - rss0) / (1024*1024)
    return out, dt, dmem

def np_encoder(o):
    if isinstance(o, (np.floating, np.integer)):
        return o.item()
    if isinstance(o, np.bool_):
        return bool(o)
    if isinstance(o, np.ndarray):
        return o.tolist()
    raise TypeError

def main():
    rep = Report()

    pdf, t_pd, m_pd = time_and_mem(load_pandas, "market_data-1.csv")
    pld, t_pl, m_pl = time_and_mem(load_polars, "market_data-1.csv")
    rep.add_row("ingest_time_s", t_pd, t_pl)
    rep.add_row("ingest_mem_MB", m_pd, m_pl)

    mp_seq, t_pd, m_pd = time_and_mem(compute_metrics_pandas, pdf)
    ml_seq, t_pl, m_pl = time_and_mem(compute_metrics_polars, pld)
    rep.add_row("rolling_seq_time_s", t_pd, t_pl)
    rep.add_row("rolling_seq_mem_MB", m_pd, m_pl)

    mp_thr, t_pd, m_pd = time_and_mem(parallel_pandas_thread, pdf)
    ml_thr, t_pl, m_pl = time_and_mem(parallel_polars_thread, pld)
    rep.add_row("rolling_thread_time_s", t_pd, t_pl)
    rep.add_row("rolling_thread_mem_MB", m_pd, m_pl)

    mp_proc, t_pd, m_pd = time_and_mem(parallel_pandas_process, pdf, max_workers=4)
    ml_proc, t_pl, m_pl = time_and_mem(parallel_polars_process, pld, max_workers=4)
    rep.add_row("rolling_process_time_s", t_pd, t_pl)
    rep.add_row("rolling_process_mem_MB", m_pd, m_pl)

    with open("portfolio_structure-1.json") as f:
        port = json.load(f)
    port_out, t_pd, m_pd = time_and_mem(aggregate, port, mp_proc)
    rep.add_row("portfolio_time_s", t_pd, float("nan"))
    rep.add_row("portfolio_mem_MB", m_pd, float("nan"))

    Path("outputs").mkdir(exist_ok=True)
    with open("outputs/portfolio_result.json", "w") as f:
        json.dump(port_out, f, indent=2, default=np_encoder)

    rep.write()

if __name__ == "__main__":
    main()
