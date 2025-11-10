"""
Usage:
    python metrics_analyze.py

Reads:
  metrics/orders_log.csv
  metrics/price_ticks.csv
Outputs:
  metrics/performance_report_generated.md
Prints key stats to stdout as well.
"""
from pathlib import Path
import csv
import statistics
from datetime import datetime

METRICS_DIR = Path("metrics")
ORDERS = METRICS_DIR / "orders_log.csv"
TICKS  = METRICS_DIR / "price_ticks.csv"
OUT_MD = METRICS_DIR / "performance_report.md"

def _read_latencies():
    l_price = []
    l_strat = []
    if not ORDERS.exists():
        return l_price, l_strat
    with ORDERS.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                v = row.get("latency_price_to_recv_ms")
                if v not in (None, "", "None"):
                    l_price.append(float(v))
            except Exception:
                pass
            try:
                v = row.get("latency_strategy_to_recv_ms")
                if v not in (None, "", "None"):
                    l_strat.append(float(v))
            except Exception:
                pass
    return l_price, l_strat

def _read_throughput():
    # Simple throughput: ticks per second over the observed window
    if not TICKS.exists():
        return None, None, 0, 0.0
    ts = []
    with TICKS.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                ts.append(int(row["tick_ts_ms"]))
            except Exception:
                pass
    if not ts:
        return None, None, 0, 0.0
    ts.sort()
    start_ms, end_ms = ts[0], ts[-1]
    duration_sec = max(1e-9, (end_ms - start_ms) / 1000.0)
    tps = len(ts) / duration_sec
    return start_ms, end_ms, len(ts), tps

def _p95(xs):
    if not xs:
        return None
    xs_sorted = sorted(xs)
    k = int(0.95 * (len(xs_sorted) - 1))
    return xs_sorted[k]

def main():
    l_price, l_strat = _read_latencies()
    s_ms, e_ms, n_ticks, tps = _read_throughput()

    def _fmt_stats(arr):
        if not arr:
            return "n=0"
        return (
            f"n={len(arr)}, mean={statistics.mean(arr):.2f} ms, "
            f"median={statistics.median(arr):.2f} ms, p95={_p95(arr):.2f} ms"
        )

    print("Latency (price tick → order received):", _fmt_stats(l_price))
    print("Latency (strategy send → order received):", _fmt_stats(l_strat))
    if s_ms is not None:
        print(f"Throughput: ticks={n_ticks}, window={(e_ms - s_ms)/1000.0:.2f}s, ticks/sec={tps:.2f}")

    # Write markdown
    OUT_MD.parent.mkdir(exist_ok=True)
    with OUT_MD.open("w", encoding="utf-8") as f:
        f.write("# Performance Report (Generated)\n\n")
        f.write(f"Generated: {datetime.now().isoformat(timespec='seconds')}\n\n")
        f.write("## Latency\n")
        f.write(f"- Price→OM: {_fmt_stats(l_price)}\n")
        f.write(f"- Strategy→OM: {_fmt_stats(l_strat)}\n\n")
        f.write("## Throughput\n")
        if s_ms is None:
            f.write("- No tick data recorded.\n")
        else:
            dur = (e_ms - s_ms) / 1000.0
            f.write(f"- Ticks observed: {n_ticks}\n")
            f.write(f"- Observation window: {dur:.2f} s\n")
            f.write(f"- Approx ticks/sec: {tps:.2f}\n")

if __name__ == "__main__":
    main()
