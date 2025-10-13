import time
import tracemalloc
from data_loader import synth_to_csv, read_market_csv
from strategies import NaiveMovingAverageStrategy, WindowedMovingAverageStrategy, OptimizedCumulativeAverageStrategy
from models import MarketDataPoint

def run_stream(strat, data):
    out = 0
    for t in data:
        out += len(strat.generate_signals(t))
    return out

def test_correctness_small():
    # small deterministic set
    data = [
        MarketDataPoint(timestamp=None, symbol="X", price=p)
        for p in [1.0, 2.0, 3.0, 4.0, 5.0]
    ]
    naive = NaiveMovingAverageStrategy()
    opt = OptimizedCumulativeAverageStrategy()
    for i, tick in enumerate(data, 1):
        n_ma = dict(naive.generate_signals(tick))["ma_naive"]
        o_ma = dict(opt.generate_signals(tick))["ma_cum"]
        assert abs(n_ma - o_ma) < 1e-9

def test_performance_constraints():
    # synth 100k points
    import os
    path = os.path.join(os.path.dirname(__file__), "..", "tmp_100k.csv")
    synth_to_csv(path, n=100_000)
    points = read_market_csv(path)
    start = time.perf_counter()
    tracemalloc.start()
    out = run_stream(OptimizedCumulativeAverageStrategy(), points)
    elapsed = time.perf_counter() - start
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert elapsed < 1.0, f"Elapsed {elapsed:.3f}s exceeds 1s"
    assert peak < 100 * 1024 * 1024, f"Peak {peak/1e6:.1f}MB exceeds 100MB"
