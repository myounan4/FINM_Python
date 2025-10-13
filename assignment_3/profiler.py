from __future__ import annotations
import time
import tracemalloc
from typing import Callable, Dict, Any, List
from models import MarketDataPoint, Strategy

def consume_stream(strategy: Strategy, stream: List[MarketDataPoint]) -> int:
    """
    Feed all ticks to a strategy; return number of signals produced.
    This isolates the cost of generate_signals.
    """
    count = 0
    for tick in stream:
        sigs = strategy.generate_signals(tick)
        count += len(sigs)
    return count

def profile_strategy(strategy_factory: Callable[[], Strategy], stream: List[MarketDataPoint]) -> Dict[str, Any]:
    """
    Measure wall time and peak memory using tracemalloc.
    """
    tracemalloc.start()
    t0 = time.perf_counter()
    strat = strategy_factory()
    produced = consume_stream(strat, stream)
    elapsed = time.perf_counter() - t0
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {
        "signals": produced,
        "seconds": elapsed,
        "peak_bytes": int(peak),
    }

def run_benchmarks(strategy_factories: Dict[str, Callable[[], Strategy]], datasets: Dict[str, List[MarketDataPoint]]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for size_label, data in datasets.items():
        for name, factory in strategy_factories.items():
            metrics = profile_strategy(factory, data)
            row = {
                "dataset": size_label,
                "n_ticks": len(data),
                "strategy": name,
                **metrics
            }
            results.append(row)
    return results
