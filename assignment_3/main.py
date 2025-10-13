from __future__ import annotations
import os
from typing import Dict, List
from data_loader import read_market_csv, synth_to_csv
from models import MarketDataPoint
from strategies import NaiveMovingAverageStrategy, WindowedMovingAverageStrategy, OptimizedCumulativeAverageStrategy
from profiler import run_benchmarks
from reporting import plot_scaling, write_markdown_report

OUT_DIR = os.path.join(os.path.dirname(__file__), "artifacts")

def ensure_data(csv_path: str, n: int = 20000) -> str:
    if not os.path.exists(csv_path):
        synth_to_csv(csv_path, n=n)
    return csv_path

def take_first(points: List[MarketDataPoint], n: int) -> List[MarketDataPoint]:
    return points[:n]

def main():
    repo_dir = os.path.dirname(__file__)
    data_path = os.path.join(repo_dir, "market_data.csv")
    ensure_data(data_path, n=30000)
    all_points = read_market_csv(data_path)

    datasets = {
        "1k": take_first(all_points, 1_000),
        "10k": take_first(all_points, 10_000),
        "100k*": take_first(all_points * 4, 100_000),
    }

    strategies = {
        "naive": lambda: NaiveMovingAverageStrategy(),
        "window_50": lambda: WindowedMovingAverageStrategy(window=50),
        "optimized_cum": lambda: OptimizedCumulativeAverageStrategy(),
    }

    results = run_benchmarks(strategies, datasets)

    os.makedirs(OUT_DIR, exist_ok=True)
    runtime_path, mem_path = plot_scaling(results, OUT_DIR)
    report_path = write_markdown_report(results, os.path.join(repo_dir, "complexity_report.md"))

    print("Artifacts:")
    print(" - Runtime plot:", runtime_path)
    print(" - Memory plot :", mem_path)
    print(" - Report      :", report_path)

if __name__ == "__main__":
    main()
