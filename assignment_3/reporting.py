from __future__ import annotations
import os
from typing import List, Dict, Any
import matplotlib.pyplot as plt

def _ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def plot_scaling(results: List[Dict[str, Any]], out_dir: str):
    labels = sorted(set(r["strategy"] for r in results))
    sizes = sorted(set(r["n_ticks"] for r in results))
    # Runtime vs size
    plt.figure()
    for lbl in labels:
        xs, ys = [], []
        for n in sizes:
            for r in results:
                if r["strategy"] == lbl and r["n_ticks"] == n:
                    xs.append(n)
                    ys.append(r["seconds"])
        plt.plot(xs, ys, marker="o", label=lbl)
    plt.xlabel("Input size (ticks)")
    plt.ylabel("Runtime (seconds)")
    plt.title("Runtime vs Input Size")
    plt.legend()
    runtime_path = os.path.join(out_dir, "runtime_vs_size.png")
    _ensure_dir(runtime_path)
    plt.savefig(runtime_path, bbox_inches="tight")
    plt.close()

    # Memory vs size
    plt.figure()
    for lbl in labels:
        xs, ys = [], []
        for n in sizes:
            for r in results:
                if r["strategy"] == lbl and r["n_ticks"] == n:
                    xs.append(n)
                    ys.append(r["peak_bytes"] / (1024 * 1024.0))
        plt.plot(xs, ys, marker="o", label=lbl)
    plt.xlabel("Input size (ticks)")
    plt.ylabel("Peak memory (MB)")
    plt.title("Memory Usage vs Input Size")
    plt.legend()
    mem_path = os.path.join(out_dir, "memory_vs_size.png")
    _ensure_dir(mem_path)
    plt.savefig(mem_path, bbox_inches="tight")
    plt.close()

    return runtime_path, mem_path

def write_markdown_report(results: List[Dict[str, Any]], out_path: str):
    headers = ["dataset", "n_ticks", "strategy", "seconds", "peak_MB", "signals"]
    lines = []
    lines.append("# Complexity & Profiling Report\n")
    lines.append("This report summarizes runtime and memory usage across strategies and input sizes.\n")
    lines.append("## Results Table\n")
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join(["---"] * len(headers)) + "|")
    for r in sorted(results, key=lambda x: (x["strategy"], x["n_ticks"])):
        row = [
            str(r["dataset"]),
            str(r["n_ticks"]),
            str(r["strategy"]),
            f'{r["seconds"]:.6f}',
            f'{r["peak_bytes"] / (1024 * 1024.0):.3f}',
            str(r["signals"]),
        ]
        lines.append("| " + " | ".join(row) + " |")

    lines.append("\n## Complexity Annotations\n")
    lines.append("- **NaiveMovingAverageStrategy**: Time per tick O(n), Space O(n) (stores full history and recomputes sum).\n")
    lines.append("- **WindowedMovingAverageStrategy**: Time per tick O(1), Space O(k) for fixed window.\n")
    lines.append("- **OptimizedCumulativeAverageStrategy**: Time per tick O(1), Space O(1) by tracking (sum,count).\n")

    with open(out_path, "w") as f:
        f.write("\n".join(lines))
    return out_path
