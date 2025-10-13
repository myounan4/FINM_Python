# Complexity & Profiling Report

This report summarizes runtime and memory usage across strategies and input sizes.

## Results Table

| dataset | n_ticks | strategy | seconds | peak_MB | signals |
|---|---|---|---|---|---|
| 1k | 1000 | naive | 0.005811 | 0.010 | 2000 |
| 10k | 10000 | naive | 0.297800 | 0.082 | 20000 |
| 100k* | 100000 | naive | 22.474139 | 0.764 | 200000 |
| 1k | 1000 | optimized_cum | 0.001620 | 0.001 | 2000 |
| 10k | 10000 | optimized_cum | 0.012443 | 0.000 | 20000 |
| 100k* | 100000 | optimized_cum | 0.106282 | 0.000 | 200000 |
| 1k | 1000 | window_50 | 0.001993 | 0.002 | 2000 |
| 10k | 10000 | window_50 | 0.014705 | 0.001 | 20000 |
| 100k* | 100000 | window_50 | 0.132378 | 0.000 | 200000 |

## Complexity Annotations

- **NaiveMovingAverageStrategy**: Time per tick O(n), Space O(n) (stores full history and recomputes sum).

- **WindowedMovingAverageStrategy**: Time per tick O(1), Space O(k) for fixed window.

- **OptimizedCumulativeAverageStrategy**: Time per tick O(1), Space O(1) by tracking (sum,count).
