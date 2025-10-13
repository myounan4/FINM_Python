from __future__ import annotations
from collections import deque
from typing import List
from models import MarketDataPoint, Strategy

class NaiveMovingAverageStrategy(Strategy):
    """
    For each tick, recompute the arithmetic mean from scratch over all seen prices.
    Time per tick: O(n) because it sums all history each time.
    Space: O(n) because it stores full history.
    """
    def __init__(self):
        self.history: List[float] = []

    def generate_signals(self, tick: MarketDataPoint) -> list:
        self.history.append(tick.price)
        s = sum(self.history)  # O(n)
        avg = s / len(self.history)
        return [("ma_naive", avg), ("price", tick.price)]

class WindowedMovingAverageStrategy(Strategy):
    """
    Maintain a fixed-size window (k) and update sum incrementally.
    Time per tick: O(1) amortized (push/pop and arithmetic).
    Space: O(k).
    """
    def __init__(self, window: int = 50):
        self.window = window
        self.buf: deque[float] = deque(maxlen=window)
        self.running_sum: float = 0.0

    def generate_signals(self, tick: MarketDataPoint) -> list:
        if len(self.buf) == self.window:
            removed = self.buf[0]
        else:
            removed = 0.0
        self.buf.append(tick.price)
        if len(self.buf) > 1 and len(self.buf) == self.window:
            self.running_sum -= removed
        self.running_sum += tick.price
        avg = self.running_sum / len(self.buf)
        return [("ma_window", avg), ("price", tick.price)]

class OptimizedCumulativeAverageStrategy(Strategy):
    """
    Optimized version of the naive 'average of all history' strategy.
    We avoid storing the full list and avoid O(n) summation by tracking (sum, count).
    Time per tick: O(1)
    Space: O(1)
    """
    def __init__(self):
        self.count: int = 0
        self.sum_prices: float = 0.0

    def generate_signals(self, tick: MarketDataPoint) -> list:
        self.count += 1
        self.sum_prices += tick.price
        avg = self.sum_prices / self.count
        return [("ma_cum", avg), ("price", tick.price)]
