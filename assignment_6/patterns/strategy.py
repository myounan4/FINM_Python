# assignment_6/patterns/strategy.py
from abc import ABC, abstractmethod
from typing import List, Dict
from ..models import MarketDataPoint

class Strategy(ABC):
    @abstractmethod
    def generate_signals(self, tick: MarketDataPoint) -> List[Dict]: ...

class MeanReversionStrategy(Strategy):
    def __init__(self, lookback=5, threshold=0.02, **kwargs):
        lookback = kwargs.get("lookback_window", kwargs.get("window", lookback))
        threshold = kwargs.get("threshold_pct", kwargs.get("band", threshold))
        self.window = int(lookback)
        self.th = float(threshold)
        self.h = []
    def generate_signals(self, tick: MarketDataPoint):
        self.h.append(tick.price)
        if len(self.h) < self.window: return []
        avg = sum(self.h[-self.window:]) / self.window
        r = tick.price / avg - 1.0
        if r < -self.th: return [{"action":"BUY","symbol":tick.symbol,"qty":100,"reason":"mr"}]
        if r >  self.th: return [{"action":"SELL","symbol":tick.symbol,"qty":100,"reason":"mr"}]
        return []

class BreakoutStrategy(Strategy):
    def __init__(self, lookback=20, **kwargs):
        lookback = kwargs.get("lookback_window", kwargs.get("window", lookback))
        self.window = int(lookback)
        self.h = []
    def generate_signals(self, tick: MarketDataPoint):
        self.h.append(tick.price)
        if len(self.h) < self.window: return []
        hi = max(self.h[-self.window:]); lo = min(self.h[-self.window:])
        if tick.price >= hi: return [{"action":"BUY","symbol":tick.symbol,"qty":100,"reason":"bo"}]
        if tick.price <= lo:  return [{"action":"SELL","symbol":tick.symbol,"qty":100,"reason":"bo"}]
        return []
