from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from abc import ABC, abstractmethod

@dataclass(frozen=True)
class MarketDataPoint:
    """
    Immutable tick of market data.

    Space: O(1) per tick (fixed fields). Storing N ticks -> O(N).
    """
    timestamp: datetime
    symbol: str
    price: float

class Strategy(ABC):
    @abstractmethod
    def generate_signals(self, tick: MarketDataPoint) -> list:
        """
        Return a list of signals for a single tick.
        """
        raise NotImplementedError
