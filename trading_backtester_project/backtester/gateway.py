from dataclasses import dataclass
from typing import Dict, Any, Iterator
import pandas as pd


@dataclass
class MarketDataPoint:
    timestamp: pd.Timestamp
    data: Dict[str, Any]


class MarketDataGateway:
    # Feeds historical market data row-by-row to simulate a live stream.
    # Assumes df index is DatetimeIndex.

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def stream(self) -> Iterator[MarketDataPoint]:
        for ts, row in self.df.iterrows():
            yield MarketDataPoint(timestamp=ts, data=row.to_dict())
