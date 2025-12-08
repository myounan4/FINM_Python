from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple
import pandas as pd
import numpy as np


class BaseStrategy:
    # All strategies must implement prepare_data and generate_order.

    def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError

    def generate_order(self, row: Dict[str, Any]) -> Tuple[Optional[str], int]:
        raise NotImplementedError


@dataclass
class MACrossoverConfig:
    ma_fast: int = 20
    ma_slow: int = 60
    units: int = 10


class MovingAverageCrossoverStrategy(BaseStrategy):
    # Long when fast MA > slow MA, short when fast MA < slow MA.

    def __init__(self, config: MACrossoverConfig):
        self.config = config
        self.prev_signal: Optional[int] = None

    def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["ma_fast"] = df["Close"].rolling(
            self.config.ma_fast, min_periods=1
        ).mean()
        df["ma_slow"] = df["Close"].rolling(
            self.config.ma_slow, min_periods=1
        ).mean()
        df["signal"] = 0
        df.loc[df["ma_fast"] > df["ma_slow"], "signal"] = 1
        df.loc[df["ma_fast"] < df["ma_slow"], "signal"] = -1
        return df

    def generate_order(self, row: Dict[str, Any]) -> Tuple[Optional[str], int]:
        curr_signal = int(row.get("signal", 0))
        if self.prev_signal is None:
            self.prev_signal = curr_signal
            return None, 0

        trade_signal = curr_signal - self.prev_signal
        self.prev_signal = curr_signal

        if trade_signal > 0:
            return "BUY", self.config.units
        elif trade_signal < 0:
            return "SELL", self.config.units
        return None, 0


@dataclass
class RSIMeanReversionConfig:
    rsi_period: int = 14
    oversold: float = 30.0
    overbought: float = 70.0
    units: int = 10


def _compute_rsi(series: pd.Series, period: int) -> pd.Series:
    delta = series.diff()
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    gain_ema = pd.Series(gain, index=series.index).ewm(
        alpha=1 / period, adjust=False
    ).mean()
    loss_ema = pd.Series(loss, index=series.index).ewm(
        alpha=1 / period, adjust=False
    ).mean()
    rs = gain_ema / (loss_ema + 1e-9)
    rsi = 100 - (100 / (1 + rs))
    return rsi


class RSIMeanReversionStrategy(BaseStrategy):
    # Mean-reversion using RSI.

    def __init__(self, config: RSIMeanReversionConfig):
        self.config = config

    def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["rsi"] = _compute_rsi(df["Close"], self.config.rsi_period)
        return df

    def generate_order(self, row: Dict[str, Any]) -> Tuple[Optional[str], int]:
        rsi = float(row.get("rsi", 50.0))
        if rsi < self.config.oversold:
            return "BUY", self.config.units
        elif rsi > self.config.overbought:
            return "SELL", self.config.units
        return None, 0


@dataclass
class MomentumBreakoutConfig:
    lookback: int = 50
    breakout_pct: float = 0.01
    units: int = 10


class MomentumBreakoutStrategy(BaseStrategy):
    """
    Trend-following breakout:
      - Go long if today's HIGH breaks above the previous lookback high by breakout_pct
      - Go short if today's LOW breaks below the previous lookback low by breakout_pct
    """

    def __init__(self, config: MomentumBreakoutConfig):
        self.config = config
        self.prev_position: int = 0  # +1 long, -1 short, 0 flat

    def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Use ONLY past bars for the lookback (shift(1) excludes current bar)
        df["lookback_high"] = (
            df["High"]
            .shift(1)
            .rolling(self.config.lookback, min_periods=1)
            .max()
        )
        df["lookback_low"] = (
            df["Low"]
            .shift(1)
            .rolling(self.config.lookback, min_periods=1)
            .min()
        )
        return df

    def generate_order(self, row: Dict[str, Any]) -> Tuple[Optional[str], int]:
        high_today = float(row.get("High", row["Close"]))
        low_today = float(row.get("Low", row["Close"]))

        lookback_high = float(row.get("lookback_high", high_today))
        lookback_low = float(row.get("lookback_low", low_today))

        long_trigger = high_today > lookback_high * (1 + self.config.breakout_pct)
        short_trigger = low_today < lookback_low * (1 - self.config.breakout_pct)

        desired_pos = self.prev_position
        if long_trigger:
            desired_pos = 1
        elif short_trigger:
            desired_pos = -1

        trade_signal = desired_pos - self.prev_position
        self.prev_position = desired_pos

        if trade_signal > 0:
            return "BUY", self.config.units
        elif trade_signal < 0:
            return "SELL", self.config.units
        return None, 0
