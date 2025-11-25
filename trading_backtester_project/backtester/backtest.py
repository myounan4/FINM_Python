from typing import Dict, Any, List
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from .gateway import MarketDataGateway
from .order_book import OrderBook, Order
from .order_manager import OrderManager
from .matching_engine import MatchingEngine, ExecutionReport
from .strategy import BaseStrategy


class Backtester:
    # Generic backtester that can run any BaseStrategy.

    def __init__(
        self,
        df: pd.DataFrame,
        strategy: BaseStrategy,
        starting_cash: float = 100_000.0,
    ):
        self.strategy = strategy
        self.df = self.strategy.prepare_data(df)
        self.gateway = MarketDataGateway(self.df)
        self.order_book = OrderBook()
        self.order_manager = OrderManager(starting_cash=starting_cash)
        self.matching_engine = MatchingEngine(self.order_book)

        self.equity_curve: List[float] = []
        self.timestamps: List[pd.Timestamp] = []
        self.trades: List[ExecutionReport] = []

    def run(self):
        for md in self.gateway.stream():
            ts = md.timestamp
            row: Dict[str, Any] = md.data
            price = row["Close"]

            equity = self.order_manager.cash + self.order_manager.position * price
            self.equity_curve.append(equity)
            self.timestamps.append(ts)

            side, qty = self.strategy.generate_order(row)
            if side is None or qty <= 0:
                continue

            order: Order = self.order_book.add_order(
                price=price,
                quantity=qty,
                side=side,
                timestamp=ts.timestamp(),
            )

            if not self.order_manager.approve_order(order):
                continue

            report = self.matching_engine.submit_order(order, mid_price=price)
            if report.filled_qty > 0:
                self.order_manager.apply_fill(
                    order, report.avg_price, report.filled_qty
                )
            self.trades.append(report)

    def _compute_metrics(self) -> Dict[str, float]:
        eq = np.array(self.equity_curve, dtype=float)
        if len(eq) < 2:
            return {}

        rets = np.diff(eq) / eq[:-1]
        if len(rets) == 0:
            return {}

        bars_per_day = 390
        days_per_year = 252
        factor = bars_per_day * days_per_year

        sharpe = (
            np.mean(rets) / (np.std(rets) + 1e-8) * np.sqrt(factor)
        )
        cummax = np.maximum.accumulate(eq)
        drawdown = (eq - cummax) / cummax
        max_dd = drawdown.min()
        total_pnl = eq[-1] - eq[0]

        return {
            "total_pnl": float(total_pnl),
            "sharpe": float(sharpe),
            "max_drawdown": float(max_dd),
            "final_equity": float(eq[-1]),
        }

    def report(self, title: str = "Equity Curve"):
        metrics = self._compute_metrics()
        print("\nBacktest Metrics")
        for k, v in metrics.items():
            print(f"{k}: {v:.6f}")

        plt.figure()
        plt.plot(self.timestamps, self.equity_curve)
        plt.title(title)
        plt.xlabel("Time")
        plt.ylabel("Equity")
        plt.tight_layout()
        plt.show()
