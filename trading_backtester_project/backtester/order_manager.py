from dataclasses import dataclass
from typing import List
import csv
import os
import time

from .order_book import Order


@dataclass
class RiskConfig:
    max_notional: float = 100_000.0   # per order
    max_position: int = 1_000         # max absolute units
    max_orders_per_min: int = 60      # throttling


class OrderManager:
    # Validates orders against capital & risk limits and logs them to file.

    def __init__(
        self,
        starting_cash: float = 100_000.0,
        risk_config: RiskConfig = RiskConfig(),
        log_path: str = "data/order_log.csv",
    ):
        self.cash = starting_cash
        self.position = 0
        self.risk_config = risk_config
        self.orders_last_minute: List[float] = []
        self.log_path = log_path

        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        if not os.path.exists(log_path):
            with open(log_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "timestamp",
                        "event_type",
                        "order_id",
                        "side",
                        "price",
                        "quantity",
                        "details",
                    ]
                )

    def _log(self, event_type: str, order: Order, details: str = ""):
        with open(self.log_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    time.time(),
                    event_type,
                    order.order_id,
                    order.side,
                    order.price,
                    order.quantity,
                    details,
                ]
            )

    def _update_orders_per_minute(self):
        now = time.time()
        self.orders_last_minute = [
            t for t in self.orders_last_minute if now - t < 60
        ]

    def _check_risk_limits(self, side: str, price: float, quantity: int) -> bool:
        self._update_orders_per_minute()

        if len(self.orders_last_minute) >= self.risk_config.max_orders_per_min:
            return False

        notional = price * quantity
        if notional > self.risk_config.max_notional:
            return False

        # Position limit
        new_position = self.position + (quantity if side == "BUY" else -quantity)
        if abs(new_position) > self.risk_config.max_position:
            return False

        # Capital sufficiency
        if side == "BUY" and self.cash < notional:
            return False

        return True

    def approve_order(self, order: Order) -> bool:
        ok = self._check_risk_limits(order.side, order.price, order.quantity)
        self._log("SENT" if ok else "REJECTED", order,
                  "approved" if ok else "risk_limit")
        if ok:
            self.orders_last_minute.append(time.time())
        return ok

    def apply_fill(self, order: Order, fill_price: float, fill_qty: int):
        notional = fill_price * fill_qty
        if order.side == "BUY":
            self.cash -= notional
            self.position += fill_qty
        else:
            self.cash += notional
            self.position -= fill_qty
        self._log(
            "FILLED",
            order,
            f"fill_price={fill_price}, fill_qty={fill_qty}",
        )
