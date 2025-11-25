import random
from dataclasses import dataclass
from typing import List

from .order_book import OrderBook, Order


@dataclass
class ExecutionReport:
    order: Order
    status: str        # "FILLED", "PARTIAL", "CANCELLED"
    filled_qty: int
    avg_price: float


class MatchingEngine:
    # Simulates order execution outcomes: FULL, PARTIAL, or CANCEL.

    def __init__(self, order_book: OrderBook):
        self.book = order_book

    def submit_order(self, order: Order, mid_price: float) -> ExecutionReport:
        outcome = random.choice(["FULL", "PARTIAL", "CANCEL"])

        if outcome == "CANCEL":
            return ExecutionReport(
                order=order, status="CANCELLED", filled_qty=0, avg_price=0.0
            )

        if outcome == "PARTIAL":
            filled_qty = max(1, order.quantity // 2)
            return ExecutionReport(
                order=order,
                status="PARTIAL",
                filled_qty=filled_qty,
                avg_price=mid_price,
            )

        # FULL
        return ExecutionReport(
            order=order,
            status="FILLED",
            filled_qty=order.quantity,
            avg_price=mid_price,
        )
