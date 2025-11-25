from dataclasses import dataclass, field
from typing import Optional, List, Tuple
import heapq
import itertools


@dataclass(order=True)
class Order:
    sort_index: tuple = field(init=False, repr=False)
    price: float
    quantity: int
    side: str          # "BUY" or "SELL"
    order_id: int
    timestamp: float   # Unix timestamp, used for time priority

    def __post_init__(self):
        # For BUY: (-price, timestamp) => max price, earliest time
        # For SELL: (price, timestamp)  => min price, earliest time
        if self.side.upper() == "BUY":
            self.sort_index = (-self.price, self.timestamp)
        else:
            self.sort_index = (self.price, self.timestamp)


class OrderBook:
    # Simple limit order book using heapq:
    #   - bids: max-heap on price (via -price)
    #   - asks: min-heap on price

    def __init__(self):
        self.bids: List[Order] = []
        self.asks: List[Order] = []
        self._id_counter = itertools.count()

    def add_order(self, price: float, quantity: int, side: str, timestamp: float) -> Order:
        order = Order(
            price=price,
            quantity=quantity,
            side=side.upper(),
            order_id=next(self._id_counter),
            timestamp=timestamp,
        )
        if order.side == "BUY":
            heapq.heappush(self.bids, order)
        else:
            heapq.heappush(self.asks, order)
        return order

    def _best_bid(self) -> Optional[Order]:
        return self.bids[0] if self.bids else None

    def _best_ask(self) -> Optional[Order]:
        return self.asks[0] if self.asks else None

    def match(self) -> List[Tuple[Order, Order, int, float]]:
        # Match orders based on price-time priority.
        # Returns list of (buy_order, sell_order, traded_qty, trade_price).
        trades: List[Tuple[Order, Order, int, float]] = []

        while self.bids and self.asks:
            best_bid = self._best_bid()
            best_ask = self._best_ask()

            # Crossing condition using actual prices
            if best_bid.price >= best_ask.price:
                traded_qty = min(best_bid.quantity, best_ask.quantity)
                trade_price = (best_bid.price + best_ask.price) / 2.0

                best_bid.quantity -= traded_qty
                best_ask.quantity -= traded_qty

                trades.append((best_bid, best_ask, traded_qty, trade_price))

                if best_bid.quantity == 0:
                    heapq.heappop(self.bids)
                if best_ask.quantity == 0:
                    heapq.heappop(self.asks)
            else:
                break

        return trades
